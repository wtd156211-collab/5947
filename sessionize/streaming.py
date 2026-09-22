"""流式会话化：内存占用只跟活跃用户数和乱序窗口有关，不随行数增长。

思路
----
- 乱序上限 LATE_MS = 300000：任何事件都不会比“已见最大时间戳”
  （水位线）早超过 5 分钟。
- 每个用户维护按开始时间升序的“暂定会话”，相邻会话间隔严格大于
  GAP_MS。迟到事件按时间戳归位，必要时把被它桥接的会话粘回去。
  关键不变量：最早的暂定会话，一旦满足
  ``watermark - end > GAP_MS + LATE_MS``（35 分钟），就不可能再有
  合法迟到事件改到它（未来事件最早只可能是 watermark - LATE_MS，
  而那时它与该会话的间隔已严格大于 GAP_MS），可以定版丢弃。
  35 分钟窗口内不可能塞下三个互相间隔超过 30 分钟的会话，所以每个
  用户的暂定会话至多两个，状态量与总行数无关。
- 各用户定版会话进按 (end, user_id) 的最小堆，满足最终输出顺序
  (session_end 升序, user_id UTF-8 字节序升序)，边定版边吐出；
  未来新会话的 end 不可能小于已吐出的 end，顺序不会被破坏。
"""

import heapq
from collections import deque

from .timefmt import format_epoch_ms, parse_timestamp

GAP_MS = 1_800_000
LATE_MS = 300_000
FINALIZE_MS = GAP_MS + LATE_MS

Header = b"user_id,session_start,session_end,duration_ms,event_count\n"
_EXPECTED_HEADER = b"timestamp,user_id,event_type"


def _parse_event(line: bytes, line_no: int):
    """解析一行 -> (epoch_ms, user_id_bytes)。

    user_id 保留 bytes，UTF-8 字节序天然就是 Python bytes 比较顺序。
    """
    ts_part, rest = line.split(b",", 1)
    user_id, _event_type = rest.split(b",", 1)
    try:
        return parse_timestamp(ts_part), user_id
    except ValueError:
        raise ValueError("line %d: bad timestamp" % line_no)


def _merge_sweep(sessions):
    """从头扫一遍，把相邻间隔不大于 GAP_MS 的会话粘起来。"""
    i = 0
    while i + 1 < len(sessions):
        start, end, count = sessions[i]
        nxt_start, nxt_end, nxt_count = sessions[i + 1]
        if nxt_start - end > GAP_MS:
            i += 1
        else:
            sessions[i] = (start, nxt_end, count + nxt_count)
            del sessions[i + 1]


def _absorb(sessions, ts):
    """把一条时间戳为 ts 的事件并入某用户的暂定会话（含迟到归位）。"""
    last_start, last_end, last_count = sessions[-1]

    if ts >= last_end:
        # 文件序更晚，或同一毫秒行序在后（同毫秒只加计数，端点不变）。
        if ts - last_end > GAP_MS:
            sessions.append((ts, ts, 1))
        else:
            sessions[-1] = (last_start, ts, last_count + 1)
        return

    # ts < last_end：迟到事件。定位它时间上落在哪个会话附近。
    for idx in range(len(sessions) - 1, -1, -1):
        start, end, count = sessions[idx]
        if ts >= start:
            # 落在会话内部（含与 start 同毫秒）：只加计数。
            sessions[idx] = (start, end, count + 1)
            return
        if idx == 0:
            if start - ts <= GAP_MS:
                sessions[0] = (ts, end, count + 1)
            else:
                sessions.appendleft((ts, ts, 1))
            return
        prev_start, prev_end, prev_count = sessions[idx - 1]
        if ts - prev_end <= GAP_MS:
            if start - ts <= GAP_MS:
                # 事件把前后两个会话桥接成一个。
                sessions[idx - 1] = (prev_start, end,
                                     prev_count + count + 1)
                del sessions[idx]
            else:
                # 属于前一个会话的尾巴。
                sessions[idx - 1] = (prev_start, ts, prev_count + 1)
        elif start - ts <= GAP_MS:
            # 属于后一个会话的新起点。
            sessions[idx] = (ts, end, count + 1)
        else:
            sessions.insert(idx, (ts, ts, 1))
        return


def _format_row(user_id: bytes, start_ms: int, end_ms: int,
                count: int) -> bytes:
    return (user_id + b","
            + format_epoch_ms(start_ms).encode("ascii") + b","
            + format_epoch_ms(end_ms).encode("ascii") + b","
            + str(end_ms - start_ms).encode("ascii") + b","
            + str(count).encode("ascii") + b"\n")


def sessionize_lines(lines):
    """事件行字节迭代器 -> 会话 CSV 字节输出迭代器（每个元素一行 LF）。

    输入必须带表头，事件须满足 5 分钟乱序约定。内存只与
    “结尾 35 分钟窗口内的活跃用户数”有关，与总行数无关。
    """
    yield Header

    states = {}   # user_id -> deque[(start, end, count)]
    ready = []    # 堆: (end, user_id, start, count)
    watermark = None
    header_seen = False
    line_no = 0

    for raw in lines:
        line = raw.rstrip(b"\r\n")
        if not header_seen:
            if line != _EXPECTED_HEADER:
                raise ValueError("bad header: %r" % (line,))
            header_seen = True
            continue
        if not line:
            raise ValueError("line %d: empty line" % (line_no + 1))
        line_no += 1
        ts, user = _parse_event(line, line_no)

        if watermark is None or ts > watermark:
            watermark = ts

        sessions = states.get(user)
        if sessions is None:
            sessions = deque([(ts, ts, 1)])
        else:
            _absorb(sessions, ts)
            _merge_sweep(sessions)
        states[user] = sessions

        start, end, count = sessions[0]
        heapq.heappush(ready, (end, user, start, count))

        # 水位线推进后，定版不再可能被改到的最早会话，按输出顺序吐出。
        cutoff = watermark - FINALIZE_MS
        while ready and ready[0][0] < cutoff:
            end, u, start, count = heapq.heappop(ready)
            cur = states.get(u)
            if not cur or cur[0] != (start, end, count):
                continue  # 过期投递：该用户的最早会话已变化或已定版
            cur.popleft()
            if cur:
                nxt_start, nxt_end, nxt_count = cur[0]
                heapq.heappush(ready, (nxt_end, u, nxt_start, nxt_count))
            else:
                del states[u]
            yield _format_row(u, start, end, count)

    if not header_seen:
        raise ValueError("missing header")

    # 收尾：剩余暂定会话全部定版，统一排序输出。
    final = []
    for u, sessions in states.items():
        for start, end, count in sessions:
            final.append((end, u, start, count))
    final.sort()
    for end, u, start, count in final:
        yield _format_row(u, start, end, count)


def run(input_path: str, output_path: str) -> None:
    """命令行入口：从文件读、往文件写，全程流式。"""
    with open(input_path, "rb") as src, open(output_path, "wb") as dst:
        for chunk in sessionize_lines(src):
            dst.write(chunk)


def main(argv=None) -> int:
    import sys
    args = sys.argv[1:] if argv is None else argv
    if len(args) != 2:
        sys.stderr.write(
            "usage: python -m sessionize.streaming INPUT OUTPUT\n")
        return 2
    run(args[0], args[1])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
