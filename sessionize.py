"""设备事件会话化（流式实现，正常路径入口）。

边读边处理：内存占用只取决于“5 分钟乱序窗口内的事件数”和
“当前未关闭会话的用户数”，不随输入行数增长。
切分规则、同时刻排序规则、输出格式见 README.md。
"""

import heapq
import sys
from datetime import datetime, timedelta, timezone

GAP_MS = 30 * 60 * 1000          # 相邻事件间隔严格大于 30 分钟才断开会话
MAX_LATENESS_MS = 5 * 60 * 1000  # 上游允许的最大迟到（相对已出现的最大时间戳）

HEADER = "timestamp,user_id,event_type"
OUTPUT_HEADER = "user_id,session_start,session_end,duration_ms,event_count"

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def parse_timestamp_ms(text):
    """把 ``YYYY-MM-DDTHH:MM:SS.mmmZ`` 解析成 UTC 毫秒时间戳（int）。"""
    if (
        len(text) != 24
        or text[4] != "-"
        or text[7] != "-"
        or text[10] != "T"
        or text[13] != ":"
        or text[16] != ":"
        or text[19] != "."
        or text[23] != "Z"
    ):
        raise ValueError("非法时间戳: %r" % (text,))
    dt = datetime(
        int(text[0:4]), int(text[5:7]), int(text[8:10]),
        int(text[11:13]), int(text[14:16]), int(text[17:19]),
        int(text[20:23]) * 1000, tzinfo=timezone.utc,
    )
    delta = dt - _EPOCH
    return (delta.days * 86400 + delta.seconds) * 1000 + delta.microseconds // 1000


def format_timestamp_ms(ms):
    """把 UTC 毫秒时间戳格式化成 ``YYYY-MM-DDTHH:MM:SS.mmmZ``。"""
    dt = _EPOCH + timedelta(milliseconds=ms)
    return "%s.%03dZ" % (dt.strftime("%Y-%m-%dT%H:%M:%S"), dt.microsecond // 1000)


class StreamingSessionizer:
    """增量会话化器。

    事件先进重排堆，按 (时间戳, 行号) 顺序吐出——只有当一个事件不可能
    再被更晚到达的乱序事件插队（其时间戳 <= 已见最大时间戳 - 5 分钟）时
    才吐出。会话在水位线越过 last_event + 30 分钟时关闭；由于水位线单调
    不减，会话按结束时间非递减的顺序关闭，同一批内按 (结束时间, user_id
    的 UTF-8 字节序) 排序后写出，因此全局输出顺序确定。
    """

    def __init__(self, out):
        self._out = out
        self._reorder = []    # 小根堆: (ts, line_no, user_id)，行号保证同时刻按行序
        self._max_seen = -1   # 文件中目前出现过的最大时间戳
        self._open = {}       # user_id -> [start_ms, end_ms, count]
        self._deadlines = []  # 小根堆: (end_ms + GAP_MS, user_id, end_ms 快照)
        self._pending = []    # 同一水位线下关闭、待排序写出的会话
        self._pending_watermark = None

    def feed(self, ts, user_id, line_no):
        """喂入一条原始事件（允许最多 5 分钟的乱序）。"""
        if ts > self._max_seen:
            self._max_seen = ts
        heapq.heappush(self._reorder, (ts, line_no, user_id))
        threshold = self._max_seen - MAX_LATENESS_MS
        while self._reorder and self._reorder[0][0] <= threshold:
            event_ts, _, event_user = heapq.heappop(self._reorder)
            self._consume(event_ts, event_user)

    def finish(self):
        """输入结束：排空重排堆，关闭所有未关闭会话。"""
        while self._reorder:
            event_ts, _, event_user = heapq.heappop(self._reorder)
            self._consume(event_ts, event_user)
        self._flush_pending()
        remaining = [
            (state[1], user_id, state[0], state[2])
            for user_id, state in self._open.items()
        ]
        remaining.sort(key=lambda item: (item[0], item[1].encode("utf-8")))
        for end_ms, user_id, start_ms, count in remaining:
            self._write_session(user_id, start_ms, end_ms, count)
        self._open.clear()

    def _consume(self, ts, user_id):
        # 水位线推进到 ts：last + GAP < ts 的会话不可能再被延长，关闭。
        deadlines = self._deadlines
        while deadlines and deadlines[0][0] < ts:
            _, uid, snapshot = heapq.heappop(deadlines)
            state = self._open.get(uid)
            if state is not None and state[1] == snapshot:
                self._finalize(uid, state, ts)
        state = self._open.get(user_id)
        if state is None:
            self._open[user_id] = [ts, ts, 1]
        else:
            # 过期会话已在上面关闭，到这里间隔必然 <= GAP_MS。
            state[1] = ts
            state[2] += 1
        heapq.heappush(self._deadlines, (ts + GAP_MS, user_id, ts))

    def _finalize(self, user_id, state, watermark):
        if self._pending_watermark is not None and watermark != self._pending_watermark:
            # 不同水位线关闭的会话，结束时间严格递增，可以先写出上一批。
            self._flush_pending()
        self._pending_watermark = watermark
        self._pending.append((state[1], user_id, state[0], state[2]))
        del self._open[user_id]

    def _flush_pending(self):
        if not self._pending:
            return
        self._pending.sort(key=lambda item: (item[0], item[1].encode("utf-8")))
        for end_ms, user_id, start_ms, count in self._pending:
            self._write_session(user_id, start_ms, end_ms, count)
        self._pending.clear()
        self._pending_watermark = None

    def _write_session(self, user_id, start_ms, end_ms, count):
        self._out.write(
            "%s,%s,%s,%d,%d\n"
            % (
                user_id,
                format_timestamp_ms(start_ms),
                format_timestamp_ms(end_ms),
                end_ms - start_ms,
                count,
            )
        )


def sessionize_stream(fin, out):
    """从 fin 逐行读事件，把会话结果写入 out。fin 可以是任何按行迭代的对象。"""
    out.write(OUTPUT_HEADER + "\n")
    sessionizer = StreamingSessionizer(out)
    line_no = 0
    for line in fin:
        line = line.rstrip("\r\n")
        if not line:
            continue
        line_no += 1
        if line_no == 1 and line == HEADER:
            continue
        ts_text, user_id, _event_type = line.split(",", 2)
        sessionizer.feed(parse_timestamp_ms(ts_text), user_id, line_no)
    sessionizer.finish()


def main(argv=None):
    args = list(sys.argv[1:] if argv is None else argv)
    if len(args) not in (1, 2):
        sys.stderr.write("用法: python3 sessionize.py 输入.csv [输出.csv]\n")
        return 2
    with open(args[0], "r", encoding="utf-8", newline="") as fin:
        if len(args) == 2:
            with open(args[1], "w", encoding="utf-8", newline="") as fout:
                sessionize_stream(fin, fout)
        else:
            sessionize_stream(fin, sys.stdout)
    return 0


if __name__ == "__main__":
    sys.exit(main())
