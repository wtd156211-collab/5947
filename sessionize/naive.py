"""笨办法实现：全部读入、排序、再切会话。仅供测试对拍使用。

正常路径（sessionize.streaming）严禁导入本模块；tests 里有专门的
检查保证生产代码没有引用它。

处理规则与 README 完全一致：显式按 ``(user_id, timestamp, line_no)``
排序，同毫秒严格按输入行序；相邻事件间隔严格大于 30 分钟才切会话。
"""

from .streaming import Header
from .timefmt import format_epoch_ms, parse_timestamp

_EXPECTED_HEADER = "timestamp,user_id,event_type"
GAP_MS = 1_800_000


def naive_sessionize(lines):
    """事件行字符串/字节迭代器 -> 会话 CSV 文本（含表头）。

    一次性吃进全部事件，内存随行数线性增长，只用于测试。
    """
    events = []
    header_seen = False
    line_no = 0
    for raw in lines:
        line = raw.decode("utf-8").rstrip("\r\n") if isinstance(raw, bytes) \
            else raw.rstrip("\r\n")
        if not header_seen:
            if line != _EXPECTED_HEADER:
                raise ValueError("bad header: %r" % (line,))
            header_seen = True
            continue
        if not line:
            raise ValueError("line %d: empty line" % (line_no + 1))
        line_no += 1
        ts_text, rest = line.split(",", 1)
        user_id, _event_type = rest.split(",", 1)
        events.append((parse_timestamp(ts_text), user_id.encode("utf-8"),
                       line_no))
    if not header_seen:
        raise ValueError("missing header")

    events.sort(key=lambda event: (event[1], event[0], event[2]))

    rows = []
    prev_user = None
    current = None  # [start, end, count]
    for ts, user, _line_no in events:
        if user != prev_user:
            if current is not None:
                rows.append((current[1], prev_user, current[0], current[2]))
            current = [ts, ts, 1]
            prev_user = user
        elif ts - current[1] > GAP_MS:
            rows.append((current[1], prev_user, current[0], current[2]))
            current = [ts, ts, 1]
        else:
            current[1] = ts
            current[2] += 1
    if current is not None:
        rows.append((current[1], prev_user, current[0], current[2]))

    rows.sort()

    out = [Header.decode("ascii")]
    for end, user, start, count in rows:
        out.append("%s,%s,%s,%d,%d\n" % (
            user.decode("utf-8"),
            format_epoch_ms(start), format_epoch_ms(end),
            end - start, count))
    return "".join(out)
