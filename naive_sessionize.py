"""笨办法会话化：全部读进内存、排好序再切会话。

仅供测试对拍用，不参与正常路径；正常入口（sessionize.py）不得调用本模块。
"""

from sessionize import (
    GAP_MS,
    HEADER,
    OUTPUT_HEADER,
    format_timestamp_ms,
    parse_timestamp_ms,
)


def sessionize_naive(lines):
    """输入事件行（可迭代），返回完整的输出 CSV 文本。"""
    events = []
    for line_no, line in enumerate(lines, 1):
        line = line.rstrip("\r\n")
        if not line or line == HEADER:
            continue
        ts_text, user_id, _event_type = line.split(",", 2)
        events.append((parse_timestamp_ms(ts_text), line_no, user_id))
    # 按时间戳升序，同一毫秒按输入行序（行号唯一，结果确定）。
    events.sort()

    open_sessions = {}  # user_id -> [start_ms, end_ms, count]
    done = []           # (end_ms, user_id, start_ms, count)
    for ts, _line_no, user_id in events:
        state = open_sessions.get(user_id)
        if state is None:
            open_sessions[user_id] = [ts, ts, 1]
        elif ts - state[1] > GAP_MS:
            done.append((state[1], user_id, state[0], state[2]))
            open_sessions[user_id] = [ts, ts, 1]
        else:
            state[1] = ts
            state[2] += 1
    for user_id, state in open_sessions.items():
        done.append((state[1], user_id, state[0], state[2]))

    # 输出顺序：结束时间升序，再按 user_id 的 UTF-8 字节序。
    done.sort(key=lambda item: (item[0], item[1].encode("utf-8")))

    out = [OUTPUT_HEADER]
    for end_ms, user_id, start_ms, count in done:
        out.append(
            "%s,%s,%s,%d,%d"
            % (
                user_id,
                format_timestamp_ms(start_ms),
                format_timestamp_ms(end_ms),
                end_ms - start_ms,
                count,
            )
        )
    return "\n".join(out) + "\n"
