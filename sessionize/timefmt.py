"""时间戳解析与格式化（只用标准库）。

输入输出都固定为 UTC、毫秒精度的 ISO-8601，形如
``2026-09-22T10:00:00.000Z``。内部一律用整数毫秒纪元时间，
避免浮点误差，也方便做差值比较。
"""

EPOCH_MONTH_DAYS = (0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334)
def _is_leap(year: int) -> bool:
    return year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)


def parse_timestamp(line_part) -> int:
    """把 24 字符的 ISO-8601 毫秒时间戳解析成纪元毫秒数。

    接受 ``bytes`` 或 ``str``，严格校验固定形状
    ``YYYY-MM-DDTHH:MM:SS.mmmZ``，非法即抛 ``ValueError``。
    """
    if isinstance(line_part, bytes):
        text = line_part.decode("ascii")
    else:
        text = line_part
    if len(text) != 24:
        raise ValueError("bad timestamp: %r" % (line_part,))
    if text[4] != "-" or text[7] != "-" or text[10] != "T" or text[13] != ":" \
            or text[16] != ":" or text[19] != "." or text[23] != "Z":
        raise ValueError("bad timestamp: %r" % (line_part,))
    try:
        year = int(text[0:4])
        month = int(text[5:7])
        day = int(text[8:10])
        hour = int(text[11:13])
        minute = int(text[14:16])
        second = int(text[17:19])
        millis = int(text[20:23])
    except ValueError:
        raise ValueError("bad timestamp: %r" % (line_part,))
    if not 1 <= month <= 12 or not 1 <= day <= 31 or hour > 23 \
            or minute > 59 or second > 59:
        raise ValueError("bad timestamp: %r" % (line_part,))
    month_days = EPOCH_MONTH_DAYS[month - 1]
    days = (year - 1970) * 365 + (year - 1969) // 4 - (year - 1901) // 100 \
        + (year - 1601) // 400 + month_days + (day - 1)
    if month >= 3 and _is_leap(year):
        days += 1
    if day > _days_in_month(year, month):
        raise ValueError("bad timestamp: %r" % (line_part,))
    return ((days * 24 + hour) * 60 + minute) * 60000 + second * 1000 + millis


def _days_in_month(year: int, month: int) -> int:
    if month == 2:
        return 29 if _is_leap(year) else 28
    if month in (4, 6, 9, 11):
        return 30
    return 31


def format_epoch_ms(value: int) -> str:
    """把纪元毫秒数格式化成固定形状的 UTC 毫秒时间戳字符串。"""
    seconds, millis = divmod(value, 1000)
    days, secs_of_day = divmod(seconds, 86400)
    hour, rem = divmod(secs_of_day, 3600)
    minute, second = divmod(rem, 60)
    year, month, day = _days_to_ymd(days)
    return "%04d-%02d-%02dT%02d:%02d:%02d.%03dZ" % (
        year, month, day, hour, minute, second, millis)


def _days_to_ymd(days: int):
    """1970-01-01 起的天数转 (year, month, day)，Howard Hinnant 算法。"""
    days += 719468
    era = (days if days >= 0 else days - 146096) // 146097
    doe = days - era * 146097
    yoe = (doe - doe // 1460 + doe // 36524 - doe // 146096) // 365
    year = yoe + era * 400
    doy = doe - (365 * yoe + yoe // 4 - yoe // 100)
    mp = (5 * doy + 2) // 153
    day = doy - (153 * mp + 2) // 5 + 1
    month = mp + 3 if mp < 10 else mp - 9
    if month <= 2:
        year += 1
    return year, month, day
