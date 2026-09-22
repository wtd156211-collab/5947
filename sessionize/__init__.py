"""设备事件会话化。

正常入口为 :mod:`sessionize.streaming`。包内另有一个全量读入排序的
参考实现，仅供测试对拍，生产代码不得导入。
"""

from .streaming import Header, sessionize_lines

__all__ = ["Header", "sessionize_lines"]
