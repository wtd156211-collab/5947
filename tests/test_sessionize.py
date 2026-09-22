"""会话化测试：样例、随机对拍、30 万行规模、真实峰值内存、模块隔离。"""

import os
import random
import resource
import subprocess
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from sessionize import naive, streaming
from sessionize.timefmt import format_epoch_ms, parse_timestamp

HEADER = "timestamp,user_id,event_type\n"

# 64 MiB：需求里给的内存余量。
MEM_BUDGET_BYTES = 64 * 1024 * 1024
# 流式进程的绝对 RSS 上限：解释器基线 + 64MiB 余量。


def build_lines(events):
    """events: [(epoch_ms, user_id)]，按给定顺序拼成带表头的文本行列表。"""
    lines = [HEADER.encode("ascii")]
    for ts, user in events:
        lines.append(("%s,%s,e\n" % (format_epoch_ms(ts), user)).encode("utf-8"))
    return lines


def stream_result(events):
    return b"".join(streaming.sessionize_lines(iter(build_lines(events))))


def naive_result(events):
    return naive.naive_sessionize(iter(build_lines(events))).encode("utf-8")


def random_disordered_events(rng, n_events, n_users, span_ms=6 * 3600_000,
                              late_ms=300_000):
    """生成严格满足 5 分钟乱序约定的事件序列。

    先造按时间升序的事件，再按不超过 late_ms 的时间跨度分块，块内
    Fisher-Yates 洗牌。块内最早与最晚时间戳之差 <= late_ms，所以块内
    任何排列都不会让某条事件比已见最大值早超过 late_ms。
    """
    users = ["u%05d" % i for i in range(n_users)]
    ordered = []
    for _ in range(n_events):
        ts = rng.randint(0, span_ms)
        ordered.append((ts, rng.choice(users)))
    ordered.sort(key=lambda e: e[0])

    blocks = []
    block = []
    block_min = None
    for ts, user in ordered:
        if block_min is None:
            block_min = ts
        if ts - block_min > late_ms:
            blocks.append(block)
            block = []
            block_min = ts
        block.append((ts, user))
    if block:
        blocks.append(block)
    for block in blocks:
        rng.shuffle(block)
    result = []
    for block in blocks:
        result.extend(block)
    return result


class SampleTests(unittest.TestCase):
    def test_samples_match_expected_outputs(self):
        for name in ("case-1", "case-2"):
            with open(os.path.join(ROOT, "samples", name + ".events.csv"),
                      "rb") as f:
                got = b"".join(streaming.sessionize_lines(f))
            with open(os.path.join(ROOT, "samples", name + ".sessions.csv"),
                      "rb") as f:
                want = f.read()
            self.assertEqual(got, want, name)

    def test_samples_naive_agrees(self):
        for name in ("case-1", "case-2"):
            with open(os.path.join(ROOT, "samples", name + ".events.csv"),
                      "rb") as f:
                lines = f.readlines()
            got = naive.naive_sessionize(iter(lines)).encode("utf-8")
            with open(os.path.join(ROOT, "samples", name + ".sessions.csv"),
                      "rb") as f:
                want = f.read()
            self.assertEqual(got, want, name)


class CrossCheckTests(unittest.TestCase):
    def _check(self, events):
        a = stream_result(events)
        b = naive_result(events)
        self.assertEqual(a, b)

    def test_random_disordered_many_seeds(self):
        for seed in range(40):
            rng = random.Random(seed)
            events = random_disordered_events(
                rng, n_events=2000, n_users=rng.randint(1, 40))
            self._check(events)

    def test_single_user_gap_boundaries(self):
        base = parse_timestamp("2026-01-01T00:00:00.000Z")
        # 正好 30 分钟：同一会话；1ms 之后：断开。
        events = [(base, "u1"), (base + 1_800_000, "u1"),
                  (base + 3_600_001, "u1"), (base + 5_400_000, "u1")]
        self._check(events)

    def test_same_millisecond_line_order(self):
        base = parse_timestamp("2026-01-01T00:00:00.000Z")
        events = [(base, "u1")] * 5 + [(base, "u2")] * 3
        self._check(events)
        rows = stream_result(events).decode().strip().splitlines()
        self.assertEqual(rows[1], "u1,%s,%s,0,5" % (
            format_epoch_ms(base), format_epoch_ms(base)))
        self.assertEqual(rows[2], "u2,%s,%s,0,3" % (
            format_epoch_ms(base), format_epoch_ms(base)))

    def test_late_exactly_five_minutes(self):
        base = parse_timestamp("2026-01-01T00:10:00.000Z")
        # 迟到恰好 5 分钟，落在已有会话中间。
        events = [(base, "u1"), (base + 600_000, "u1"),
                  (base + 300_000, "u1")]
        self._check(events)

    def test_late_event_bridges_split(self):
        base = parse_timestamp("2026-01-01T00:00:00.000Z")
        # 先看到两个间隔 >30 分钟的事件，迟到的中间事件把它们粘回一个会话。
        events = [(base, "u1"), (base + 1_900_000, "u1"),
                  (base + 1_500_000, "u1")]
        self._check(events)
        rows = stream_result(events).decode().strip().splitlines()
        self.assertEqual(len(rows), 2)  # 表头 + 被粘回的单个会话
        self.assertEqual(rows[1].split(",")[4], "3")

    def test_output_order_end_then_userid_bytes(self):
        base = parse_timestamp("2026-01-01T00:00:00.000Z")
        # 结束时间相同，user_id 按 UTF-8 字节序；非 ASCII 也参与。
        events = [(base, "用户z"), (base, "用户a"), (base, "u9"),
                  (base, "u10"), (base, "u1")]
        rows = stream_result(events).decode().strip().splitlines()[1:]
        order = [r.split(",")[0] for r in rows]
        expect = sorted(["用户z", "用户a", "u9", "u10", "u1"],
                        key=lambda x: x.encode("utf-8"))
        self.assertEqual(order, expect)

    def test_deterministic_byte_for_byte(self):
        rng = random.Random(123)
        events = random_disordered_events(rng, 5000, 30)
        first = stream_result(events)
        second = stream_result(list(events))
        self.assertEqual(first, second)
        self.assertTrue(first.endswith(b"\n"))

    def test_empty_events_only_header(self):
        self.assertEqual(stream_result([]),
                         b"user_id,session_start,session_end,duration_ms,"
                         b"event_count\n")

    def test_missing_or_bad_header_raises(self):
        with self.assertRaises(ValueError):
            list(streaming.sessionize_lines(iter([b"nope\n"])))
        with self.assertRaises(ValueError):
            list(streaming.sessionize_lines(iter([])))


class LargeScaleTests(unittest.TestCase):
    def test_300k_events_cross_check(self):
        rng = random.Random(20260922)
        events = random_disordered_events(
            rng, n_events=300_000, n_users=5_000)
        a = stream_result(events)
        b = naive_result(events)
        self.assertEqual(a, b)
        self.assertGreater(a.count(b"\n"), 1)


PEAK_MEM_RUNNER = r"""
import os, sys, random, tempfile, resource
sys.path.insert(0, "__ROOT__")
from sessionize import streaming
from sessionize.timefmt import format_epoch_ms

# 注意：本脚本必须自包含，不能 import tests 包——否则会把测试模块
# （含 30 万行对拍数据）加载进子进程，污染峰值内存测量。

# 分块生成：块间时间严格递增，块内 shuffle。块的时间跨度 <= 5 分钟，
# 因此任何排列都满足“不晚于已见最大值 5 分钟”的乱序约定；
# 生成侧每次只持有一个块，不随总行数涨内存。
n_events = int(sys.argv[1])
n_users = int(sys.argv[2])
span_ms = 24 * 3600 * 1000
block_ms = 300000
n_blocks = span_ms // (block_ms + 1)
rng = random.Random(99)
fd, path = tempfile.mkstemp(prefix="events-", suffix=".csv")
os.close(fd)
try:
    with open(path, "wb") as f:
        f.write(b"timestamp,user_id,event_type\n")
        base = 0
        remaining = n_events
        for b in range(n_blocks):
            left_blocks = n_blocks - b
            k = max(1, round(remaining / left_blocks)) if b < n_blocks - 1 \
                else remaining
            remaining -= k
            block = []
            for _ in range(k):
                ts = base + rng.randint(0, block_ms)
                block.append((format_epoch_ms(ts), "u%05d"
                              % rng.randrange(n_users)))
            rng.shuffle(block)
            for ts_text, user in block:
                f.write(("%s,%s,e\n" % (ts_text, user)).encode("ascii"))
            base += block_ms + 1

    # 两级 spawn：测量的是这个“孙进程”自己处理前后的 RSS 增量。
    # 不能直接用 ru_maxrss 减基线——ru_maxrss 是单调峰值——所以
    # 通过 mode=baseline / work 两次启动分别取数，两次启动环境完全
    # 相同，只差“是否处理事件文件”，差值就是处理逻辑的真实峰值占用。
    mode = sys.argv[3]
    if mode == "baseline":
        path2 = path
        peak_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        print("%d 0" % (peak_kb * 1024))
    else:
        n_bytes = 0
        with open(path, "rb") as src:
            for row in streaming.sessionize_lines(src):
                n_bytes += len(row)
        peak_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        print("%d %d" % (peak_kb * 1024, n_bytes))
finally:
    if os.path.exists(path):
        os.unlink(path)
""".replace("__ROOT__", ROOT)


class MemoryTests(unittest.TestCase):
    def _measure(self, n_events, n_users):
        """返回 (处理逻辑真实峰值增量字节, 输出字节数)。

        两次启动同一独立进程，分别测“解释器+生成文件”基线与
        “再加流式处理”的峰值，差值抵消基线；独立进程又抵消测试
        父进程自身占用（posix_spawn 的 ru_maxrss 会反映父进程页）。
        """
        env = dict(os.environ, PYTHONPATH=ROOT)

        def run(mode):
            out = subprocess.check_output(
                [sys.executable, "-c", PEAK_MEM_RUNNER, str(n_events),
                 str(n_users), mode], cwd=ROOT, env=env)
            return map(int, out.split())

        base_peak, _ = run("baseline")
        work_peak, out_bytes = run("work")
        self.assertGreater(out_bytes, 0)
        return work_peak - base_peak, out_bytes

    def test_peak_memory_does_not_grow_with_lines(self):
        # 行数翻 4 倍（10 万 -> 40 万），处理逻辑自身的峰值内存增量
        # 必须落在 64MiB 余量内；理想情况下两次几乎一样大。
        small, _ = self._measure(100_000, 20_000)
        large, _ = self._measure(400_000, 20_000)
        self.assertLess(large - small, MEM_BUDGET_BYTES,
                        "delta grew %d bytes (small=%d large=%d)"
                        % (large - small, small, large))

    def test_peak_working_set_under_budget(self):
        # 处理逻辑的真实工作集（相对基线）本身也要在 64MiB 余量内，
        # 再留 16MiB 给分配器波动。
        peak, _ = self._measure(400_000, 20_000)
        self.assertLess(peak, MEM_BUDGET_BYTES + 16 * 1024 * 1024,
                        "working set peak %d bytes" % peak)


class IsolationTests(unittest.TestCase):
    def test_naive_not_imported_by_streaming_path(self):
        import ast
        pkg_dir = os.path.join(ROOT, "sessionize")
        for fname in ("__init__.py", "streaming.py", "timefmt.py"):
            with open(os.path.join(pkg_dir, fname), encoding="utf-8") as f:
                tree = ast.parse(f.read(), filename=fname)
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    names = [a.name for a in node.names]
                elif isinstance(node, ast.ImportFrom):
                    names = [node.module or ""]
                else:
                    continue
                for name in names:
                    self.assertFalse(name.endswith("naive"),
                                     "%s imports %s" % (fname, name))

    def test_cli_end_to_end(self):
        rng = random.Random(7)
        events = random_disordered_events(rng, 3000, 50)
        with tempfile.TemporaryDirectory() as tmp:
            in_path = os.path.join(tmp, "in.csv")
            out_path = os.path.join(tmp, "out.csv")
            with open(in_path, "wb") as f:
                for line in build_lines(events):
                    f.write(line)
            rc = streaming.main([in_path, out_path])
            self.assertEqual(rc, 0)
            with open(out_path, "rb") as f:
                got = f.read()
            self.assertEqual(got, stream_result(events))
            self.assertEqual(got, naive_result(events))


if __name__ == "__main__":
    unittest.main()
