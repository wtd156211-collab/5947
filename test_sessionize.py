"""sessionize 的单元测试与对拍测试（unittest，仅标准库）。"""

import io
import os
import random
import tempfile
import tracemalloc
import unittest

import naive_sessionize
import sessionize
from sessionize import (
    GAP_MS,
    MAX_LATENESS_MS,
    OUTPUT_HEADER,
    format_timestamp_ms,
)

SAMPLES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "samples")

# 内存余量：流式路径处理大文件的峰值不得超过 64 MiB。
MEMORY_LIMIT_BYTES = 64 * 1024 * 1024

BASE_MS = sessionize.parse_timestamp_ms("2026-09-22T00:00:00.000Z")


def run_streaming(lines):
    out = io.StringIO()
    sessionize.sessionize_stream(iter(lines), out)
    return out.getvalue()


def make_events_lines(events):
    """events: [(ts_ms, user_id), ...]，按给定顺序拼成输入行。"""
    lines = [sessionize.HEADER]
    for i, (ts, user_id) in enumerate(events):
        lines.append("%s,%s,evt%d" % (format_timestamp_ms(ts), user_id, i % 7))
    return lines


def jitter_order(events, rng, max_lateness_ms=MAX_LATENESS_MS):
    """把事件打乱成“最多迟到 max_lateness_ms”的合法顺序。

    给每条事件一个 位置键 = ts + [0, max_lateness] 的随机偏移，再按位置键
    排序。可以证明：任意事件之前的最大时间戳不超过它自身 ts + max_lateness，
    即迟到不超过 max_lateness，符合输入约定。
    """
    keyed = [(ts + rng.randint(0, max_lateness_ms), i, ts, user)
             for i, (ts, user) in enumerate(events)]
    keyed.sort()
    return [(ts, user) for _, _, ts, user in keyed]


class SampleCaseTest(unittest.TestCase):
    def _check(self, name):
        with open(os.path.join(SAMPLES_DIR, name + ".events.csv"),
                  encoding="utf-8") as f:
            events = f.read().splitlines()
        with open(os.path.join(SAMPLES_DIR, name + ".sessions.csv"),
                  encoding="utf-8") as f:
            expected = f.read()
        self.assertEqual(run_streaming(events), expected, "流式实现与样例不符")
        self.assertEqual(naive_sessionize.sessionize_naive(events), expected,
                         "笨办法与样例不符")

    def test_case_1(self):
        self._check("case-1")

    def test_case_2(self):
        self._check("case-2")


class BoundaryTest(unittest.TestCase):
    def test_exactly_30min_stays_in_session(self):
        lines = make_events_lines([(BASE_MS, "u1"), (BASE_MS + GAP_MS, "u1")])
        out = run_streaming(lines).splitlines()
        self.assertEqual(len(out), 2)
        self.assertTrue(out[1].endswith(",%d,2" % GAP_MS))

    def test_30min_plus_1ms_splits(self):
        lines = make_events_lines([(BASE_MS, "u1"), (BASE_MS + GAP_MS + 1, "u1")])
        out = run_streaming(lines).splitlines()
        self.assertEqual(len(out), 3)
        self.assertTrue(out[1].endswith(",0,1"))
        self.assertTrue(out[2].endswith(",0,1"))

    def test_same_millisecond_events(self):
        lines = make_events_lines([(BASE_MS, "u1")] * 3)
        out = run_streaming(lines).splitlines()
        self.assertEqual(len(out), 2)
        self.assertTrue(out[1].endswith(",0,3"))

    def test_output_tie_break_by_user_id_bytes(self):
        # 两个会话结束时间相同：按 user_id 的 UTF-8 字节序，"u10" < "u2"。
        lines = make_events_lines([(BASE_MS, "u2"), (BASE_MS, "u10")])
        out = run_streaming(lines).splitlines()
        self.assertEqual([line.split(",", 1)[0] for line in out[1:]], ["u10", "u2"])

    def test_exactly_5min_late_is_accepted(self):
        events = [(BASE_MS, "u1"), (BASE_MS + 10 * 60 * 1000, "u2"),
                  (BASE_MS + 5 * 60 * 1000, "u1")]
        out = run_streaming(make_events_lines(events)).splitlines()
        self.assertEqual(len(out), 3)
        u1 = [line for line in out if line.startswith("u1,")][0]
        self.assertTrue(u1.endswith(",%d,2" % (5 * 60 * 1000)))

    def test_empty_input(self):
        self.assertEqual(run_streaming([sessionize.HEADER]), OUTPUT_HEADER + "\n")
        self.assertEqual(run_streaming([]), OUTPUT_HEADER + "\n")


class CrossCheckTest(unittest.TestCase):
    """随机造带乱序的数据，流式实现与笨办法对拍，结果必须逐字节一致。"""

    def test_randomized_cross_check(self):
        for seed in range(8):
            rng = random.Random(seed)
            n_users = rng.randint(1, 60)
            users = ["u%04d" % i for i in range(n_users)]
            n_events = rng.randint(1, 20000)
            span_ms = rng.choice([60_000, 3_600_000, 86_400_000])
            events = [(BASE_MS + rng.randint(0, span_ms), rng.choice(users))
                      for _ in range(n_events)]
            # 故意制造大量同一毫秒的并列事件。
            if seed % 2:
                events += [(BASE_MS, rng.choice(users)) for _ in range(200)]
            shuffled = jitter_order(events, rng)
            lines = make_events_lines(shuffled)
            with self.subTest(seed=seed):
                self.assertEqual(run_streaming(lines),
                                 naive_sessionize.sessionize_naive(lines))

    def test_deterministic_across_runs(self):
        rng = random.Random(12345)
        events = [(BASE_MS + rng.randint(0, 3_600_000), "u%03d" % rng.randint(0, 99))
                  for _ in range(20000)]
        lines = make_events_lines(jitter_order(events, rng))
        first = run_streaming(lines)
        second = run_streaming(lines)
        self.assertEqual(first, second)


class LargeScaleMemoryTest(unittest.TestCase):
    """放大到几十万行：流式路径实测内存峰值，并与笨办法对拍。"""

    N_EVENTS = 300_000

    @classmethod
    def setUpClass(cls):
        rng = random.Random(20260922)
        users = ["user%05d" % i for i in range(5000)]
        events = [(BASE_MS + rng.randint(0, 86_400_000), rng.choice(users))
                  for _ in range(cls.N_EVENTS)]
        cls.lines = make_events_lines(jitter_order(events, rng))
        tmp = tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", newline="", suffix=".csv", delete=False)
        tmp.write("\n".join(cls.lines) + "\n")
        tmp.close()
        cls.events_path = tmp.name

    @classmethod
    def tearDownClass(cls):
        os.unlink(cls.events_path)

    def test_streaming_memory_peak_and_correctness(self):
        expected = naive_sessionize.sessionize_naive(self.lines)

        # 真实文件逐行读取 + tracemalloc 实测峰值，不是注释里写一句省内存。
        tracemalloc.start()
        out = io.StringIO()
        with open(self.events_path, encoding="utf-8", newline="") as fin:
            sessionize.sessionize_stream(fin, out)
        _current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        self.assertEqual(out.getvalue(), expected, "大规模输入下两种实现结果不一致")
        self.assertLess(
            peak, MEMORY_LIMIT_BYTES,
            "流式处理峰值内存 %.1f MiB，超出 64 MiB 余量" % (peak / 1024 / 1024))

    def test_normal_entry_does_not_use_naive(self):
        with open(sessionize.__file__, encoding="utf-8") as f:
            self.assertNotIn("naive", f.read(),
                             "正常入口不得引用笨办法实现")


if __name__ == "__main__":
    unittest.main()
