import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "user_data" / "strategies"))
from process_metrics import sample_process_memory


def test_separates_rss_cache_and_working_set(tmp_path):
    proc = tmp_path / "proc" / "self"
    proc.mkdir(parents=True)
    (proc / "status").write_text("VmRSS: 65 kB\nVmHWM: 70 kB\nThreads: 9\n")
    group = tmp_path / "group"
    group.mkdir()
    for name, value in {"memory.current": "100", "memory.max": "200",
                        "memory.stat": "anon 60\nfile 30\ninactive_file 20\n",
                        "memory.events": "oom 0\noom_kill 0\n"}.items():
        (group / name).write_text(value)
    sample = sample_process_memory(proc.parent, group)
    assert sample["rssBytes"] == 65 * 1024
    assert sample["anonymousBytes"] == 60
    assert sample["fileCacheBytes"] == 30
    assert sample["workingSetEstimateBytes"] == 80
    assert sample["oomKills"] == 0


def test_missing_counters_are_unknown_not_zero(tmp_path):
    sample = sample_process_memory(tmp_path, tmp_path)
    assert sample["rssBytes"] is None
    assert sample["oomKills"] is None
    assert sample["workingSetEstimateBytes"] is None
