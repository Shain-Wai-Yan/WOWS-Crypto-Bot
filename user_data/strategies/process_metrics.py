"""Small Linux memory sample; no tracing, extra worker threads, or trading decisions."""
from pathlib import Path


def sample_process_memory(proc_root="/proc", cgroup_root="/sys/fs/cgroup"):
    proc = Path(proc_root)
    group = Path(cgroup_root)

    def text(path):
        try:
            return path.read_text()
        except OSError:
            return ""

    def counter(name):
        value = text(group / name).strip()
        return int(value) if value.isdigit() else None

    def pairs(value):
        result = {}
        for line in value.splitlines():
            fields = line.split()
            if len(fields) >= 2 and fields[1].isdigit():
                result[fields[0].rstrip(":")] = int(fields[1])
        return result

    status = pairs(text(proc / "self" / "status"))
    stats = pairs(text(group / "memory.stat"))
    total = counter("memory.current")
    inactive = stats.get("inactive_file")
    return {
        "rssBytes": status.get("VmRSS") * 1024 if "VmRSS" in status else None,
        "peakRssBytes": status.get("VmHWM") * 1024 if "VmHWM" in status else None,
        "threads": status.get("Threads"),
        "cgroupBytes": total,
        "limitBytes": counter("memory.max"),
        "anonymousBytes": stats.get("anon"),
        "fileCacheBytes": stats.get("file"),
        "inactiveFileBytes": inactive,
        "workingSetEstimateBytes": max(0, total - inactive) if total is not None and inactive is not None else None,
        "oomKills": pairs(text(group / "memory.events")).get("oom_kill"),
    }
