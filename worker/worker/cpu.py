from __future__ import annotations

import os
import re


def _parse_cpuset(cpuset: str) -> int:
    # Examples: "0-3", "0,2", "0-1,4-5"
    cpuset = cpuset.strip()
    if not cpuset:
        return 0
    count = 0
    for part in cpuset.split(","):
        part = part.strip()
        if "-" in part:
            a, b = part.split("-", 1)
            count += int(b) - int(a) + 1
        else:
            count += 1
    return count


def available_cpu_cores() -> int:
    # Prefer cgroup limits when in containers. Fallback to os.cpu_count().
    # cgroup v2 cpu.max: "<quota> <period>" or "max <period>"
    # cpuset.cpus: list/range
    try:
        # cpuset (v2)
        for p in ["/sys/fs/cgroup/cpuset.cpus.effective", "/sys/fs/cgroup/cpuset/cpuset.cpus"]:
            if os.path.exists(p):
                with open(p, "r", encoding="utf-8") as f:
                    n = _parse_cpuset(f.read())
                    if n > 0:
                        return n

        # cpu quota (v2)
        cpu_max = "/sys/fs/cgroup/cpu.max"
        if os.path.exists(cpu_max):
            with open(cpu_max, "r", encoding="utf-8") as f:
                quota, period = f.read().strip().split()
                if quota != "max":
                    q = int(quota)
                    p = int(period)
                    if q > 0 and p > 0:
                        return max(1, q // p)

        # cgroup v1 quota
        quota_path = "/sys/fs/cgroup/cpu/cpu.cfs_quota_us"
        period_path = "/sys/fs/cgroup/cpu/cpu.cfs_period_us"
        if os.path.exists(quota_path) and os.path.exists(period_path):
            with open(quota_path, "r", encoding="utf-8") as fq, open(period_path, "r", encoding="utf-8") as fp:
                quota = int(fq.read().strip())
                period = int(fp.read().strip())
                if quota > 0 and period > 0:
                    return max(1, quota // period)
    except Exception:
        pass

    return max(1, os.cpu_count() or 1)
