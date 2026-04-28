from __future__ import annotations

import subprocess
import sys


def test_python_m_minions_tune_sqlite_help() -> None:
    completed = subprocess.run(
        [sys.executable, "-m", "minions", "tune", "sqlite", "--help"],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0
    assert "python -m minions tune sqlite" in completed.stdout
    assert "--recommend-config" in completed.stdout


def test_python_m_minions_tune_sqlite_recommend_config_smoke() -> None:
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "minions",
            "tune",
            "sqlite",
            "--recommend-config",
            "--rounds",
            "1",
            "--payload-bytes",
            "128",
            "--warmup-ops",
            "8",
            "--profile-low-ops",
            "8",
            "--profile-low-concurrency",
            "1",
            "--profile-medium-ops",
            "16",
            "--profile-medium-concurrency",
            "4",
            "--profile-upper-ops",
            "32",
            "--profile-upper-concurrency",
            "16",
            "--tune-batch-max-queued-writes",
            "16",
            "--tune-flush-delay-ms",
            "5",
            "--tune-interarrival-delay-ms",
            "2",
            "--tune-top-n",
            "1",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0
    assert "Immediate baselines:" in completed.stdout
    assert "Recommended batched_balanced config:" in completed.stdout
    assert "Recommended batched_max_throughput config:" in completed.stdout
