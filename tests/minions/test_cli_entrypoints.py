from __future__ import annotations

import subprocess
import sys
import uuid
from pathlib import Path


# Note: if this module keeps growing, extract small CLI subprocess/assert helpers
# to keep command setup and output checks consistent across entrypoint tests.


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


def test_python_m_minions_stamp_component_ids_help() -> None:
    completed = subprocess.run(
        [sys.executable, "-m", "minions", "stamp", "component-ids", "--help"],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0
    assert "python -m minions stamp component-ids" in completed.stdout
    assert "--dry-run" in completed.stdout


def test_python_m_minions_stamp_config_ids_help() -> None:
    completed = subprocess.run(
        [sys.executable, "-m", "minions", "stamp", "config-ids", "--help"],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0
    assert "python -m minions stamp config-ids" in completed.stdout
    assert "--dry-run" in completed.stdout


def test_python_m_minions_stamp_all_help() -> None:
    completed = subprocess.run(
        [sys.executable, "-m", "minions", "stamp", "all", "--help"],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0
    assert "python -m minions stamp all" in completed.stdout
    assert "--dry-run" in completed.stdout


def test_python_m_minions_doctor_ids_help() -> None:
    completed = subprocess.run(
        [sys.executable, "-m", "minions", "doctor", "ids", "--help"],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0
    assert "python -m minions doctor ids" in completed.stdout


def test_python_m_minions_stamp_component_ids_smoke(tmp_path: Path) -> None:
    source_path = tmp_path / "components.py"
    source_path.write_text(
        "\n".join(
            [
                "from minions import Minion, Pipeline, Resource",
                "",
                "class Event:",
                "    pass",
                "",
                "class Ctx:",
                "    pass",
                "",
                "class MyMinion(Minion[Event, Ctx]):",
                "    pass",
                "",
                "class MyPipeline(Pipeline[Event]):",
                "    pass",
                "",
                "class MyResource(Resource):",
                "    pass",
                "",
            ]
        )
    )

    completed = subprocess.run(
        [sys.executable, "-m", "minions", "stamp", "component-ids", str(source_path)],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0
    assert "stamped MyMinion with @minion_id" in completed.stdout
    assert "stamped MyPipeline with @pipeline_id" in completed.stdout
    assert "stamped MyResource with @resource_id" in completed.stdout

    stamped = source_path.read_text()
    assert "from minions import minion_id, pipeline_id, resource_id" in stamped
    ids = [
        line.split("\"")[1]
        for line in stamped.splitlines()
        if line.startswith(("@minion_id", "@pipeline_id", "@resource_id"))
    ]
    assert len(ids) == 3
    assert [str(uuid.UUID(component_id)) for component_id in ids] == ids

    second = subprocess.run(
        [sys.executable, "-m", "minions", "stamp", "component-ids", "--dry-run", str(source_path)],
        check=False,
        capture_output=True,
        text=True,
    )

    assert second.returncode == 0
    assert "no missing component ids" in second.stdout


def test_python_m_minions_stamp_component_ids_recurses_directories(tmp_path: Path) -> None:
    source_dir = tmp_path / "src"
    nested_dir = source_dir / "nested"
    hidden_dir = source_dir / ".hidden"
    nested_dir.mkdir(parents=True)
    hidden_dir.mkdir()
    (nested_dir / "components.py").write_text(
        "\n".join(
            [
                "from minions import Resource",
                "",
                "class MyResource(Resource):",
                "    pass",
                "",
            ]
        )
    )
    (hidden_dir / "ignored.py").write_text(
        "\n".join(
            [
                "from minions import Resource",
                "",
                "class IgnoredResource(Resource):",
                "    pass",
                "",
            ]
        )
    )
    (source_dir / "notes.txt").write_text("not python")

    completed = subprocess.run(
        [sys.executable, "-m", "minions", "stamp", "component-ids", str(source_dir)],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0
    assert "components.py" in completed.stdout
    assert "ignored.py" not in completed.stdout
    assert "@resource_id" in (nested_dir / "components.py").read_text()
    assert "@resource_id" not in (hidden_dir / "ignored.py").read_text()


def test_python_m_minions_stamp_config_ids_toml_smoke(tmp_path: Path) -> None:
    config_path = tmp_path / "minion.toml"
    config_path.write_text('[config]\nname = "alpha"\n')

    completed = subprocess.run(
        [sys.executable, "-m", "minions", "stamp", "config-ids", str(config_path)],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0
    assert "stamped _minions_config_id" in completed.stdout
    lines = config_path.read_text().splitlines()
    assert lines[0] == "# Generated by `minions stamp config-ids`; do not edit manually."
    config_id = lines[1].split("\"")[1]
    assert str(uuid.UUID(config_id)) == config_id

    second = subprocess.run(
        [sys.executable, "-m", "minions", "stamp", "config-ids", "--dry-run", str(config_path)],
        check=False,
        capture_output=True,
        text=True,
    )

    assert second.returncode == 0
    assert "already has _minions_config_id" in second.stdout


def test_python_m_minions_stamp_config_ids_toml_detects_whitespace_assignment(tmp_path: Path) -> None:
    config_path = tmp_path / "minion.toml"
    config_id = "11111111-1111-4111-8111-111111111111"
    source = f'   _minions_config_id   =   "{config_id}"\n[config]\nname = "alpha"\n'
    config_path.write_text(source)

    completed = subprocess.run(
        [sys.executable, "-m", "minions", "stamp", "config-ids", str(config_path)],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0
    assert "already has _minions_config_id" in completed.stdout
    assert config_path.read_text() == source


def test_python_m_minions_stamp_config_ids_yaml_smoke(tmp_path: Path) -> None:
    config_path = tmp_path / "minion.yaml"
    config_path.write_text("config:\n  name: alpha\n")

    completed = subprocess.run(
        [sys.executable, "-m", "minions", "stamp", "config-ids", str(config_path)],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0
    assert "stamped _minions_config_id" in completed.stdout
    lines = config_path.read_text().splitlines()
    assert lines[0] == "# Generated by `minions stamp config-ids`; do not edit manually."
    config_id = lines[1].split("\"")[1]
    assert str(uuid.UUID(config_id)) == config_id


def test_python_m_minions_stamp_config_ids_yml_smoke(tmp_path: Path) -> None:
    config_path = tmp_path / "minion.yml"
    config_path.write_text("config:\n  name: alpha\n")

    completed = subprocess.run(
        [sys.executable, "-m", "minions", "stamp", "config-ids", str(config_path)],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0
    assert "stamped _minions_config_id" in completed.stdout
    lines = config_path.read_text().splitlines()
    assert lines[0] == "# Generated by `minions stamp config-ids`; do not edit manually."
    config_id = lines[1].split("\"")[1]
    assert str(uuid.UUID(config_id)) == config_id


def test_python_m_minions_stamp_config_ids_recurses_directories(tmp_path: Path) -> None:
    config_dir = tmp_path / "configs"
    nested_dir = config_dir / "nested"
    hidden_dir = config_dir / ".hidden"
    nested_dir.mkdir(parents=True)
    hidden_dir.mkdir()
    toml_path = nested_dir / "minion.toml"
    yaml_path = nested_dir / "minion.yaml"
    json_path = nested_dir / "minion.json"
    ignored_path = hidden_dir / "ignored.toml"
    toml_path.write_text('[config]\nname = "alpha"\n')
    yaml_path.write_text("config:\n  name: bravo\n")
    json_path.write_text('{"name": "charlie"}')
    ignored_path.write_text('[config]\nname = "ignored"\n')
    (config_dir / "notes.txt").write_text("not config")

    completed = subprocess.run(
        [sys.executable, "-m", "minions", "stamp", "config-ids", str(config_dir)],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0
    assert "minion.toml" in completed.stdout
    assert "minion.yaml" in completed.stdout
    assert "minion.json" in completed.stdout
    assert "ignored.toml" not in completed.stdout
    assert "_minions_config_id" in toml_path.read_text()
    assert "_minions_config_id" in yaml_path.read_text()
    assert "_minions_config_id" in json_path.read_text()
    assert "_minions_config_id" not in ignored_path.read_text()


def test_python_m_minions_stamp_all_recurses_components_and_configs(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    src_dir = project_dir / "src"
    config_dir = project_dir / "configs"
    src_dir.mkdir(parents=True)
    config_dir.mkdir()
    component_path = src_dir / "components.py"
    config_path = config_dir / "minion.toml"
    component_path.write_text(
        "\n".join(
            [
                "from minions import Resource",
                "",
                "class MyResource(Resource):",
                "    pass",
                "",
            ]
        )
    )
    config_path.write_text('[config]\nname = "alpha"\n')

    completed = subprocess.run(
        [sys.executable, "-m", "minions", "stamp", "all", str(project_dir)],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0
    assert "components.py" in completed.stdout
    assert "minion.toml" in completed.stdout
    assert "@resource_id" in component_path.read_text()
    assert "_minions_config_id" in config_path.read_text()


def test_python_m_minions_stamp_all_accepts_mixed_explicit_files(tmp_path: Path) -> None:
    component_path = tmp_path / "components.py"
    config_path = tmp_path / "minion.yaml"
    component_path.write_text(
        "\n".join(
            [
                "from minions import Resource",
                "",
                "class MyResource(Resource):",
                "    pass",
                "",
            ]
        )
    )
    config_path.write_text("config:\n  name: alpha\n")

    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "minions",
            "stamp",
            "all",
            str(component_path),
            str(config_path),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0
    assert "@resource_id" in component_path.read_text()
    assert "_minions_config_id" in config_path.read_text()


def test_python_m_minions_doctor_ids_passes_clean_project(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    component_id = "11111111-1111-4111-8111-11111111111a"
    config_id = "22222222-2222-4222-8222-22222222222b"
    (project_dir / "components.py").write_text(
        "\n".join(
            [
                "from minions import Resource, resource_id",
                "",
                f"@resource_id(\"{component_id}\")",
                "class MyResource(Resource):",
                "    pass",
                "",
            ]
        )
    )
    (project_dir / "minion.toml").write_text(f'_minions_config_id = "{config_id}"\n')

    completed = subprocess.run(
        [sys.executable, "-m", "minions", "doctor", "ids", str(project_dir)],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0
    assert "No identity issues found." in completed.stdout


def test_python_m_minions_doctor_ids_reports_missing_invalid_and_duplicate_ids(tmp_path: Path) -> None:
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    duplicate_component_id = "33333333-3333-4333-8333-33333333333c"
    duplicate_config_id = "44444444-4444-4444-8444-44444444444d"
    (project_dir / "components.py").write_text(
        "\n".join(
            [
                "from minions import Resource, resource_id",
                "",
                "class MissingResource(Resource):",
                "    pass",
                "",
                "@resource_id(\"not-a-uuid\")",
                "class InvalidResource(Resource):",
                "    pass",
                "",
                f"@resource_id(\"{duplicate_component_id}\")",
                "class FirstResource(Resource):",
                "    pass",
                "",
                f"@resource_id(\"{duplicate_component_id}\")",
                "class SecondResource(Resource):",
                "    pass",
                "",
            ]
        )
    )
    (project_dir / "missing.toml").write_text('[config]\nname = "missing"\n')
    (project_dir / "invalid.yaml").write_text('_minions_config_id: "not-a-uuid"\n')
    (project_dir / "first.json").write_text(f'{{"_minions_config_id": "{duplicate_config_id}"}}')
    (project_dir / "second.json").write_text(f'{{"_minions_config_id": "{duplicate_config_id}"}}')

    completed = subprocess.run(
        [sys.executable, "-m", "minions", "doctor", "ids", str(project_dir)],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 1
    assert "missing-component-id" in completed.stdout
    assert "invalid-component-id" in completed.stdout
    assert "duplicate-component-id" in completed.stdout
    assert "missing-config-id" in completed.stdout
    assert "invalid-config-id" in completed.stdout
    assert "duplicate-config-id" in completed.stdout
