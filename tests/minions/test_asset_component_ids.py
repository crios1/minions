from pathlib import Path

from minions._internal._cli.doctor_ids import inspect_paths


def test_official_asset_component_ids_are_valid_and_unique(tests_dir: Path) -> None:
    component_id_issues = [
        issue
        for issue in inspect_paths([tests_dir / "assets"])
        if issue.code in {"duplicate-component-id", "invalid-component-id"}
    ]
    assert component_id_issues == []
