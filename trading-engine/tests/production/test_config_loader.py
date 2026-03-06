from pathlib import Path

import yaml

from production.runtime.config_loader import load_simulation_profile_config


def test_simulation_override_applies_per_profile(tmp_path: Path) -> None:
    overrides_path = tmp_path / "overrides.yaml"
    overrides_path.write_text(
        yaml.safe_dump(
            {
                "profiles": {
                    "paper": {"start_date": "2001-01-01"},
                    "live": {"start_date": "2011-01-01"},
                }
            }
        ),
        encoding="utf-8",
    )

    paper_config = load_simulation_profile_config("paper", overrides_path=overrides_path)
    live_config = load_simulation_profile_config("live", overrides_path=overrides_path)

    assert str(paper_config.start_date) == "2001-01-01"
    assert str(live_config.start_date) == "2011-01-01"
    assert paper_config.models == live_config.models


def test_simulation_override_only_changes_target_profile(tmp_path: Path) -> None:
    overrides_path = tmp_path / "overrides.yaml"
    overrides_path.write_text(
        yaml.safe_dump(
            {"profiles": {"paper": {"end_date": "2040-12-31"}}}
        ),
        encoding="utf-8",
    )

    paper_config = load_simulation_profile_config("paper", overrides_path=overrides_path)
    live_config = load_simulation_profile_config("live", overrides_path=overrides_path)

    assert str(paper_config.end_date) == "2040-12-31"
    assert str(live_config.end_date) != "2040-12-31"
