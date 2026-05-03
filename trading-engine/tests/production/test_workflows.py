from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
RELEASE_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "release-controller.yml"
ROLLBACK_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "manual-rollback.yml"


def test_release_workflow_parses() -> None:
    data = yaml.safe_load(RELEASE_WORKFLOW.read_text(encoding="utf-8"))
    assert isinstance(data, dict)


def test_rollback_workflow_parses() -> None:
    data = yaml.safe_load(ROLLBACK_WORKFLOW.read_text(encoding="utf-8"))
    assert isinstance(data, dict)


def test_release_workflow_has_profile_and_run_mode_envs() -> None:
    content = RELEASE_WORKFLOW.read_text(encoding="utf-8")
    required_tokens = [
        "RUN_MODE=production",
        "PIPELINE_PROFILE=paper",
        "PIPELINE_PROFILE=live",
        "SIMULATION_PROFILE=paper",
        "SIMULATION_PROFILE=live",
        "GITHUB_ACTIONS=true",
        "release-sim-live",
    ]
    for token in required_tokens:
        assert token in content


def test_rollback_workflow_includes_simulations_live() -> None:
    content = ROLLBACK_WORKFLOW.read_text(encoding="utf-8")
    assert "simulations-live" in content
