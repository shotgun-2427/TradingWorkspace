import os
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

from common.logging import setup_logger

logger = setup_logger(__name__)

VALID_PROFILES = {"paper", "live"}
VALID_RUN_MODES = {"local", "production"}


class RuntimeContextError(ValueError):
    """Raised when runtime configuration from environment variables is invalid."""


@dataclass(frozen=True)
class RuntimeContext:
    pipeline_kind: str
    profile: str
    run_mode: str
    current_date: str
    github_actions: bool

    @property
    def side_effects_enabled(self) -> bool:
        """Return True when side-effectful sinks are allowed."""
        return self.run_mode == "production"


def _parse_run_mode() -> str:
    """Parse and validate RUN_MODE from environment variables."""
    run_mode = os.environ.get("RUN_MODE", "local").strip().lower()
    if run_mode not in VALID_RUN_MODES:
        raise RuntimeContextError(
            f"Invalid RUN_MODE='{run_mode}'. Expected one of: {sorted(VALID_RUN_MODES)}."
        )
    return run_mode


def _parse_profile(
    *,
    profile_env_var: str,
    default_profile: str = "paper",
) -> str:
    """Parse and validate a profile value from the given environment variable."""
    profile = os.environ.get(profile_env_var, default_profile).strip().lower()
    if profile not in VALID_PROFILES:
        raise RuntimeContextError(
            f"Invalid {profile_env_var}='{profile}'. Expected one of: {sorted(VALID_PROFILES)}."
        )
    return profile


def _resolve_execution_profile(run_mode: str) -> str:
    """Resolve execution profile with PIPELINE_PROFILE and legacy PIPELINE_MODE support."""
    explicit = os.environ.get("PIPELINE_PROFILE", "").strip().lower()
    if explicit:
        if explicit not in VALID_PROFILES:
            raise RuntimeContextError(
                f"Invalid PIPELINE_PROFILE='{explicit}'. Expected one of: {sorted(VALID_PROFILES)}."
            )
        return explicit

    legacy_mode = os.environ.get("PIPELINE_MODE", "").strip().lower()
    if not legacy_mode:
        return "paper"

    if run_mode == "local":
        logger.warning(
            "PIPELINE_MODE is deprecated. Use PIPELINE_PROFILE=paper|live instead."
        )

    if legacy_mode in VALID_PROFILES:
        return legacy_mode

    if legacy_mode == "all":
        if run_mode == "production":
            raise RuntimeContextError(
                "PIPELINE_MODE=all is not allowed with RUN_MODE=production. "
                "Use separate deployments with PIPELINE_PROFILE=paper or PIPELINE_PROFILE=live."
            )
        logger.warning(
            "PIPELINE_MODE=all is deprecated in local mode; defaulting to PIPELINE_PROFILE=paper."
        )
        return "paper"

    raise RuntimeContextError(
        f"Invalid PIPELINE_MODE='{legacy_mode}'. Expected one of: all, paper, live."
    )


def _parse_github_actions() -> bool:
    """Return whether GITHUB_ACTIONS is explicitly set to true."""
    return os.environ.get("GITHUB_ACTIONS", "").strip().lower() == "true"


def _enforce_production_guardrail(run_mode: str, github_actions: bool) -> None:
    """Fail closed when production mode is requested outside GitHub Actions."""
    if run_mode == "production" and not github_actions:
        raise RuntimeContextError(
            "RUN_MODE=production requires GITHUB_ACTIONS=true. "
            "Refusing to start to prevent unsafe side effects."
        )


def _current_new_york_date() -> str:
    """Return the current date string in America/New_York timezone."""
    return datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")


def load_execution_context() -> RuntimeContext:
    """Build and validate runtime context for execution pipeline runs."""
    run_mode = _parse_run_mode()
    profile = _resolve_execution_profile(run_mode)
    github_actions = _parse_github_actions()
    _enforce_production_guardrail(run_mode, github_actions)
    return RuntimeContext(
        pipeline_kind="pipeline",
        profile=profile,
        run_mode=run_mode,
        current_date=_current_new_york_date(),
        github_actions=github_actions,
    )


def load_simulation_context() -> RuntimeContext:
    """Build and validate runtime context for simulation pipeline runs."""
    run_mode = _parse_run_mode()
    profile = _parse_profile(profile_env_var="SIMULATION_PROFILE", default_profile="paper")
    github_actions = _parse_github_actions()
    _enforce_production_guardrail(run_mode, github_actions)
    return RuntimeContext(
        pipeline_kind="simulations",
        profile=profile,
        run_mode=run_mode,
        current_date=_current_new_york_date(),
        github_actions=github_actions,
    )
