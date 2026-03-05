from common.model import Config
from production.runtime.config_loader import load_execution_profile_config

VALID_PROFILES = {"paper", "live"}


def _validate_profile(profile: str) -> None:
    """Ensure the profile name is one of the supported runtime profiles."""
    if profile not in VALID_PROFILES:
        raise ValueError(
            f"Invalid profile '{profile}'. Expected one of: {sorted(VALID_PROFILES)}."
        )


def _validate_single_pipeline_config(config: Config, label: str) -> None:
    """Validate core shape constraints for a single pipeline config."""
    if len(config.aggregators) != 1:
        raise ValueError(f"{label}: exactly one aggregator must be specified")
    if len(config.models) == 0:
        raise ValueError(f"{label}: at least one model must be specified")

    portfolio_optimizers = getattr(config, "optimizers", None)
    if portfolio_optimizers is not None and not isinstance(portfolio_optimizers, list):
        raise ValueError(f"{label}: optimizers must be a list when provided")
    if isinstance(portfolio_optimizers, list) and len(portfolio_optimizers) > 1:
        raise ValueError(f"{label}: at most one optimizer is currently supported")


def _validate_execution_vs_simulation_alignment(
    execution_config: Config,
    simulation_config: Config,
    profile: str,
) -> None:
    """Verify execution and simulation configs stay aligned for one profile."""
    execution_models = set(execution_config.models)
    simulation_models = set(simulation_config.models)
    if execution_models != simulation_models:
        raise ValueError(
            f"{profile}: models mismatch between execution and simulation merged configs. "
            f"Execution: {sorted(execution_models)}, Simulations: {sorted(simulation_models)}"
        )

    execution_aggregators = set(execution_config.aggregators)
    simulation_aggregators = set(simulation_config.aggregators)
    if execution_aggregators != simulation_aggregators:
        raise ValueError(
            f"{profile}: aggregators mismatch between execution and simulation merged configs. "
            f"Execution: {sorted(execution_aggregators)}, Simulations: {sorted(simulation_aggregators)}"
        )

    execution_optimizers = set(getattr(execution_config, "optimizers", []) or [])
    simulation_optimizers = set(getattr(simulation_config, "optimizers", []) or [])
    if execution_optimizers != simulation_optimizers:
        raise ValueError(
            f"{profile}: optimizers mismatch between execution and simulation merged configs. "
            f"Execution: {sorted(execution_optimizers)}, Simulations: {sorted(simulation_optimizers)}"
        )


def validate_execution_config(config: Config, profile: str) -> None:
    """Validate an execution config for the target profile."""
    _validate_profile(profile)
    _validate_single_pipeline_config(config=config, label=f"execution:{profile}")


def validate_simulation_merged_config(config: Config, profile: str) -> None:
    """Validate merged simulation config and its alignment with execution config."""
    _validate_profile(profile)
    _validate_single_pipeline_config(config=config, label=f"simulations:{profile}")

    execution_config = load_execution_profile_config(profile)
    _validate_single_pipeline_config(
        config=execution_config,
        label=f"execution:{profile}",
    )
    _validate_execution_vs_simulation_alignment(
        execution_config=execution_config,
        simulation_config=config,
        profile=profile,
    )
