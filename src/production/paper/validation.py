from common.model import Config


def validate_production_config(config: Config) -> None:
    # Exactly one aggregator must be specified for MVP simplicity
    assert len(config.aggregators) == 1, "Exactly one aggregator must be specified"
    assert len(config.models) > 0, "At least one model must be specified"
    # portfolio_optimizers is optional; if present, allow one for MVP
    portfolio_optimizers = getattr(config, "optimizers", [])
    assert (
            isinstance(portfolio_optimizers, list) or portfolio_optimizers is None
    ), "optimizers must be a list if provided"
    if isinstance(portfolio_optimizers, list):
        assert len(portfolio_optimizers) <= 1, "At most one portfolio optimizer for MVP"
