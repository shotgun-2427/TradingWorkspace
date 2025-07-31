from common.model import Config


def validate_production_config(config: Config) -> None:
    assert len(config.optimizers) == 1, "Exactly one optimizer must be specified"
    assert len(config.models) > 0, "At least one model must be specified"
