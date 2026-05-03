"""
trading_engine — multi-strategy, registry-driven research and execution
pipeline.

Five subsystems wired together by ``core.orchestrate_*``:

  * model_state — feature engineering registry
  * models      — signal generators (one weight stream per model)
  * aggregators — combine multiple model signals into a portfolio
  * optimizers  — refine weights using an asset-level risk model (optional)
  * risk        — covariance / risk model providers

Each subsystem exposes a ``REGISTRY`` (a `dict[name, spec]`) so models,
aggregators, optimizers, etc. can be referenced by name in YAML configs
without code changes.
"""
