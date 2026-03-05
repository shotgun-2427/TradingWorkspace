from production.runtime.context import RuntimeContext
from production.runtime.sinks import (
    NoopExecutionSink,
    NoopNotificationSink,
    build_artifact_sink,
    build_execution_sink,
    build_gcs_prefix,
    build_notification_sink,
)


def test_gcs_prefix_contracts() -> None:
    assert (
        build_gcs_prefix(
            pipeline_kind="pipeline",
            profile="paper",
            current_date="2026-03-05",
        )
        == "hcf/paper/production_audit/2026-03-05"
    )
    assert (
        build_gcs_prefix(
            pipeline_kind="pipeline",
            profile="live",
            current_date="2026-03-05",
        )
        == "hcf/live/production_audit/2026-03-05"
    )
    assert (
        build_gcs_prefix(
            pipeline_kind="simulations",
            profile="paper",
            current_date="2026-03-05",
        )
        == "hcf/paper/simulations_audit/2026-03-05"
    )
    assert (
        build_gcs_prefix(
            pipeline_kind="simulations",
            profile="live",
            current_date="2026-03-05",
        )
        == "hcf/live/simulations_audit/2026-03-05"
    )


def test_local_mode_uses_local_and_noop_sinks() -> None:
    context = RuntimeContext(
        pipeline_kind="pipeline",
        profile="paper",
        run_mode="local",
        current_date="2026-03-05",
        github_actions=False,
    )
    artifact_sink = build_artifact_sink(context)
    assert artifact_sink.__class__.__name__ == "LocalSink"
    assert isinstance(build_notification_sink(context, webhook_url="https://example.com"), NoopNotificationSink)
    assert isinstance(build_execution_sink(context), NoopExecutionSink)


def test_production_mode_uses_gcs_sink(monkeypatch) -> None:
    class FakeGcsSink:
        def __init__(self, *, bucket_name: str, prefix: str):
            self.bucket_name = bucket_name
            self.prefix = prefix

    monkeypatch.setattr("production.runtime.sinks.GcsSink", FakeGcsSink)

    context = RuntimeContext(
        pipeline_kind="simulations",
        profile="live",
        run_mode="production",
        current_date="2026-03-05",
        github_actions=True,
    )
    artifact_sink = build_artifact_sink(context)
    assert isinstance(artifact_sink, FakeGcsSink)
    assert artifact_sink.prefix == "hcf/live/simulations_audit/2026-03-05"
