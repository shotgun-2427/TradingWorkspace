import datetime
from datetime import date

from opentelemetry import trace

from pipeline.common.enums import Environment, WriteMode
from pipeline.common.otel import setup_otel, flush_otel
from pipeline.pipeline import Pipeline
from pipeline.sources.eia_petroleum import EIAPetroleum

setup_otel('test')
tracer = trace.get_tracer(__name__)

with tracer.start_as_current_span("test") as span:
    pipeline = Pipeline(
        source=EIAPetroleum(
            environment=Environment.PRODUCTION,
        ),
        write_mode=WriteMode.BIGQUERY
    )

    yesterday = date.today() - datetime.timedelta(days=1)
    pipeline.run(
        start_date="1980-01-01",
        end_date="2019-12-31",
        # Fetches all series by default: WCESTUS1, WCRNTUS2, WCRFPUS2
    )

flush_otel()
