import pandas as pd

from pipeline.base_source import BaseSource
from pipeline.common.enums import Environment, WriteMode
from pipeline.pipeline import Pipeline


class DummySourceWithErrors(BaseSource):
    def __init__(self):
        super().__init__(environment=Environment.DEVELOPMENT, source_type="dummy")
        self.write_called_with = None

    def fetch(self, **kwargs):
        return pd.DataFrame(
            {
                "hawk_id": [1],
                "record_timestamp": [pd.Timestamp("2024-01-01")],
                "field_id": [5],
                "char_value": ["a"],
                "int_value": [1],
                "double_value": [1.0],
            }
        )

    def transform(self, df):
        return df

    def validate(self, df):
        return ["bad data"]

    def write(self, data, write_mode, output_path=None):
        self.write_called_with = (data.copy(), write_mode)


class DummySourceNoErrors(DummySourceWithErrors):
    def validate(self, df):
        return []


def test_pipeline_run_with_errors(patch_base_source):
    source = DummySourceWithErrors()
    pipeline = Pipeline(source=source, write_mode=WriteMode.LOG)
    errors = pipeline.run()

    # Validation errors returned
    assert errors == ["bad data"]
    # Write executed
    assert source.write_called_with is not None


def test_pipeline_run_no_errors(patch_base_source):
    source = DummySourceNoErrors()
    pipeline = Pipeline(source=source, write_mode=WriteMode.LOG)
    errors = pipeline.run()

    assert errors == []
    assert source.write_called_with is not None
    # No validation warning logged
    assert not any("Validation error:" in m for m in patch_base_source.messages)
