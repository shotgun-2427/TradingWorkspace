# Universal Supplemental

Supplemental data is not tied to hawk_ids. It's identified by a combination of `source` (e.g., `eia_petroleum`, `fred`) and `series_id` (e.g., `WCESTUS1`).

## List Available Sources

```python
from hawk_sdk.api import UniversalSupplemental

supplemental = UniversalSupplemental()

sources = supplemental.get_available_sources()
sources.show()
```

## List Available Series

```python
from hawk_sdk.api import UniversalSupplemental

supplemental = UniversalSupplemental()

# All series
all_series = supplemental.get_all_series()
all_series.show(n=20)

# Series for a specific source
eia_series = supplemental.get_all_series(source="eia_petroleum")
eia_series.show()
```

Output columns: `source`, `series_id`, `name`, `description`, `frequency`, `unit`.

## Query Data (Date Range)

```python
from hawk_sdk.api import UniversalSupplemental

supplemental = UniversalSupplemental()

response = supplemental.get_data(
    sources=["eia_petroleum"],
    series_ids=["WCESTUS1", "WCRFPUS2"],
    start_date="2024-01-01",
    end_date="2024-12-31"
)

df = response.to_df()
response.show()
```

Output columns: `source`, `series_id`, `series_name`, `record_timestamp`, `value`, `char_value`.

## Query All Data by Source

Fetch all series for a source without specifying individual series_ids:

```python
response = supplemental.get_data_by_source(
    sources=["eia_petroleum"],
    start_date="2024-01-01",
    end_date="2024-12-31"
)
```

## Latest Data

Get the most recent data point for each series:

```python
response = supplemental.get_latest_data(
    sources=["eia_petroleum"],
    series_ids=["WCESTUS1", "WCRNTUS2", "WCRFPUS2"]
)

response.show()
```
