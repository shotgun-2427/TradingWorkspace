# Universal

## Query Data (Date Range)

```python
from hawk_sdk.api import Universal

universal = Universal()

response = universal.get_data(
    hawk_ids=[1, 2, 3],
    field_ids=[1, 4, 5],
    start_date="2024-04-01",
    end_date="2024-10-01",
    interval="1d"
)

df = response.to_df()
response.show()
```

## Query Snapshot (Point-in-Time)

Use `interval="snapshot"` to get the most recent data up to `end_date`:

```python
response = universal.get_data(
    hawk_ids=[1, 2, 3],
    field_ids=[17, 18, 19, 20, 21],
    start_date="",
    end_date="2024-12-01 15:00:00",
    interval="snapshot"
)
```

## Latest Snapshot

Get the most recent data available (no timestamp needed):

```python
response = universal.get_latest_snapshot(
    hawk_ids=[1, 2, 3],
    field_ids=[17, 18, 19, 20, 21]
)
```

Output columns: `date`, `hawk_id`, `ticker`, plus one column per field. Missing values are `NaN`.

## List All Fields

```python
from hawk_sdk.api import Universal

universal = Universal()
fields = universal.get_all_fields()
fields.show(n=50)
```

## Lookup Field IDs by Name

```python
from hawk_sdk.api import Universal

universal = Universal()
field_lookup = universal.get_field_ids(
    field_names=["open_1d", "close_1d", "volume_1d"]
)
field_lookup.show()
```
