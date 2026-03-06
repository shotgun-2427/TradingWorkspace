# Hawk SDK

[Source Code](https://github.com/Hawk-Center/hawk-sdk)

## Installation

```bash
pip install hawk-sdk
```

**Authentication** (one of):
```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service_account.json"
```
```bash
export SERVICE_ACCOUNT_JSON='{"type":"service_account","project_id":"..."}'
```

!!! note
    All timestamps are in UTC.

---

## API Reference

=== "Universal"

    Query any combination of hawk_ids and field_ids.

    ```python
    from hawk_sdk.api import Universal
    universal = Universal(environment="production")
    ```

    | Method | Description |
    |--------|-------------|
    | `get_data(hawk_ids, field_ids, start_date, end_date, interval)` | Fetch data (use `interval='snapshot'` for point-in-time) |
    | `get_latest_snapshot(hawk_ids, field_ids)` | Fetch most recent data available |
    | `get_field_ids(field_names)` | Lookup field_ids by name |
    | `get_all_fields()` | List all available fields |

    **get_data**
    ```python
    def get_data(hawk_ids: List[int], field_ids: List[int], start_date: str, end_date: str, interval: str) -> DataObject
    ```
    
    | Parameter | Type | Description |
    |-----------|------|-------------|
    | `hawk_ids` | `List[int]` | Hawk IDs to query |
    | `field_ids` | `List[int]` | Field IDs to retrieve |
    | `start_date` | `str` | Start date (`YYYY-MM-DD`). Ignored for snapshot. |
    | `end_date` | `str` | End date (`YYYY-MM-DD`) or timestamp (`YYYY-MM-DD HH:MM:SS`) for snapshot |
    | `interval` | `str` | Data interval: `1d`, `1h`, etc. Use `snapshot` for point-in-time |

    **get_latest_snapshot**
    ```python
    def get_latest_snapshot(hawk_ids: List[int], field_ids: List[int]) -> DataObject
    ```

    Returns DataFrame with columns: `date`, `hawk_id`, `ticker`, plus one column per field. Missing values are `NaN`.

    **get_field_ids**
    ```python
    def get_field_ids(field_names: List[str]) -> DataObject
    ```

    **get_all_fields**
    ```python
    def get_all_fields() -> DataObject
    ```

=== "UniversalSupplemental"

    Query supplemental data not tied to hawk_ids (identified by source and series_id).

    ```python
    from hawk_sdk.api import UniversalSupplemental
    supplemental = UniversalSupplemental(environment="production")
    ```

    | Method | Description |
    |--------|-------------|
    | `get_data(sources, series_ids, start_date, end_date)` | Fetch data for specific series |
    | `get_data_by_source(sources, start_date, end_date)` | Fetch all series for given sources |
    | `get_latest_data(sources, series_ids)` | Fetch most recent data for each series |
    | `get_all_series(source=None)` | List available series metadata |
    | `get_available_sources()` | List available data sources |

    **get_data**
    ```python
    def get_data(sources: List[str], series_ids: List[str], start_date: str, end_date: str) -> DataObject
    ```
    
    | Parameter | Type | Description |
    |-----------|------|-------------|
    | `sources` | `List[str]` | Data source identifiers (e.g., `['eia_petroleum']`) |
    | `series_ids` | `List[str]` | Series codes (e.g., `['WCESTUS1', 'WCRFPUS2']`) |
    | `start_date` | `str` | Start date (`YYYY-MM-DD`) |
    | `end_date` | `str` | End date (`YYYY-MM-DD`) |

    **get_data_by_source**
    ```python
    def get_data_by_source(sources: List[str], start_date: str, end_date: str) -> DataObject
    ```

    Fetches all series for the given sources without specifying individual series_ids.

    **get_latest_data**
    ```python
    def get_latest_data(sources: List[str], series_ids: List[str]) -> DataObject
    ```

    Returns the most recent data point for each specified series.

    **get_all_series**
    ```python
    def get_all_series(source: Optional[str] = None) -> DataObject
    ```

    Returns DataFrame with columns: `source`, `series_id`, `name`, `description`, `frequency`, `unit`.

    **get_available_sources**
    ```python
    def get_available_sources() -> DataObject
    ```

=== "System"

    Lookup Hawk IDs from tickers.

    ```python
    from hawk_sdk.api import System
    system = System(environment="production")
    ```

    **get_hawk_ids**
    ```python
    def get_hawk_ids(tickers: List[str]) -> DataObject
    ```

    | Parameter | Type | Description |
    |-----------|------|-------------|
    | `tickers` | `List[str]` | Ticker symbols to lookup |

=== "DataObject"

    All API methods return a `DataObject` with these methods:

    | Method | Description |
    |--------|-------------|
    | `to_df()` | Convert to pandas DataFrame |
    | `to_csv(filename)` | Export to CSV |
    | `to_xlsx(filename)` | Export to Excel |
    | `show(n=5)` | Print first n rows |
