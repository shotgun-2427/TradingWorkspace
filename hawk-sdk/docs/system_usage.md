# System

## Lookup Hawk IDs

```python
from hawk_sdk.api import System

system = System()

response = system.get_hawk_ids(
    tickers=["CL00-USA", "JBT00-OSE", "SFC00-USA", "FGBS00-EUR"]
)

df = response.to_df()
response.show()
response.to_csv("hawk_ids.csv")
```
