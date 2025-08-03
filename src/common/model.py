import dataclasses
import datetime
from dataclasses import dataclass


@dataclass
class IBGatewayConfig:
    host: str
    port: int
    client_id: int


@dataclass
class Config:
    start_date: datetime.date
    end_date: datetime.date
    universe: list[str]
    model_state_features: list[str]
    models: list[str]
    optimizers: list[str]
    ib_gateway: IBGatewayConfig

    def __post_init__(self):
        if isinstance(self.ib_gateway, dict):
            self.ib_gateway = IBGatewayConfig(**self.ib_gateway)
        elif not isinstance(self.ib_gateway, IBGatewayConfig):
            raise TypeError("ib_gateway must be IBGatewayConfig or dict")

    def dump_to_gcs(self, gcs_url: str):
        """
        Serialize the Config instance to a JSON string and save it to a GCS URL.
        """
        import json
        from gcsfs import GCSFileSystem

        fs = GCSFileSystem()
        json_string = json.dumps(dataclasses.asdict(self), default=str, indent=4)

        with fs.open(gcs_url, 'w') as f:
            f.write(json_string)
