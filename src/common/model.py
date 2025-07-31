import dataclasses
import datetime
from dataclasses import dataclass


@dataclass
class Config:
    start_date: datetime.date
    end_date: datetime.date
    universe: list[str]
    model_state_features: list[str]
    models: list[str]
    optimizers: list[str]

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
