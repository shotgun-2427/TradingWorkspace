"""
@description: Dataclasses for readability.
@author: Rithwik Babu
"""
from pydantic import BaseModel, field_validator

from pipeline.common.enums import FieldType, IdentifierType, AssetClass


# Configuration Models
class GCPConfig(BaseModel):
    project: str
    dataset: str


class HawkIdentifierModel(BaseModel, validate_assignment=True):
    hawk_id: int
    asset_class: AssetClass
    id_type: IdentifierType
    value: str


class HawkIdentifiersConfig(BaseModel):
    hawk_identifiers: list[HawkIdentifierModel]

    @field_validator("hawk_identifiers")
    @classmethod
    def validate_unique_hawk_ids(cls, value: list[HawkIdentifierModel]) -> list[HawkIdentifierModel]:
        seen_ids = set()
        for identifier in value:
            if identifier.hawk_id in seen_ids:
                raise ValueError(f"Duplicate hawk_id: {identifier.hawk_id}")
            seen_ids.add(identifier.hawk_id)
        return value


class FieldModel(BaseModel, validate_assignment=True):
    field_id: int
    field_name: str
    field_type: FieldType


class FieldsConfig(BaseModel):
    fields: list[FieldModel]

    @field_validator("fields")
    @classmethod
    def validate_unique_field_ids(cls, value: list[FieldModel]) -> list[FieldModel]:
        seen_ids = set()
        for field in value:
            if field.field_id in seen_ids:
                raise ValueError(f"Duplicate field_id: {field.field_id}")
            seen_ids.add(field.field_id)
        return value
