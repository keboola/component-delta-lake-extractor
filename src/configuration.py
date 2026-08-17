from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field, ValidationError, computed_field, field_validator
from keboola.component.exceptions import UserException


class AccessMethod(str, Enum):
    unity_catalog = "unity_catalog"
    direct_storage = "direct_storage"


class AuthType(str, Enum):
    pat = "pat"
    service_principal = "service_principal"


class DataSelectionMode(str, Enum):
    all_data = "all_data"
    select_columns = "select_columns"
    custom_query = "custom_query"
    workspace_query = "workspace_query"


class LoadType(str, Enum):
    full_load = "full_load"
    incremental_load = "incremental_load"


class Source(BaseModel):
    container_name: str = ""
    blob_name: str = ""

    catalog: str = ""
    schema_name: str = ""
    table: str = ""


class DataSelection(BaseModel):
    mode: DataSelectionMode = Field(default=DataSelectionMode.all_data)
    columns: list[str] = Field(default_factory=list)
    query: str = ""
    warehouse_id: str = ""


class Destination(BaseModel):
    preserve_insertion_order: bool = True
    parquet_output: bool = False
    file_name: str = ""
    table_name: str = ""
    load_type: LoadType = Field(default=LoadType.incremental_load)
    primary_key: list[str] = Field(default_factory=list)

    @computed_field
    @property
    def incremental(self) -> bool:
        return self.load_type in (LoadType.incremental_load)


class Configuration(BaseModel):
    access_method: AccessMethod = Field(default=AccessMethod.direct_storage)
    provider: str = None
    unity_catalog_url: str = None
    auth_type: AuthType = Field(default=AuthType.pat)
    unity_catalog_token: str = Field(alias="#unity_catalog_token", default=None)
    unity_catalog_client_id: str = None
    unity_catalog_client_secret: str = Field(alias="#unity_catalog_client_secret", default=None)
    abs_account_name: str = None
    abs_sas_token: str = Field(alias="#abs_sas_token", default=None)
    # Non-standard port of the Azure storage endpoint (private endpoint / gateway / emulator).
    # Left unset for the default 443 - no port is then injected anywhere.
    abs_port: Optional[int] = Field(default=None, ge=1, le=65535)
    aws_region: str = None
    aws_key_id: str = None
    aws_key_secret: str = Field(alias="#aws_key_secret", default=None)
    gcp_hmac_id: str = None
    gcp_hmac_secret: str = Field(alias="#gcp_hmac_secret", default=None)
    source: Source
    data_selection: DataSelection
    destination: Destination
    debug: bool = False
    threads: int = 1
    max_memory: int = 256

    @field_validator("abs_port", mode="before")
    @classmethod
    def _empty_port_to_none(cls, value):
        # The UI submits an empty string when the (optional) port field is left blank.
        if value == "" or value is None:
            return None
        return value

    def __init__(self, **data):
        try:
            super().__init__(**data)
        except ValidationError as e:
            error_messages = [f"{err['loc'][0]}: {err['msg']}" for err in e.errors()]
            raise UserException(f"Validation Error: {', '.join(error_messages)}")
