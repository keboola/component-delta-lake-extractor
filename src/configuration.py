from enum import Enum
from pydantic import BaseModel, Field, ValidationError, computed_field
from keboola.component.exceptions import UserException


class AccessMethod(str, Enum):
    unity_catalog = "unity_catalog"
    direct_storage = "direct_storage"


class DataSelectionMode(str, Enum):
    all_data = "all_data"
    select_columns = "select_columns"
    custom_query = "custom_query"


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
    unity_catalog_token: str = Field(alias="#unity_catalog_token", default=None)
    abs_account_name: str = None
    abs_sas_token: str = Field(alias="#abs_sas_token", default=None)
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

    def __init__(self, **data):
        try:
            super().__init__(**data)
        except ValidationError as e:
            error_messages = [f"{err['loc'][0]}: {err['msg']}" for err in e.errors()]
            raise UserException(f"Validation Error: {', '.join(error_messages)}")
