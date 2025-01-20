import logging
from enum import Enum
from pydantic import BaseModel, Field, ValidationError, computed_field
from keboola.component.exceptions import UserException


class DataSelectionMode(str, Enum):
    all_data = "all_data"
    select_columns = "select_columns"
    custom_query = "custom_query"


class LoadType(str, Enum):
    full_load = "full_load"
    incremental_load = "incremental_load"


class Auth(BaseModel):
    account_name: str = Field()
    sas_token: str = Field(alias="#sas_token")


class Source(BaseModel):
    container_name: str = Field()
    blob_name: str = Field()


class DataSelection(BaseModel):
    mode: DataSelectionMode = Field(default=DataSelectionMode.all_data)
    columns: list[str] = Field(default_factory=list)
    query: str = Field(default=None)


class Destination(BaseModel):
    preserve_insertion_order: bool = True
    parquet_output: bool = False
    file_name: str = Field(default=None)
    table_name: str = Field(default=None)
    load_type: LoadType = Field(default=LoadType.incremental_load)
    primary_key: list[str] = Field(default_factory=list)

    @computed_field
    def incremental(self) -> bool:
        return self.load_type in (LoadType.incremental_load)


class Configuration(BaseModel):
    auth: Auth
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

        if self.debug:
            logging.debug("Component will run in Debug mode")
