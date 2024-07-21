from pydantic import BaseModel

from azfn_starter_kit.utilities.file_system import path_builder


class DataLakeConfig(BaseModel):
    STORAGE_NAME: str = "weatherefrei"
    CONTAINER_NAME: str = "app"
    RAW_PATH: str = path_builder("exec", "internal", "raw")
    TRANSFORMED_PATH: str = path_builder("exec", "internal", "transformed")
    COMPUTED_PATH: str = path_builder("exec", "exposed", "computed")
    #mettre sa propre clé Azure
    ACCOUNT_KEY: str = "account_key"
