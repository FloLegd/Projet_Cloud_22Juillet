import os
import struct
from urllib.parse import quote_plus

from azure.identity import UsernamePasswordCredential
from pydantic import BaseModel



class AzureSQLConfig(BaseModel):
    server: str
    database: str
    port: int = 1433
    username: str
    password: str

    @classmethod
    def from_env(cls) -> "AzureSQLConfig":
        return cls(
            server="weatherefrei.database.windows.net",
            database="weatherefrei",
            username="weatherefrei",
            password="Password1"
        )

    def connection_string(self) -> str:
        quoted = quote_plus(
            f"Server={self.server};Port={self.port};Database={self.database};UID={self.username};PWD={self.password}"
        )
        return f"mssql+pyodbc:///?odbc_connect={quoted}"


    def connect_args(self):
        """Build connect args."""
        credential = UsernamePasswordCredential(
            client_id="04b07795-8ddb-461a-bbee-02f9e1bf7b46",  # client_id de base Microsoft Azure SQL Database
            username=self.username,
            password=self.password,
        )
        token = credential.get_token("https://database.windows.net/.default").token.encode("utf-16-le")
        return {"attrs_before": {1256: struct.pack(f"<I{len(token)}s", len(token), token)}, "connect_timeout": 10}
