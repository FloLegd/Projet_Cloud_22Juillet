from datetime import datetime
from typing import Literal, Optional

import pandas as pd
import sqlalchemy as db
from sqlalchemy import create_engine, text
from sqlalchemy.sql.elements import TextClause

from azfn_starter_kit.config.database_config import AzureSQLConfig
from azfn_starter_kit.utilities.logger import get_logger

LoadingAction = Literal["update", "upsert", "insert"]

SQL_ALIAS_SOURCE = "src"
SQL_ALIAS_TARGET = "trg"


class DatabaseClient:
    """SQLAlchemy and Pandas based client for AzureSQL"""

    def __init__(self, config: AzureSQLConfig):
        self.logger = get_logger(__name__)
        self.engine = db.create_engine(config.connection_string())

    def read_sql(self, query: str) -> pd.DataFrame:
        """Execute an SQL query and return the result as a DataFrame."""

        with self.engine.begin() as connection:
            return pd.read_sql(query, con=connection)

    def to_sql(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: Optional[str] = None,
        chunksize: Optional[int] = 1000,
        dtype: Optional[dict] = None,
        if_exists: str = "replace",
        index: bool = False,
    ):
        """Write a DataFrame to a SQL table."""

        df.to_sql(
            name=table_name,
            con=self.engine,
            schema=schema,
            index=index,
            if_exists=if_exists,
            chunksize=chunksize,
            dtype=dtype,
        )


class DatabaseEngine:
    """
    A utility class for creating and managing a SQLAlchemy database engine.

    This class handles the creation of a AzureSQL client with support of
    insert, update, upsert, truncate and delete operations.
    """

    def __init__(self, config: AzureSQLConfig):
        self.logger = get_logger(__name__)
        self.client = DatabaseClient(config)
        self.engine = self.client.engine


    def df_to_sql(
        self,
        source_df: pd.DataFrame,
        target_table_name: str,
        primary_key: list,
        loading_action: LoadingAction,
        columns: Optional[list] = None,
        data_type: Optional[dict] = None,
        tmp_table_schema: Optional[str] = None,
        tmp_table_chunksize: Optional[int] = 1000,
        metadata: Optional[dict] = None,
    ):
        """Update the target table in the database using data from a DataFrame, through a temporary table.

        Args:
            source_df (DataFrame): Source DataFrame containing data to be updated.
            target_table_name (str): Name of the target table to be updated.
            primary_key (list): List of primary key column names for update matching.
            loading_action (LoadingAction): Type of DB action
            columns (list, optional): List of columns to update. Defaults to None (update all matching columns).
            data_type (dict, optional): Dictionary specifying data types for columns. Defaults to None.
            tmp_table_schema (str, optional): Schema for temporary table. Defaults to None.
            tmp_table_chunksize (int, optional): Chunk size for data insertion. Defaults to 1000.
            metadata (dict, optional): Metadata dictionary. Defaults to {}.
        """
        tmp_table_name = f'tmp_{target_table_name}_{datetime.now().strftime("%Y%m%d%H%M%S%f")}'

        if columns is None:
            sql_table = db.Table(target_table_name, db.MetaData(), autoload_with=self.engine)
            columns = [column.name for column in sql_table.columns if column.name in source_df.columns]
        else:
            columns = [column for column in columns if column in source_df.columns]
        source_df = source_df[columns]

        self.client.to_sql(
            df=source_df,
            table_name=tmp_table_name,
            schema=tmp_table_schema,
            chunksize=tmp_table_chunksize,
            dtype=data_type,
            if_exists="replace",
            index=False,
        )

        match loading_action:
            case "update":
                self.sql_to_sql_update(tmp_table_name, target_table_name, columns, primary_key, metadata)
            case "upsert":
                self.sql_to_sql_upsert(tmp_table_name, target_table_name, columns, primary_key, metadata)
            case "insert":
                self.sql_to_sql_insert(tmp_table_name, target_table_name, columns, primary_key, metadata)
            case _:
                raise ValueError("Invalid loading action")

        self.sql_drop_table(table_name=tmp_table_name)

    def sql_to_sql_insert(
        self,
        source_table_name: str,
        target_table_name: str,
        table_columns: list,
        primary_key: Optional[list] = None,
        metadata: Optional[dict] = None,
    ):
        """Insert data from one SQL table into another SQL table if matching rows do not exist.

        Args:
            source_table_name (str): Name of the source SQL table.
            target_table_name (str): Name of the target SQL table.
            primary_key (list): List of primary key column names for checking existence.
            table_columns (list, optional): List of columns to insert. Defaults to None (insert all columns).
            metadata (dict, optional): Metadata dictionary. Defaults to {}.
        """

        builder = SQLQueryBuilder()
    
        if primary_key:
            insert_query = builder.insert_if_not_exist(
                source_table_name, target_table_name, primary_key, table_columns, metadata
            )
        else:
            insert_query = builder.insert(source_table_name, target_table_name, table_columns, metadata)

        self.logger.debug(insert_query)
      
        with self.engine.begin() as session:
            session.execute(insert_query)
            session.commit()

    def sql_to_sql_update(
        self,
        source_table_name: str,
        target_table_name: str,
        table_columns: list,
        primary_key: list,
        metadata: Optional[dict] = None,
    ):
        """Update data in the target SQL table using data from the source SQL table.

        Args:
            source_table_name (str): Name of the source SQL table.
            target_table_name (str): Name of the target SQL table.
            primary_key (list): List of primary key column names for update matching.
            table_columns (list, optional): List of columns to update. Defaults to None (update all matching columns).
            metadata (dict, optional): Metadata dictionary. Defaults to {}.
        """

       
        metadata = metadata or {}
        meta_data: str = (
            "",
            ", "
            + ",".join([f"{val['column']} = {val['value']}" for key, val in metadata.items() if key != "insert_date"]),
        )[bool(metadata)]

        builder = SQLQueryBuilder()
        update_query = builder.update(source_table_name, target_table_name, primary_key, table_columns, meta_data)

        self.logger.debug(update_query)

        with self.engine.begin() as session:
            session.execute(update_query)
            session.commit()

    def sql_to_sql_upsert(
        self,
        source_table_name: str,
        target_table_name: str,
        table_columns: list,
        primary_key: list,
        metadata: Optional[dict] = None,
    ):
        """Upsert (insert or update) data from the source SQL table into the target SQL table.

        Args:
            source_table_name (str): Name of the source SQL table.
            target_table_name (str): Name of the target SQL table.
            primary_key (list): List of primary key column names for upsert matching.
            table_columns (list, optional): List of columns to upsert. Defaults to None (upsert all matching columns).
            metadata (dict, optional): Metadata dictionary. Defaults to {}.
        """
        # update
        self.sql_to_sql_update(source_table_name, target_table_name, table_columns, primary_key, metadata=metadata)

        # insert si besoin
        self.sql_to_sql_insert(
            source_table_name, target_table_name, table_columns, primary_key=primary_key, metadata=metadata
        )

    def sql_truncate_table(self, table_name: str):
        """Truncate (remove all rows from) a specified SQL table.

        Args:
            table_name (str): Name of the SQL table to be truncated.
        """
        truncate_query = text(f"TRUNCATE TABLE {table_name};")

        with self.engine.begin() as session:
            session.execute(truncate_query)
            session.commit()

    def sql_drop_table(self, table_name: str):
        """Drop (delete) a specified SQL table from the database.

        Args:
            table_name (str): Name of the SQL table to be dropped.
        """
        drop_query = text(f"DROP TABLE {table_name};")

        with self.engine.begin() as session:
            session.execute(drop_query)
            session.commit()

    def sql_to_df(self, table_name: str, columns: Optional[list] = None, clause_where_sql: Optional[str] = None):
        """Execute an SQL query to retrieve data from a table and return the result as a DataFrame.

        Args:
            table_name (str): Name of the SQL table to query.
            columns (list, optional): List of column names to retrieve. Defaults to None (all columns).
            clause_where_sql (str, optional): WHERE clause to filter rows. Defaults to None.

        Returns:
            pandas.DataFrame: A DataFrame containing the queried data.
        """
        columns_str = ", ".join(columns) if columns is not None else "*"
        clause_where_sql = f"WHERE {clause_where_sql}" if clause_where_sql is not None else ""
        query = f"""SELECT {columns_str}
                    FROM {table_name}
                    {clause_where_sql}
                """

        return self.client.read_sql(query)


class SQLQueryBuilder:
    """A helper class for building SQL queries for data loading operations.
    Beware, this is legacy code, it is not advised to use this as is.
    """

    def __init__(self):
        self.logger = get_logger(__name__)

    def _create_concat_string(self, prefix, items):
        return "concat ('-', " + ", '-', ".join([prefix + item for item in items]) + ")"

    def _compute_hash_key(self, prefix, items):
        return "HASHBYTES('SHA1',CONCAT ('-', " + ",  ".join([prefix + item for item in items]) + "))"

    def _create_metadata(self, metadata: Optional[dict], col_name: str, filtered_col: str):
        metadata = metadata or {}
        return ("", ", " + ", ".join([val[col_name] for key, val in sorted(metadata.items()) if key != filtered_col]))[
            bool(metadata)
        ]

    def insert(
        self, source_table_name: str, target_table_name: str, table_columns: list, metadata: Optional[dict]
    ) -> TextClause:
        query = """
            INSERT INTO {target} ({columns} {meta_data_column})
            SELECT {columns} {meta_data_value}
            FROM {source} {alias};
        """.format(
            target=target_table_name,
            source=source_table_name,
            alias=SQL_ALIAS_SOURCE,
            columns=", ".join(table_columns),
            meta_data_column=self._create_metadata(metadata, "column", "update_date"),
            meta_data_value=self._create_metadata(metadata, "value", "update_date"),
        )

        return text(query)

    def insert_if_not_exist(
        self,
        source_table_name: str,
        target_table_name: str,
        primary_key: list,
        table_columns: list,
        metadata: Optional[dict],
    ) -> TextClause:
        query = """
        INSERT INTO {target} ({columns} {meta_data_column})
        SELECT {columns} {meta_data_value}
        FROM {source} {alias_source}
        LEFT JOIN {target} {alias_target}
        ON {source_key} = {target_key}
        WHERE {clause_where}
        """.format(
            target=target_table_name,
            source=source_table_name,
            alias_source=SQL_ALIAS_SOURCE,
            alias_target=SQL_ALIAS_TARGET,
            source_key=self._create_concat_string(f"{SQL_ALIAS_SOURCE}.", primary_key),
            target_key=self._create_concat_string(f"{SQL_ALIAS_TARGET}.", primary_key),
            columns=", ".join([f"{SQL_ALIAS_SOURCE}.{column}" for column in table_columns]),
            meta_data_column=self._create_metadata(metadata, "column", "update_date"),
            meta_data_value=self._create_metadata(metadata, "value", "update_date"),
            clause_where=" and ".join([f"{SQL_ALIAS_TARGET}.{pk} IS NULL" for pk in primary_key]),
        )

        return text(query)

    def update(
        self, source_table_name: str, target_table_name: str, primary_key: list, table_columns: list, metadata: str
    ) -> TextClause:
        query = """
            UPDATE {target}
            SET {update_clause} {meta_data_update_clause}
            FROM {source}  {alias_source}
            INNER JOIN  {target}  {alias_target}
            ON {source_key} = {target_key}
            WHERE {source_hash_key} != {target_hash_key};
        """.format(
            target=target_table_name,
            source=source_table_name,
            alias_source=SQL_ALIAS_SOURCE,
            alias_target=SQL_ALIAS_TARGET,
            update_clause=",".join(
                [column + f" = {SQL_ALIAS_SOURCE}." + column for column in table_columns if column not in primary_key]
            ),
            source_key=self._create_concat_string(f"{SQL_ALIAS_SOURCE}.", primary_key),
            target_key=self._create_concat_string(f"{SQL_ALIAS_TARGET}.", primary_key),
            source_hash_key=self._compute_hash_key(f"{SQL_ALIAS_SOURCE}.", table_columns),
            target_hash_key=self._compute_hash_key(f"{SQL_ALIAS_TARGET}.", table_columns),
            meta_data_update_clause=metadata,
        )

        return text(query)
