from unittest import mock

import pandas as pd
import pytest
from azure.identity import DefaultAzureCredential

from azfn_starter_kit.common.db.database import DatabaseEngine, SQLQueryBuilder
from azfn_starter_kit.config.database_config import AzureSQLConfig


@pytest.fixture(autouse=True)
def token_mock(monkeypatch):
    def mock_get_token(self, *args, **kwargs):
        class DummyToken:
            def __init__(self, cool):
                self.token = cool

        return DummyToken("abc")

    monkeypatch.setattr(DefaultAzureCredential, "get_token", mock_get_token)


@pytest.fixture
def logger_mock():
    logger = mock.patch("azfn_starter_kit.utilities.logger.get_logger")
    return logger


# Sample data (pour tester)
@pytest.fixture(scope="module")
def sample_data():
    data = {
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
        "age": [25, 30, 27, 22, 35],
    }
    return pd.DataFrame(data)


class TestDatabaseEngine(object):
    @pytest.fixture
    def database_engine(self, logger_mock):
        config = AzureSQLConfig(server="test_server", database="test_db")
        return DatabaseEngine(config)

    def test_build_insert_query(self):
        # GIVEN
        builder = SQLQueryBuilder()
        source_table_name = "tmp_table"
        target_table_name = "table"
        meta_data = {
            "source_file_name": {"column": "SOURCE_FILE_NAME", "value": "source_file.parquet"},
            "insert_date": {"column": "INSERT_DATE", "value": "2023-12-01"},
        }

        table_columns = ["NAME", "PROJECT_ID", "START_DATE", "STATUS"]

        # WHEN
        insert_query = builder.insert(
            source_table_name,
            target_table_name,
            table_columns,
            meta_data,
        )

        # THEN
        assert (
            insert_query.text == "\n            INSERT INTO table (NAME, PROJECT_ID, START_DATE, STATUS , INSERT_DATE,"
            " SOURCE_FILE_NAME)\n            SELECT NAME, PROJECT_ID, START_DATE, STATUS , 2023-12-01, source_file.parquet"
            "\n            FROM tmp_table src;\n        "
        )

    def test_build_insert_if_not_exist_query(self):
        # GIVEN
        builder = SQLQueryBuilder()
        source_table_name = "tmp_table"
        target_table_name = "table"
        meta_data = {
            "source_file_name": {"column": "SOURCE_FILE_NAME", "value": "source_file.parquet"},
            "insert_date": {"column": "INSERT_DATE", "value": "2023-12-01"},
        }

        primary_key = ["PROJECT_ID"]

        table_columns = [
            "PROJECT_ID",
            "START_DATE",
            "STATUS",
        ]

        # WHEN
        insert_query = builder.insert_if_not_exist(
            source_table_name,
            target_table_name,
            primary_key,
            table_columns,
            meta_data,
        )

        # THEN
        assert (
            insert_query.text == "\n        INSERT INTO table (src.PROJECT_ID, src.START_DATE, src.STATUS , "
            "INSERT_DATE, SOURCE_FILE_NAME)\n        SELECT src.PROJECT_ID, src.START_DATE, src.STATUS , "
            "2023-12-01, source_file.parquet\n        FROM tmp_table src\n        "
            "LEFT JOIN table trg\n        ON concat ('-', src.PROJECT_ID) = concat ('-', trg.PROJECT_ID)"
            "\n        WHERE trg.PROJECT_ID IS NULL\n        "
        )

    def test_build_update_query(self):
        # GIVEN
        builder = SQLQueryBuilder()
        source_table_name = "tmp_table"
        target_table_name = "table"
        meta_data = {
            "source_file_name": {"column": "SOURCE_FILE_NAME", "value": "source_file.parquet"},
            "insert_date": {"column": "INSERT_DATE", "value": "2023-12-01"},
        }

        primary_key = ["PROJECT_ID"]

        table_columns = [
            "PROJECT_ID",
            "START_DATE",
            "STATUS",
        ]

        # WHEN
        update_query = builder.update(
            source_table_name,
            target_table_name,
            primary_key,
            table_columns,
            meta_data,
        )

        # THEN
        assert (
            update_query.text == "\n            UPDATE table\n            SET START_DATE = src.START_DATE,STATUS = "
            "src.STATUS {'source_file_name': {'column': 'SOURCE_FILE_NAME', 'value': 'source_file.parquet'}, "
            "'insert_date': {'column': 'INSERT_DATE', 'value': '2023-12-01'}}\n            FROM tmp_table  "
            "src\n            INNER JOIN  table  trg\n            ON concat ('-', src.PROJECT_ID) = "
            "concat ('-', trg.PROJECT_ID)\n            WHERE HASHBYTES('SHA1',CONCAT ('-', src.PROJECT_ID,  "
            "src.START_DATE,  src.STATUS)) != HASHBYTES('SHA1',CONCAT ('-', trg.PROJECT_ID,  "
            "trg.START_DATE,  trg.STATUS));\n        "
        )

    def test_sql_to_sql_insert(self, database_engine, sample_data):
        with (
            mock.patch("pandas.DataFrame.to_sql") as mock_to_sql,
            mock.patch("azfn_starter_kit.common.db.database.SQLQueryBuilder.insert") as mock_build_insert_query,
            mock.patch("azfn_starter_kit.common.db.database.DatabaseEngine.sql_drop_table") as mock_sql_drop_table,
            mock.patch("sqlalchemy.engine.base.Engine.begin") as mock_engine_begin,
        ):
            database_engine.df_to_sql(sample_data, "insert_test_table", [], "insert", columns=["name", "age"])
            mock_to_sql.assert_called_once()
            mock_engine_begin.assert_called_once()
            mock_build_insert_query.assert_called_once()
            mock_sql_drop_table.assert_called_once()

    def test_sql_to_sql_update(self, database_engine, sample_data):
        with (
            mock.patch("pandas.DataFrame.to_sql") as mock_to_sql,
            mock.patch("azfn_starter_kit.common.db.database.DatabaseEngine.sql_drop_table") as mock_sql_drop_table,
            mock.patch("azfn_starter_kit.common.db.database.SQLQueryBuilder.update") as mock_build_update_query,
            mock.patch("sqlalchemy.engine.base.Engine.begin") as mock_engine_begin,
        ):
            database_engine.df_to_sql(sample_data, "insert_test_table", [], "update", columns=["name", "age"])
            mock_to_sql.assert_called_once()
            mock_engine_begin.assert_called_once()
            mock_build_update_query.assert_called_once()
            mock_sql_drop_table.assert_called_once()

    def test_sql_to_sql_upsert(self, database_engine, sample_data):
        with (
            mock.patch("pandas.DataFrame.to_sql") as mock_to_sql,
            mock.patch("azfn_starter_kit.common.db.database.DatabaseEngine.sql_drop_table") as mock_sql_drop_table,
            mock.patch(
                "azfn_starter_kit.common.db.database.DatabaseEngine.sql_to_sql_update"
            ) as mock_sql_to_sql_update,
            mock.patch(
                "azfn_starter_kit.common.db.database.DatabaseEngine.sql_to_sql_insert"
            ) as mock_sql_to_sql_insert,
        ):
            database_engine.df_to_sql(sample_data, "insert_test_table", [], "upsert", columns=["name", "age"])
            mock_to_sql.assert_called_once()
            mock_sql_to_sql_insert.assert_called_once()
            mock_sql_to_sql_update.assert_called_once()
            mock_sql_drop_table.assert_called_once()
