from unittest import mock
from unittest.mock import Mock

import pandas as pd
import pytest
from azure.storage.filedatalake import DataLakeServiceClient

from azfn_starter_kit.common.fs.datalake_file_system import DataLakeGen2FileSystemClient
from azfn_starter_kit.utilities.file_system import path_builder


class TestDataLakeGen2FileSystemClient:
    @pytest.fixture
    def mock_data_lake_service_client(self) -> DataLakeServiceClient:
        return mock.Mock(DataLakeServiceClient)

    @pytest.fixture
    def fs_client(self) -> DataLakeGen2FileSystemClient:
        return self._get_fs_client()

    def _get_fs_client(self):
        return DataLakeGen2FileSystemClient(
            "mystorage",
            "mycontainer",
            "myrawpath",
            "mytransformedpath",
            "mycomputedpath",
            "myaccesskey",
            service_client=Mock(DataLakeServiceClient),
        )

    def test_copy_files(self, fs_client):
        fs_client.fs_client.get_file_client.return_value.download_file.return_value.readall.return_value = (
            "file content"
        )
        expected_src_path = path_builder("myrawpath", "sourcefile1")
        expected_dest_path = "mycomputedpath"
        fs_client.copy_files("myrawpath", "mycomputedpath", ["sourcefile1"])

        fs_client.fs_client.get_directory_client.assert_called_once_with(expected_dest_path)
        fs_client.fs_client.get_file_client.assert_called_once_with(expected_src_path)

        fs_other = self._get_fs_client()
        fs_other.fs_client.get_file_client.return_value.download_file.return_value.readall.return_value = "file content"
        fs_client.copy_files("myrawpath", "mycomputedpath", ["sourcefile1"], source_fs=fs_other)

        fs_other.fs_client.get_file_client.assert_called()

    def test_list_files(self, fs_client):
        list_of_files = [
            "file1.txt",
            "file2.txt",
        ]

        mock_file_client1 = mock.Mock(content_length=100)
        mock_file_client1.name = "file1.txt"

        mock_file_client2 = mock.Mock(content_length=200)
        mock_file_client2.name = "file2.txt"

        fs_client.fs_client.get_paths.return_value = [mock_file_client1, mock_file_client2]

        assert fs_client.list_files("mycomputedpath") == list_of_files
        assert fs_client.list_files("mycomputedpath", pattern=r"file\d+\.txt") == list_of_files
        assert fs_client.list_files("mycomputedpath", pattern=r"file_xyz\.txt") == []

    def test_write_csv_from_list(self, fs_client):
        file_name = "myfile.csv"
        path = path_builder("mycomputedpath", file_name)
        data = [["data1", "data2", "data3"]]

        fs_client.write_csv(path, file_name, ",", data)
        fs_client.fs_client.get_directory_client.assert_called_once_with(path)
        fs_client.fs_client.get_directory_client.return_value.create_file.assert_called_once_with(file_name)
        fs_client.fs_client.get_directory_client.return_value.create_file.return_value.append_data.assert_called_once()

    def test_write_csv_from_dataframe(self, fs_client):
        file_name = "myfile.csv"
        path = path_builder("mycomputedpath", file_name)
        data = pd.DataFrame([["name1", "age1"]], columns=["Name", "Age"])

        fs_client.write_csv(path, file_name, ",", data)
        fs_client.fs_client.get_directory_client.assert_called_once_with(path)
        fs_client.fs_client.get_directory_client.return_value.create_file.assert_called_once_with(file_name)
        fs_client.fs_client.get_directory_client.return_value.create_file.return_value.append_data.assert_called_once()

    def test_write_parquet(self, fs_client):
        file_name = "myfile.parquet"
        path = path_builder("mycomputedpath", file_name)
        df_ = pd.DataFrame({"data1": [1, 2, 3], "data2": [4, 5, 6]})

        fs_client.write_parquet(path, file_name, df_)
        fs_client.fs_client.get_directory_client.assert_called_once_with(path)
        fs_client.fs_client.get_directory_client.return_value.create_file.assert_called_once_with(file_name)
        fs_client.fs_client.get_directory_client.return_value.create_file.return_value.append_data.assert_called_once()

    def test_write_file(self, fs_client):
        file_name = "myfile.txt"
        path = path_builder("mycomputedpath", file_name)
        data = "content"

        fs_client.write_file(path, file_name, data)
        fs_client.fs_client.get_directory_client.assert_called_once_with(path)
        fs_client.fs_client.get_directory_client.return_value.create_file.assert_called_once_with(file_name)
        fs_client.fs_client.get_directory_client.return_value.create_file.return_value.append_data.assert_called_once()

    def test_read_csv(self, fs_client):
        path = path_builder("mycomputedpath", "myfile.csv")
        data = pd.DataFrame({"data1": [1, 2, 3], "data2": [4, 5, 6]})
        with mock.patch("pandas.read_csv", return_value=data):
            resulted_data = fs_client.read_csv(path, "myfile.csv", separator=",")
            assert resulted_data.equals(data)

    def test_read_parquet(self, fs_client):
        path = path_builder("mycomputedpath", "myfile.parquet")
        data = pd.DataFrame({"data1": [1, 2, 3], "data2": [4, 5, 6]})
        with mock.patch("pandas.read_parquet", return_value=data):
            resulted_data = fs_client.read_parquet(path, "myfile.parquet")
            assert resulted_data.equals(data)

    def test_read_json(self, fs_client):
        path = path_builder("mycomputedpath", "myfile.json")
        data = pd.DataFrame({"data1": [1, 2, 3], "data2": [4, 5, 6]})
        with mock.patch("pandas.read_json", return_value=data):
            resulted_data = fs_client.read_json(path, "myfile.json")
            assert resulted_data.equals(data)

    def test_delete_file(self, fs_client):
        fs_client.delete_file("test_dir", "test_file.txt")

        fs_client.fs_client.get_file_client.assert_called_once_with("test_dir/test_file.txt")
        fs_client.fs_client.get_file_client.return_value.delete_file.assert_called_once()

    def test_delete_directory(self, fs_client):
        fs_client.delete_directory("test_dir")
        fs_client.fs_client.get_directory_client.assert_called_once_with("test_dir")
