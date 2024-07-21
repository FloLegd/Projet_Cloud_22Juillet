import io
import re
from typing import List, Optional, Union

import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import DefaultAzureCredential
from azfn_starter_kit.utilities.file_system import path_builder
from azfn_starter_kit.utilities.logger import get_logger


class DataLakeGen2FileSystemClient:
    """
    Client for interacting with Azure Data Lake Storage Gen2 file system.

    Args:
        storage_name (str): The name of the Azure Data Lake Storage Gen2 account.
        container_name (str): The name of the container (often referred as file system) within the storage account.
        raw_path (str): The raw path to be used.
        computed_path (str): The computed path derived from the raw path.
        service_client (Optional[DataLakeServiceClient]): The service client to be used.
            If not provided, a new instance will be created.
        storage_account_key (Optional[str]): The access key for the storage account.
            If provided, it will be used for authentication.

    Attributes:
        storage_name (str): The name of the Azure Data Lake Storage Gen2 account.
        raw_path (str): The raw path.
        computed_path (str): The computed path.
        fs_client (FileSystemClient): The file system client for interacting with Data Lake.
    """

    BASE_URL = "https://{}.dfs.core.windows.net"

    def __init__(
        self,
        storage_name: str,
        container_name: str,
        raw_path: str,
        transformed_path: str,
        computed_path: str,
        storage_account_key: Optional[str] = None,
        service_client: Optional[DataLakeServiceClient] = None,
    ):
        self.logger = get_logger(__name__)
        self.storage_name = storage_name
        self.raw_path = raw_path
        self.transformed_path = transformed_path
        self.computed_path = computed_path

        if service_client is None:
            account_url = self.BASE_URL.format(storage_name)
            if storage_account_key:
                service_client = DataLakeServiceClient(account_url, credential={"account_name": storage_name, "account_key": storage_account_key})
            else:
                credential = DefaultAzureCredential()
                service_client = DataLakeServiceClient(account_url, credential=credential)
        self.fs_client = service_client.get_file_system_client(container_name)

    def copy_files(
        self,
        src_path: str,
        dest_path: str,
        files_name: list,
        source_fs: Optional["DataLakeGen2FileSystemClient"] = None,
    ) -> None:
        """Copy specified files from a source directory to a destination directory.

        Args:
            src_path (str): Source directory path.
            dest_path (str): Destination directory path.
            files_name (list): List of file names to copy.
            source_fs (DataLakeGen2FileSystemClient, optional): Source file system client if copying
            from another file system. Defaults to None.
        """
        directory_client = self.fs_client.get_directory_client(dest_path)
        for file in files_name:
            if source_fs:
                data_ = source_fs.fs_client.get_file_client(path_builder(src_path, file)).download_file()
            else:
                data_ = self.fs_client.get_file_client(path_builder(src_path, file)).download_file()
            data = data_.readall()
            file_client = directory_client.create_file(str(file))
            file_client.append_data(data, offset=0, length=len(data))
            file_client.flush_data(len(data))

    def list_files(
        self, path: str, pattern: Optional[str] = None, min_blob_size: int = 0, descending_sort: bool = False
    ) -> List[str]:
        """Get a list of file names in the specified directory.

        Args:
            path (str): Directory path to search for files.
            pattern (str, optional): Regular expression pattern to match file names. Defaults to None.
            min_blob_size (int, optional): Minimum size of the blob content in bytes for a
            descending_sort(bool): If true, sort files in desc order
            file to be included in the list. Defaults to 0.

        Returns:
            List[str]: A list of file names in the directory that match the pattern and minimum blob size criteria.
        """
        directory_content = [
            p.name.removeprefix(path + "/")
            for p in self.fs_client.get_paths(path=path)
            if p.content_length >= min_blob_size
        ]
        if pattern is not None:
            directory_content = [path for path in directory_content if re.match(pattern, path)]
        return sorted(directory_content, reverse=descending_sort)

    def write_csv(self, path: str, file_name: str, separator: str, data: Union[pd.DataFrame, list[list[str]]]) -> None:
        """Put the contents of a DataFrame or list of lists as a CSV file in the specified directory.

        Args:
            path (str): Directory path where the CSV file will be created.
            file_name (str): Name of the CSV file.
            separator (str): Separator to use for CSV file (e.g., ',' or ';').
            data (Union[pd.DataFrame, list[list[str]]]): Data to write to the CSV file.
            Can be either a pandas DataFrame or a list of lists.

        Returns:
            None
        """
        directory_client = self.fs_client.get_directory_client(path)
        file_client = directory_client.create_file(file_name)

        if isinstance(data, pd.DataFrame):
            file_contents = data.to_csv(index=False, sep=separator)
        else:
            file_contents = ""
            for line in data:
                file_contents += separator.join(line) + "\n"
            file_contents = file_contents.rstrip("\n")

        file_client.append_data(data=file_contents, offset=0, length=len(file_contents))
        file_client.flush_data(len(file_contents))

    def read_csv(self, path: str, file_name: str, separator: str = ",", **kwargs) -> pd.DataFrame:
        """Read the contents of a CSV file from the specified directory and return as a DataFrame.

        Args:
            path (str): Directory path where the CSV file is located.
            file_name (str): Name of the CSV file.
            separator (str): Separator used in the CSV file (e.g., ',' or ';').
            **kwargs: Additional keyword arguments to pass to pandas read_csv function.

        Returns:
            pd.DataFrame: DataFrame containing the CSV file contents.
        """
        file_client = self.fs_client.get_file_client(path_builder(path, file_name))
        stream = io.BytesIO()
        file_client.download_file().readinto(stream)
        stream.seek(0)

        df_result = pd.read_csv(filepath_or_buffer=stream, sep=separator, engine="python", **kwargs)

        return df_result

    def write_parquet(self, path: str, file_name: str, data_frame: pd.DataFrame) -> None:
        """Write a DataFrame's contents into a Parquet file in the specified directory.

        Args:
            path (str): Directory path where the Parquet file will be created.
            file_name (str): Name of the Parquet file.
            data_frame (pd.DataFrame): DataFrame containing the data to be written.

        Returns:
            None
        """
        directory_client = self.fs_client.get_directory_client(path)
        file_client = directory_client.create_file(file_name)
        file_contents = data_frame.to_parquet(use_dictionary=False)
        file_client.append_data(data=file_contents, offset=0, length=len(file_contents))
        file_client.flush_data(len(file_contents))

    def read_parquet(self, path: str, file_name: str, **kwargs) -> pd.DataFrame:
        """Read the contents of a Parquet file as a DataFrame.

        Args:
            path (str): Directory path where the Parquet file is located.
            file_name (str): Name of the Parquet file.
            **kwargs: Additional keyword arguments to pass to pd.read_parquet().

        Returns:
            pd.DataFrame: DataFrame containing the data from the Parquet file.
        """
        file_client = self.fs_client.get_file_client(path_builder(path, file_name))
        stream = io.BytesIO()
        file_client.download_file().readinto(stream)
        stream.seek(0)
        df_result = pd.read_parquet(path=stream, **kwargs)
        return df_result

    def delete_file(self, path: str, file_name: str) -> None:
        """Delete a file from the specified directory in the Data Lake Gen2 file system.

        Args:
            path (str): Directory path where the file is located.
            file_name (str): Name of the file to be deleted.
        """
        file_client = self.fs_client.get_file_client(path_builder(path, file_name))
        file_client.delete_file()

    def delete_directory(self, directory: str) -> None:
        """Delete a directory from the Data Lake Gen2 file system.

        Args:
            directory (str): Directory path to be deleted.
        """
        directory_client = self.fs_client.get_directory_client(directory)
        directory_client.delete_directory()

    def write_file(self, path: str, file_name: str, content: str) -> None:
        """Write the contents into a file in the specified directory.

        Args:
            path (str): Directory path where the file will be created.
            file_name (str): Name of the file.
            content (str): The content to be written to the file.

        Returns:
            None
        """
        directory_client = self.fs_client.get_directory_client(path)
        file_client = directory_client.create_file(file_name)
        file_client.append_data(data=content, offset=0, length=len(content))
        file_client.flush_data(len(content))

    def read_json(self, path: str, file_name: str, **kwargs) -> pd.DataFrame:
        """Read the contents of a JSON file as a DataFrame.

        Args:
            path (str): Directory path where the JSON file is located.
            file_name (str): Name of the JSON file.
            **kwargs: Additional keyword arguments to pass to pd.read_json().

        Returns:
            pd.DataFrame: DataFrame containing the data from the JSON file.
        """
        file_client = self.fs_client.get_file_client(path_builder(path, file_name))
        stream = io.BytesIO()
        file_client.download_file().readinto(stream)
        stream.seek(0)
        df_result = pd.read_json(path_or_buf=stream, **kwargs)
        return df_result
