import pytest

from azfn_starter_kit.utilities.file_system import path_builder


@pytest.mark.parametrize(
    "components, expected_path",
    [
        (["folder"], "folder"),
        (["Users", "User", "Documents", "file.txt"], "Users/User/Documents/file.txt"),
        (["C:\\Users\\User\\Documents", "folder", "file.txt"], "C:/Users/User/Documents/folder/file.txt"),
        (["", "folder", "file.txt"], "folder/file.txt"),
    ],
)
def test_path_builder(components, expected_path):
    result = path_builder(*components)
    assert result == expected_path
