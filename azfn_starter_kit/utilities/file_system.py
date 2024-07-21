import os


def path_builder(*args) -> str:
    """
    Constructs a platform-independent path by joining the given components.

    This function replaces backslashes (`\\`) with forward slashes (`/`) to ensure compatibility
    when testing Azure functions locally, as Azure functions use forward slashes regardless of the OS.

    Args:
        *args: Variable-length list of strings representing path components to be joined.

    Returns:
        str: The joined path with forward slashes.
    """
    path: str = os.path.join(*args).replace("\\", "/")
    return path
