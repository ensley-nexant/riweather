"""Miscellaneous utility functions."""


def is_compressed(filename):
    """Guess whether the file is compressed based on its name.

    Args:
        filename: Name of the file

    Returns:
        `True` if the filename ends with ".z" or ".gz", `False` otherwise
    """
    return filename.endswith((".z", ".gz"))
