"""Miscellaneous utility functions."""
from urllib.parse import urlparse


def is_compressed(filename):
    """Guess whether the file is compressed based on its name.

    Args:
        filename: Name of the file

    Returns:
        `True` if the filename ends with ".z" or ".gz", `False` otherwise
    """
    return filename.endswith((".z", ".gz"))


def parse_s3_uri(s3uri: str) -> tuple[str, str]:
    """Parse an S3 URI into its bucket name and key.

    Args:
        s3uri: A valid S3 URI

    Returns:
        bucket: Bucket name
        key: Key name (no leading slash)

    Raises:
        ValueError: If `s3uri` does not begin with `"s3://"`.
    """
    parts = urlparse(s3uri)
    if parts.scheme != "s3":
        raise ValueError("Not an S3 URI")
    return parts.netloc, parts.path.removeprefix("/")
