from urllib.parse import urlparse


def is_compressed(filename):
    return filename.endswith((".z", ".gz"))


def parse_s3_uri(s3uri: str) -> tuple[str, str]:
    parts = urlparse(s3uri)
    if parts.scheme != "s3":
        raise ValueError("Not an S3 URI")
    return parts.netloc, parts.path.removeprefix("/")
