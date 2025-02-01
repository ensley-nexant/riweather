"""Test module for the FTP connections."""

import ftplib

import pytest

from riweather import connection


def test_connection_ftp_login_welcome(mock_ftp):
    """Logs into the FTP server and can receive the welcome message."""
    with connection.NOAAFTPConnection() as conn:
        welcome = conn.ftp.getwelcome()

    mock_ftp.return_value.login.assert_called_once()
    assert welcome == b"Welcome to the mock FTP server!"


def test_ftp_calls_retrbinary(mock_ftp):
    """Makes a RETR call to the server."""
    with connection.NOAAFTPConnection() as conn:
        conn.read_file_as_bytes("/some/path/to/data.csv")

    assert mock_ftp.return_value.retrbinary.call_args.args[0] == "RETR /some/path/to/data.csv"


def test_ftp_retrieves_uncompressed_data(mock_ftp):
    """Retrieves data from an uncompressed file."""
    with connection.NOAAFTPConnection() as conn:
        contents = conn.read_file_as_bytes("/some/path/to/data.csv")

    assert contents.read() == b"mock file contents"


def test_ftp_retrieves_compressed_data(mock_ftp):
    """Retrieves data from a compressed file."""
    with connection.NOAAFTPConnection() as conn:
        contents = conn.read_file_as_bytes("/some/path/to/data.csv.z")

    assert contents.read() == b"compressed mock file contents"


def test_ftp_bad_connection_errors_out(mock_ftp):
    """Fails gracefully in the event of an FTP error."""
    mock_ftp.side_effect = OSError
    with (
        pytest.raises(connection.NOAAFTPConnectionError, match="Could not connect"),
        connection.NOAAFTPConnection() as conn,
    ):
        conn.ftp.getwelcome()


def test_ftp_file_not_found_errors_out(mock_ftp):
    """Fails gracefully when requesting a nonexistent file."""
    mock_ftp.return_value.retrbinary.side_effect = ftplib.Error
    with pytest.raises(connection.NOAAFTPConnectionError), connection.NOAAFTPConnection() as conn:
        conn.read_file_as_bytes("no/such/file.csv")
