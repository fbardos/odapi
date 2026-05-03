import datetime as dt
import lzma
import posixpath
import re
from contextlib import contextmanager
from io import BytesIO

import paramiko
from dagster import ConfigurableResource
from dagster import TimeWindow


class SFTPResource(ConfigurableResource):
    host: str
    port: int = 22
    username: str
    password: str | None = None
    key_file: str | None = None
    timeout: int = 10

    # ISO format from datetime.now().isoformat()
    _ISO_DATETIME_PATTERN = re.compile(
        r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})?)"
    )

    def _create_client(self) -> paramiko.SFTPClient:
        transport = paramiko.Transport((self.host, self.port))

        if self.key_file:
            private_key = paramiko.RSAKey.from_private_key_file(self.key_file)
            transport.connect(username=self.username, pkey=private_key)
        else:
            transport.connect(username=self.username, password=self.password)

        return paramiko.SFTPClient.from_transport(transport)

    @contextmanager
    def get_client(self):
        client = self._create_client()
        try:
            yield client
        finally:
            client.close()

    def ensure_dir(self, remote_dir: str):
        """
        Public method: ensure a directory exists (like `mkdir -p`).
        """
        with self.get_client() as client:
            self._mkdir_p(client, remote_dir)

    def _mkdir_p(self, client: paramiko.SFTPClient, remote_dir: str):
        """
        Internal recursive directory creation.
        """
        if not remote_dir or remote_dir == "/":
            return

        dirs = []
        current = remote_dir

        # Build directory tree
        while current not in ("", "/"):
            dirs.append(current)
            current = posixpath.dirname(current)

        dirs.reverse()

        # Create missing directories
        for d in dirs:
            try:
                client.stat(d)
            except FileNotFoundError:
                client.mkdir(d)

    def read_file(self, remote_path: str) -> bytes:
        with self.get_client() as client:
            with client.open(remote_path, "rb") as f:
                return f.read()

    def write_file(self, remote_path: str, data: bytes):
        with self.get_client() as client:
            with client.open(remote_path, "wb") as f:
                f.write(data)

    def list_files(self, remote_path: str) -> list[str]:
        with self.get_client() as client:
            return client.listdir(remote_path)

    def file_exists(self, remote_path: str) -> bool:
        with self.get_client() as client:
            try:
                client.stat(remote_path)
                return True
            except FileNotFoundError:
                return False

    def compress_to_xz(self, data: BytesIO, preset: int = 9) -> BytesIO:
        """
        Compress from a BytesIO into an .xz-compressed BytesIO.
        preset: 0..9, higher = stronger/slower
        """
        data.seek(0)
        raw = data.read()
        compressed = lzma.compress(raw, preset=preset)
        out = BytesIO(compressed)
        out.seek(0)
        return out

    def decompress_from_xz(self, data: BytesIO) -> BytesIO:
        data.seek(0)
        return BytesIO(lzma.decompress(data.read()))

    def find_single_file_for_date(
        self,
        remote_subdir: str,
        target_date: dt.date,
    ) -> str | None:
        """
        Search a subdirectory under the SFTP root for files whose names contain an
        ISO-8601 timestamp (e.g. datetime.now().isoformat()) matching target_date.

        Returns the full remote path if exactly one file matches.
        Returns None if no file matches.
        Raises ValueError if more than one file matches.
        """
        normalized_dir = "/" + remote_subdir.strip("/")

        with self.get_client() as client:
            filenames = client.listdir(normalized_dir)

        matches: list[str] = []

        for filename in filenames:
            iso_match = self._ISO_DATETIME_PATTERN.search(filename)
            if not iso_match:
                continue

            timestamp_str = iso_match.group(1)
            if timestamp_str.endswith("Z"):
                timestamp_str = timestamp_str[:-1] + "+00:00"

            try:
                parsed_dt = dt.datetime.fromisoformat(timestamp_str)
            except ValueError:
                continue

            if parsed_dt.date() == target_date:
                matches.append(posixpath.join(normalized_dir, filename))

        if len(matches) > 1:
            raise ValueError(
                f"Expected at most one file in '{normalized_dir}' for date "
                f"{target_date.isoformat()}, found {len(matches)}: {matches}"
            )

        return matches[0] if matches else None

    def find_files_in_time_window(
        self,
        remote_subdir: str,
        time_window: TimeWindow,
    ) -> list[str]:
        """
        Search a subdirectory under the SFTP root for files whose names contain an
        ISO-8601 timestamp (e.g. datetime.now().isoformat()) that falls within the
        given Dagster TimeWindow.

        Interprets the window as [start, end), matching Dagster's usual time-window
        semantics.

        Returns full remote paths for all matching files, sorted by embedded timestamp.
        """
        normalized_dir = "/" + remote_subdir.strip("/")

        with self.get_client() as client:
            filenames = client.listdir(normalized_dir)

        matches: list[tuple[dt.datetime, str]] = []

        for filename in filenames:
            iso_match = self._ISO_DATETIME_PATTERN.search(filename)
            if not iso_match:
                continue

            timestamp_str = iso_match.group(1)
            if timestamp_str.endswith("Z"):
                timestamp_str = timestamp_str[:-1] + "+00:00"

            try:
                parsed_dt = dt.datetime.fromisoformat(timestamp_str)
            except ValueError:
                continue

            window_start = time_window.start
            window_end = time_window.end

            # Normalize naive/aware datetime mismatch defensively.
            if parsed_dt.tzinfo is None and window_start.tzinfo is not None:
                parsed_dt = parsed_dt.replace(tzinfo=window_start.tzinfo)
            elif parsed_dt.tzinfo is not None and window_start.tzinfo is None:
                parsed_dt = parsed_dt.replace(tzinfo=None)

            if window_start <= parsed_dt < window_end:
                matches.append((parsed_dt, posixpath.join(normalized_dir, filename)))

        matches.sort(key=lambda x: x[0])
        return [path for _, path in matches]
