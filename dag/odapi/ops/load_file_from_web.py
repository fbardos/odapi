import lzma
import tempfile

import fsspec
import polars as pl


def scan_file_xz(
    url: str, uncompressed_file_ending: str = '.csv', **kwargs
) -> pl.LazyFrame:
    tmp = tempfile.NamedTemporaryFile(
        suffix=uncompressed_file_ending,
        delete=False,
    )

    with (
        fsspec.open(url, 'rb') as remote,
        lzma.open(remote, 'rb') as compressed,
        open(tmp.name, 'wb') as decompressed,
    ):
        while chunk := compressed.read(1024 * 1024):
            decompressed.write(chunk)

    return pl.scan_csv(
        tmp.name,
        **kwargs,
    )
