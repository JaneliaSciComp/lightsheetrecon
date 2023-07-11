#!/usr/bin/env python


"""Provide a command line tool to validate and transform tabular samplesheets."""


import argparse
import csv
import logging
import sys
from collections import Counter
from pathlib import Path

logger = logging.getLogger()


class RowChecker:
    """
    Define a service that can validate and transform each given row.

    Attributes:
        modified (list): A list of dicts, where each dict corresponds to a previously
            validated and transformed row. The order of rows is maintained.

    """

    def __init__(
        self,
        acq_col="id",
        filename_col="filename",
        **kwargs,
    ):
        """
        Initialize the row checker with the expected column names.

        Args:
            acq_col (str): The name of the column that contains the acquisition name
                (default "acq_name").
            filename_col (str): The name of the column that contains the file name
                (default "filename").
            filepath_col (str): The name of the column that contains the file path
                (default "filepath").

        """
        super().__init__(**kwargs)
        self._acq_col = acq_col
        self._filename_col = filename_col
        self._seen = set()
        self.modified = []

    def validate_and_transform(self, row):
        """
        Perform all validations on the given row and insert the read pairing status.

        Args:
            row (dict): A mapping from column headers (keys) to elements of that row
                (values).

        """
        self._validate_acq(row)
        self._validate_filename(row)
        self._seen.add((row[self._acq_col], row[self._filename_col]))
        self.modified.append(row)

    def _validate_acq(self, row):
        """Assert that the acquisition name exists and convert spaces to underscores."""
        if len(row[self._acq_col]) <= 0:
            raise AssertionError("Acquisition name is required.")
        # Sanitize samples slightly.
        row[self._acq_col] = row[self._acq_col].replace(" ", "_")

    def _validate_filename(self, row):
        """Assert that the filename is non-empty."""
        if len(row[self._filename_col]) <= 0:
            raise AssertionError("File name is required.")


def read_head(handle, num_lines=10):
    """Read the specified number of lines from the current position in the file."""
    lines = []
    for idx, line in enumerate(handle):
        if idx == num_lines:
            break
        lines.append(line)
    return "".join(lines)


def sniff_format(handle):
    """
    Detect the tabular format.

    Args:
        handle (text file): A handle to a `text file`_ object. The read position is
        expected to be at the beginning (index 0).

    Returns:
        csv.Dialect: The detected tabular format.

    .. _text file:
        https://docs.python.org/3/glossary.html#term-text-file

    """
    peek = read_head(handle)
    handle.seek(0)
    sniffer = csv.Sniffer()
    dialect = sniffer.sniff(peek)
    return dialect


def check_samplesheet(file_in, file_out):
    """
    Check that the tabular samplesheet has the structure expected by the pipeline.

    Validate the general shape of the table, expected columns, and each row.

    Args:
        file_in (pathlib.Path): The given tabular samplesheet. The format can be either
            CSV, TSV, or any other format automatically recognized by ``csv.Sniffer``.
        file_out (pathlib.Path): Where the validated and transformed samplesheet should
            be created; always in CSV format.

    Example:
        This function checks that the samplesheet follows the following structure:

            id,filename,checksum,uri
            LHA3_R3_tiny,LHA3_R3_tiny.mvl,https://janelia.figshare.com/ndownloader/files/30900679
            LHA3_R3_tiny,LHA3_R3_tiny_V01.czi,https://janelia.figshare.com/ndownloader/files/30900664
            LHA3_R3_tiny,LHA3_R3_tiny_V02.czi,https://janelia.figshare.com/ndownloader/files/30900676

        The checksum and uri are optional, so at minimum there should be two columns:

            id,filename
            LHA3_R3_tiny,LHA3_R3_tiny.mvl
            LHA3_R3_tiny,LHA3_R3_tiny_V01.czi
            LHA3_R3_tiny,LHA3_R3_tiny_V02.czi

    """
    required_columns = {"id", "filename"}
    # See https://docs.python.org/3.9/library/csv.html#id3 to read up on `newline=""`.
    with file_in.open(newline="") as in_handle:
        reader = csv.DictReader(in_handle, dialect=sniff_format(in_handle))
        # Validate the existence of the expected header columns.
        if not required_columns.issubset(reader.fieldnames):
            req_cols = ", ".join(required_columns)
            logger.critical(f"The sample sheet **must** contain these column headers: {req_cols}.")
            sys.exit(1)
        # Validate each row.
        checker = RowChecker()
        for i, row in enumerate(reader):
            try:
                checker.validate_and_transform(row)
            except AssertionError as error:
                logger.critical(f"{str(error)} On line {i + 2}.")
                sys.exit(1)
        # Validate metadata
        id2czis = {}
        id2pattern = {}
        for row in checker.modified:
            id = row['id']
            if 'pattern' in row and row['pattern']:
                id2pattern[id] = row['pattern']
            if row['filename'].endswith(".czi"):
                if id not in id2czis:
                    id2czis[id] = []
                id2czis[id].append(row['filename'])
        for id in id2czis.keys():
            if len(id2czis[id])>1 and (id not in id2pattern or not id2pattern[id]):
                logger.critical(f"Acquisition '{id}' has more than one CZI file, so it needs to have at least one value in the 'pattern' column.")
                sys.exit(1)

    header = list(reader.fieldnames)
    # See https://docs.python.org/3.9/library/csv.html#id3 to read up on `newline=""`.
    with file_out.open(mode="w", newline="") as out_handle:
        writer = csv.DictWriter(out_handle, header, delimiter=",")
        writer.writeheader()
        for row in checker.modified:
            writer.writerow(row)


def parse_args(argv=None):
    """Define and immediately parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Validate and transform a tabular samplesheet.",
        epilog="Example: python check_samplesheet.py samplesheet.csv samplesheet.valid.csv",
    )
    parser.add_argument(
        "file_in",
        metavar="FILE_IN",
        type=Path,
        help="Tabular input samplesheet in CSV or TSV format.",
    )
    parser.add_argument(
        "file_out",
        metavar="FILE_OUT",
        type=Path,
        help="Transformed output samplesheet in CSV format.",
    )
    parser.add_argument(
        "-l",
        "--log-level",
        help="The desired log level (default WARNING).",
        choices=("CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"),
        default="WARNING",
    )
    return parser.parse_args(argv)


def main(argv=None):
    """Coordinate argument parsing and program execution."""
    args = parse_args(argv)
    logging.basicConfig(level=args.log_level, format="[%(levelname)s] %(message)s")
    if not args.file_in.is_file():
        logger.error(f"The given input file {args.file_in} was not found!")
        sys.exit(2)
    args.file_out.parent.mkdir(parents=True, exist_ok=True)
    check_samplesheet(args.file_in, args.file_out)


if __name__ == "__main__":
    sys.exit(main())
