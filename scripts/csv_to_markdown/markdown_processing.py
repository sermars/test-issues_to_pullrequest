from pathlib import Path
import concurrent.futures
import re
import pandas as pd

from scripts.csv_to_markdown.utils import load_config
from scripts.csv_to_markdown.csv_processing import process_csv


class MarkdownTable:
    """
        A class to process and manipulate markdown tables.

    Attributes:
        - md_content (str): The content of the markdown file.
        - lines (list): The list of lines of the markdown file.
        - idx_range_table (dict): A dictionary with the start and end indices of
            the table.
        - table_columns (list): A list of column names in the markdown table.

    Properties:
        get_markdown() -> str:

        get_lines() -> list:

    Methods:
        _read_md_file(md_pth: Path) -> str:

        _find_tables(md_file: str) -> list:

        _md_table_to_lst(table_row: str, sep: str = "|") -> list:

        add_new_row(new_values: dict) -> None:

        modify_cell(column_name: str, idx_row_edit: int, new_value: str):
    """

    def __init__(self, md_pth: Path, n_tables: int = 0) -> None:
        self.md_content = self._read_md_file(md_pth)
        self.lines = self.md_content.splitlines("\n")

        idx_range_table = self._find_tables(self.md_content)[n_tables]
        self.idx_range_table = {"start": idx_range_table[0], "end": idx_range_table[1]}

        table_header = self.lines[self.idx_range_table["start"]].strip()
        self.table_columns = self._md_table_to_lst(table_header)

    def _read_md_file(self, md_pth: Path) -> str:
        """
        Reads the content of a markdown file and returns it as a string.

        Args:
            - md_pth (Path): The path to the markdown file.

        Returns:
            - str: The content of the markdown file.
        """
        with open(md_pth, "r") as md_file:
            return md_file.read()

    def _find_tables(self, md_file: str) -> list:
        """
        Find tables in a Markdown file and return a list of tuples with the start and end lines.

        Args:
            - md_content (str): The content of the Markdown file.

        Returns:
            - list: A list of tuples with the start and end lines of each table.
        """

        def _is_table_line(line: str) -> bool:
            """
            Check if a given line is a markdown table line.

            Args:
                - line (str): The line to check.

            Returns:
                - bool: True if the line is a markdown table line, False otherwise.
            """
            return re.match(r"^\|.*\|$", line.strip())

        lines = md_file.splitlines("\n")

        table_lines = list(map(_is_table_line, lines))
        indices = range(len(lines))

        start_indices = [
            i for i in indices if table_lines[i] and (i == 0 or not table_lines[i - 1])
        ]
        end_indices = [
            i
            for i in indices
            if table_lines[i] and (i == len(lines) - 1 or not table_lines[i + 1])
        ]

        return list(zip(start_indices, end_indices))

    def _md_table_to_lst(self, table_row: str, sep: str = "|") -> list:
        """
        Converts a markdown table row into a list of cell values.

        Args:
            - table_row (str): A string representing a row in a markdown table.
            - sep (str, optional): The separator used in the markdown table. Defaults to "|".

        Returns:
            - list: A list of cell values with leading and trailing whitespace removed.
        """
        return list(map(lambda x: x.strip(), table_row.split(sep)[1:-1]))

    @property
    def get_markdown(self) -> str:
        """
        Converts the list of lines into a single markdown string.

        Returns:
            - str: The concatenated markdown string.
        """
        return "".join(self.lines)

    @property
    def get_lines(self) -> list:
        """
        Returns the list of lines of the markdown file.

        Returns:
            - list: The list of lines of the markdown file.
        """
        return self.lines

    def add_new_row(self, new_values: dict) -> None:
        """
        Adds a new row to the markdown table (at the end) with the provided values.

        Args:
            - new_values (dict): A dictionary containing the column names as keys
                and the corresponding values to be added to the new row.

        Raises:
            - AssertionError: If any of the keys in new_values are not found in
                the table columns.
        """
        assert all(
            map(lambda x: x in self.table_columns, new_values.keys())
        ), f"Metadata keys not found in table columns: {new_values.keys()}"

        new_row = (
            "| "
            + " | ".join([new_values.get(col, "") for col in self.table_columns])
            + " |\n"
        )

        self.idx_range_table["end"] += 1
        self.lines.insert(self.idx_range_table["end"], new_row)

    def modify_cell(self, column_name: str, idx_row_edit: int, new_value: str):
        """
        Modify the value of a specific cell in the markdown table.

        Args:
            - column_name (str): The name of the column where the cell is located.
            - idx_row_edit (int): The index of the row to be edited (0-based,
                excluding header).
            - new_value (str): The new value to be set in the specified cell.

        Raises:
            - AssertionError: If idx_row_edit is 0, indicating an attempt to
                modify the header row.
            - AssertionError: If column_name is not found in the table columns.
        """
        assert (
            idx_row_edit != 0
        ), "The first row is the header of the table and cannot be modified"
        assert (
            column_name in self.table_columns
        ), f"Column '{column_name}' not found in table columns: {self.table_columns}"
        column_index = self.table_columns.index(column_name)

        table_row = self._md_table_to_lst(
            self.lines[self.idx_range_table["start"] + idx_row_edit]
        )
        table_row[column_index] = f" {new_value} "
        self.lines[self.idx_range_table["start"] + idx_row_edit] = (
            "| " + " | ".join(table_row) + " |\n"
        )


def markdown_page(
    dict_metadata: dict,
    df_table: pd.DataFrame,
    index_md_path: Path,
    metadata_table_md: dict,
):
    ############################################################################
    # Part 1: Add new dataset to the global table.                             #
    ############################################################################
    tableMd = MarkdownTable(index_md_path)
    # Add new row to the metadata table
    new_row_colVal = {key: dict_metadata[val] for key, val in metadata_table_md.items()}
    tableMd.add_new_row(new_row_colVal)
    # # Modify cell in the metadata table
    # tableMd.modify_cell("Dataset", 5, "TESTTTTT")

    ############################################################################
    # Part 2: Create a new markdown file with the metadata description for     #
    # the new dataset.                                                         #
    ############################################################################
    md_table = df_table.to_markdown(index=False)

    # with open("prueba.md", "w") as md_file:
    #     # md_file.write(tableMd.get_markdown)
    #     md_file.write(md_table)

    return tableMd.get_markdown, md_table


if __name__ == "__main__":

    CSV_FOLDER = "data/vlc"
    config_yml = load_config(Path("scripts/csv_to_markdown/config.yaml"))

    METADATA_KEYS = list(config_yml["metadata"].keys())
    DATA_KEYS = config_yml["dataset"]
    METADATA_TABLE_MD = dict(
        filter(
            lambda item: item[0],
            zip(
                map(lambda x: x["table_column"], config_yml["metadata"].values()),
                config_yml["metadata"].keys(),
            ),
        )
    )

    # CSV processing
    path = Path(CSV_FOLDER)
    lst_files = list(path.glob("*.csv"))
    with concurrent.futures.ThreadPoolExecutor() as executor:
        csv_processed = {
            file_path.name: result
            for file_path, result in zip(
                lst_files,
                executor.map(
                    lambda file_path: process_csv(file_path, METADATA_KEYS, DATA_KEYS),
                    lst_files,
                ),
            )
        }

    # Markdown processing
    for file_name, (metadata, df) in csv_processed.items():
        # print(f"File name: {file_name}")
        # print("Metadata:")
        # print(metadata)
        # print("Dataframe:")
        # print(df)
        markdown_page(
            metadata,
            df,
            Path(config_yml["markdowns"]["index"]),
            METADATA_TABLE_MD,
        )
