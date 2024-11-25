from typing import Optional, Tuple, Dict
import concurrent.futures
from pathlib import Path

import chardet
import pandas as pd

from scripts.csv_to_markdown.utils import load_config


def detect_encoding(file_path: Path) -> Optional[str]:
    """
    Detect the encoding of a given file.

    Args:
        - file_path (Path): The path to the file whose encoding is to be detected.

    Returns:
        - Optional[str]: The detected encoding of the file, or None if the encoding
            could not be detected.
    """
    with open(file_path, "rb") as f:
        result = chardet.detect(f.read())
    return result["encoding"]


def process_csv(
    file_path: Path, metadata_keys: list, data_keys: list
) -> Optional[Tuple[Dict[str, str], pd.DataFrame]]:
    """
    Processes a CSV file to extract metadata and dataset content.

    This function reads a CSV file, detects its encoding, and processes its content
    to extract metadata and dataset information based on the provided keys.

    Args:
        - file_path (Path): The path to the CSV file to be processed.
        - metadata_keys (list): A list of expected metadata keys.
        - data_keys (list): A list of expected dataset keys.

    Returns:
        - Optional[Tuple[Dict[str, str], pd.DataFrame]]: A tuple containing a
            dictionary with metadata key-value pairs and a DataFrame with the
            dataset content. Returns None if there is an error processing the file.

    Raises:
        - AssertionError: If the CSV file format does not meet the expected structure.
    """

    def _msg_print(msg: str) -> str:
        """
        Formats a message string by prefixing it with the name of the file.

        Args:
            - msg (str): The message to be formatted.

        Returns:
            - str: The formatted message string.
        """
        return f"[{file_path.name}] - {msg}"

    def _metatada_content(df: pd.DataFrame, key_values: list) -> Dict[str, str]:
        """
        Extracts metadata content from the first two columns of the given DataFrame.

        Args:
            - df (pd.DataFrame): The DataFrame containing the metadata.
            - key_values (list): A list of expected metadata keys.

        Returns:
            - Dict[str, str]: A dictionary containing the metadata key-value pairs.

        Raises:
            - AssertionError: If the first two columns are not filled or if the
            metadata keys do not match the expected keys.
        """

        df_metadata = df.iloc[: len(key_values)].copy()
        df_metadata = df_metadata.dropna(axis=1, how="all")
        assert df_metadata.shape[1] == 2, _msg_print(
            "Only the first two columns must be filled!"
        )

        dict_metadata = df_metadata.set_index(0).iloc[:, 0].to_dict()
        assert set(dict_metadata.keys()) == set(key_values), _msg_print(
            f"Invalid metadata keys!, expected: {key_values}"
        )
        return dict_metadata

    def _dataset_content(
        df: pd.DataFrame, key_values: list, metadata_len: int
    ) -> pd.DataFrame:
        """
        Processes a DataFrame to extract and validate dataset content.

        This function takes a DataFrame and a list of key values, processes the
        DataFrame to extract the dataset content starting from the 9th row, and
        validates that the first two columns are empty and that the remaining
        columns match the provided key values.

        Args:
            - df (pd.DataFrame): The input DataFrame to process.
            - key_values (list): A list of expected key values for the dataset
                columns.
            - metadata_len (int): The number of rows used for metadata.

        Returns:
            - pd.DataFrame: A processed DataFrame containing the dataset content.

        Raises:
            - AssertionError: If the first two columns are not empty or if the
                dataset columns do not match the provided key values.
        """
        df_dataset = df.iloc[metadata_len:].reset_index(drop=True).copy()
        assert df_dataset.iloc[:, :2].isnull().all().all(), _msg_print(
            "First two columns must be empty!"
        )
        df_dataset = df_dataset.iloc[:, 2:]
        df_dataset.columns = df_dataset.iloc[0]
        assert set(df_dataset.columns) == set(key_values), _msg_print(
            f"Invalid dataset keys!, expected: {key_values}"
        )
        df_dataset = df_dataset[1:]
        df_dataset.reset_index(drop=True, inplace=True)
        return df_dataset

    encoding = detect_encoding(file_path)
    print(_msg_print(f"Encoding detected: {encoding}"))
    try:
        df_raw = pd.read_csv(file_path, sep=";", encoding=encoding, header=None)
        print(_msg_print(f"Processing {file_path}: {len(df_raw)} rows"))
    except Exception as e:
        print(_msg_print(f"Error processing {file_path} with encoding {encoding}: {e}"))
        return None

    empty_rows = len(metadata_keys) + 1
    assert df_raw.iloc[empty_rows - 1].isnull().all(), _msg_print(
        f"Line {empty_rows} must be empty!"
    )
    return _metatada_content(df_raw, metadata_keys), _dataset_content(
        df_raw, data_keys, empty_rows
    )


if __name__ == "__main__":

    CSV_FOLDER = "data/vlc"
    config_yml = load_config(Path("scripts/csv_to_markdown/config.yaml"))
    METADATA_KEYS = list(config_yml["metadata"].keys())
    DATA_KEYS = config_yml["dataset"]

    path = Path(CSV_FOLDER)
    lst_files = list(path.glob("*.csv"))

    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = {
            file_path.name: result
            for file_path, result in zip(
                lst_files,
                executor.map(
                    lambda file_path: process_csv(file_path, METADATA_KEYS, DATA_KEYS),
                    lst_files,
                ),
            )
        }

    # print("Results:", results)
    print("All files processed.")
