from typing import Dict
import yaml
import requests
from pathlib import Path


def load_config(config_path: Path) -> Dict[str, Dict]:
    """
    Load configuration from a YAML file.

    Args:
        - config_path (Path): The path to the YAML configuration file.

    Returns:
        - Dict[str, Dict]: A dictionary with configuration keys and values.
    """
    with open(config_path, "r") as file:
        config = yaml.safe_load(file)
    return config

def download_file(url: Path, download_dir:Path, headers: dict = None) -> Path:
    """
    Downloads a file from the given URL and saves it to the specified directory.

    Args:
        - url (Path): The URL of the file to download.
        - download_dir (Path): The directory where the downloaded file will be saved.
        - headers (dict, optional): A dictionary of HTTP headers to send with the 
            request. Defaults to None.

    Returns:
        - Path: The path to the downloaded file.

    Raises:
        - requests.exceptions.HTTPError: If the HTTP request returned an 
            unsuccessful status code.
    """
    response = requests.get(url, headers)
    response.raise_for_status()
    file_path = download_dir / url.name
    with open(file_path, 'wb') as file:
        file.write(response.content)
    return file_path