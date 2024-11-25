import os
import sys
import re
from pathlib import Path
import traceback
import concurrent.futures

from github import Github

from scripts.csv_to_markdown.utils import load_config, download_file
from scripts.csv_to_markdown.csv_processing import process_csv


def main(git_token: str, repo_name: str, issue_number: str, config_yml: dict):

    def _csv_processing(
        urls: list, yml_config: dict, csv_path: Path, header: dict = None
    ) -> dict:
        """
        Downloads and processes CSV files based on provided URLs and configuration.

        Args:
            - urls (list): List of URLs pointing to the CSV files to be downloaded.
            - yml_config (dict): Configuration dictionary containing metadata and dataset keys.
            - csv_path (Path): Path to the directory where the CSV files will be downloaded.
             - header (dict, optional): Optional headers to include in the download request.

        Returns:
            - dict: A dictionary where keys are the names of the CSV files and
                values are the processed results.
        """
        # Metadata and data keys
        METADATA_KEYS = list(yml_config["metadata"].keys())
        DATA_KEYS = yml_config["dataset"]

        # Download the CSV in temporary folder
        with concurrent.futures.ThreadPoolExecutor() as executor:
            csv_url_to_path = {
                file_path: download_file(file_path, csv_path, header)
                for file_path in urls
            }
        csv_files = list(csv_url_to_path.values())

        # Process the CSV file
        with concurrent.futures.ThreadPoolExecutor() as executor:
            results = {
                file_path.name: result
                for file_path, result in zip(
                    csv_files,
                    executor.map(
                        lambda f_pth: process_csv(f_pth, METADATA_KEYS, DATA_KEYS),
                        csv_files,
                    ),
                )
            }

        return results

    # GitHub API
    GIT = Github(git_token)
    REPO = GIT.get_repo(repo_name)
    ISSUE = REPO.get_issue(int(issue_number))

    # Check if the issue has a link to a CSV file
    assert any(re.findall(r"\[.*?\]\((.*?\.csv)\)", ISSUE.body)), "csv file not found."

    # Process the CSV file
    csv_urls = re.findall(r"\[.*?\]\((https://.*?\.csv)\)", ISSUE.body)
    PTH_FILES = config_yml["github_actions"]["csv_path"]
    PTH_FILES.mkdir(parents=True, exist_ok=True)
    csv_processed = _csv_processing(csv_urls, config_yml, PTH_FILES)
    print(f"::LOGGER:: Processed {csv_processed}")


if __name__ == "__main__":
    # Get Actions environment variables
    GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
    REPO_NAME = os.getenv("GITHUB_REPOSITORY")
    ISSUE_NUMBER = os.getenv("ISSUE_NUMBER")
    # Configurations
    CONFIG_PARAMS = load_config(Path("scripts/csv_to_markdown/config.yaml"))

    try:
        main(GITHUB_TOKEN, REPO_NAME, ISSUE_NUMBER, CONFIG_PARAMS)
    except Exception as e:
        print(e, file=sys.stderr)
        print(traceback.format_exc())
        sys.exit(1)
