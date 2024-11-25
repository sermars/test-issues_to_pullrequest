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
    ):
        # Metadata and data keys
        METADATA_KEYS = list(yml_config["metadata"].keys())
        DATA_KEYS = yml_config["dataset"]

        # # Get the link to the CSV file
        # print(f"::LOGGER:: Issue comment: {issue_comment}")
        # csv_urls = re.findall(r'\[.*?\]\((https://.*?\.csv)\)', issue_comment)

        # Download the CSV files in parallel
        with concurrent.futures.ThreadPoolExecutor() as executor:
            csv_files = {
                file_path: download_file(file_path, csv_path, header)
                for file_path in urls
            }
        print(f"::LOGGER:: Downloaded {csv_files}")

        # Process the CSV file
        with concurrent.futures.ThreadPoolExecutor() as executor:
            results = {
                file_path.split("/")[-1]: result
                for file_path, result in zip(
                    csv_files,
                    executor.map(
                        lambda file_path: process_csv(
                            file_path, METADATA_KEYS, DATA_KEYS
                        ),
                        csv_files,
                    ),
                )
            }

        return results

    # GitHub API
    GIT = Github(git_token)
    REPO = GIT.get_repo(repo_name)
    ISSUE = REPO.get_issue(int(issue_number))

    # Folder to save the CSV files
    PTH_FILES = Path("/tmp/csv_files")
    PTH_FILES.mkdir(parents=True, exist_ok=True)

    # Check if the issue has a link to a CSV file
    assert any(re.findall(r"\[.*?\]\((.*?\.csv)\)", ISSUE.body)), "csv file not found."

    # Process the CSV file
    print(f"::LOGGER:: Issue: {ISSUE.body}")
    csv_urls = list(map(Path, re.findall(r"\[.*?\]\((https://.*?\.csv)\)", ISSUE.body)))
    print(f"::LOGGER:: CSV URLs: {csv_urls}")
    csv_processed = _csv_processing(csv_urls, config_yml, PTH_FILES)


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
