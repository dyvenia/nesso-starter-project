"""
Script for managing Prefect deployments based on Git commits and file changes. It provides functions to create and delete deployments based on modified files.

The script performs the following tasks:
- Retrieves the current Git branch and the commit hashes between the main branch and the current branch.
- Retrieves the modified files in each commit.
- Retrieves current Prefect deployments.
- Visits each modified file and performs deployment operations based on the file path and operation.
  - If the modified file is in the 'prefect/flows/deployments/' directory and the operation is 'A', 'M', 'R', or 'C', it creates a deployment from the file.
  - If the modified file is in the 'prefect/flows/deployments/' directory and the operation is 'D', it deletes the corresponding deployment.
"""

import asyncio
import logging
import os
import subprocess
from typing import Dict, List

from packaging import version

from prefect import __version__
from prefect.deployments import Deployment
from prefect.settings import PREFECT_API_KEY, PREFECT_API_URL

logging.basicConfig()
logger = logging.getLogger("Automatic Prefect Deployment")
logger.setLevel(logging.INFO)


if version.parse(__version__) >= version.parse("2.10"):
    from prefect.client.orchestration import get_client

    async def get_prefect_deployments() -> List[Deployment]:
        """Retrieve a list of Prefect deployments."""
        async with get_client() as client:
            deployments = await client.read_deployments()

            return deployments

else:
    from prefect.client import OrionClient

    async def get_prefect_deployments() -> List[Deployment]:
        """Retrieve a list of Prefect deployments."""
        async with OrionClient(
            api=PREFECT_API_URL.value(), api_key=PREFECT_API_KEY.value()
        ) as client:
            deployments = await client.read_deployments()

            return deployments


def create_deployment(file_name: str) -> None:
    """
    Create a deployment from the specified file_name.

    Args:
        file_name (str): The file_name to the deployment script.

    Raises:
        subprocess.CalledProcessError: If the deployment creation fails.
    """
    try:
        logger.info(f"Creating deployment {file_name} ...")
        subprocess.run(["python3", file_name], check=True)
        logger.info(f"Successfully completed {file_name} deployment creation")
    except subprocess.CalledProcessError as e:
        logger.warning(f"FAILED to create deployment {file_name}")
        raise e


def delete_deployment(deployment_id: str, deployment_name: str) -> None:
    """
    Delete a deployment with the specified id.

    Args:
        deployment_name (str): The name of the deployment.

    Raises:
        subprocess.CalledProcessError: If the deployment deletion fails.
    """
    try:
        logger.info(f"Deleting deployment {deployment_name} ...")
        delete_command = ["prefect", "deployment", "delete", "--id", deployment_id]
        subprocess.run(delete_command, capture_output=True, text=True)
        logger.info(f"Successfully completed {deployment_name} deployment deleting")
    except subprocess.CalledProcessError as e:
        logger.warning(f"FAILED to delete deployment {deployment_name}")
        raise e


def get_current_branch() -> str:
    """
    Retrieve the hash of the current branch.

    Returns:
        str: The hash of the current branch.
    """
    current_branch_command = ["git", "rev-parse", "HEAD"]
    current_branch_result = subprocess.run(
        current_branch_command, capture_output=True, text=True
    )
    current_branch = current_branch_result.stdout.strip()

    return current_branch


def get_commits(branch: str) -> List[str]:
    """
    Retrieves the commit hashes for the given branch.

    Args:
        branch (str): The name of the branch.
        
    Returns:
        List[str]: A list of commit hashes.
    """
    # Get the commit hashes for the given branch
    commit_hashes_command = [
        "git",
        "rev-list",
        "--ancestry-path",
        f"{branch}^..HEAD",
    ]
    commit_hashes_result = subprocess.run(
        commit_hashes_command, capture_output=True, text=True
    )
    commit_hashes = commit_hashes_result.stdout.strip().split()
    return commit_hashes


def get_modified_files(commit: str) -> Dict[str, str]:
    """
    Get the modified files in a commit.

    Args:
        commit (str): The commit hash.

    Returns:
        Dict[str, str]: A dictionary where the keys are the modified file paths and the values are the file operations.
    """
    changed_files_command = [
        "git",
        "diff-tree",
        "--no-commit-id",
        "--name-status",
        "-r",
        commit,
    ]
    changed_files_result = subprocess.run(
        changed_files_command, capture_output=True, text=True
    )

    changes = changed_files_result.stdout.strip().split()
    modified_files = {changes[i + 1]: changes[i] for i in range(0, len(changes), 2)}
    return modified_files


def visit_deployment(
    path: str,
    operation: str,
    prefect_deployments: List[Deployment],
    script_directory: str,
) -> None:
    """
    Visit a deployment based on the modified file path and based operation create or delete the deployment.

    Args:
        path (str): The modified file path.
        operation (str): The file operation ('A', 'M', 'R', 'C', or 'D').
        prefect_deployments (List[Deployment]): A list of Prefect deployments.
        script_directory (str): The directory of the deployment scripts.
    """
    os.chdir(script_directory)
    file_name = os.path.basename(path)

    if path.startswith("prefect/flows/deployments/") and operation in [
        "A",
        "M",
        "R",
        "C",
    ]:
        logger.info(f"Creating deployment from {path}")
        os.chdir("../../prefect/flows/deployments/")
        if os.path.exists(file_name):
            create_deployment(file_name)

    elif path.startswith("prefect/flows/deployments/") and operation == "D":
        logger.info(f"Deleting deployment from {path}")
        for deployment in prefect_deployments:
            if file_name in deployment.tags:
                delete_deployment(str(deployment.id), deployment.name)         

branch = get_current_branch()

commits = get_commits(branch=branch)

# Reverse the list of commits to ensure operations are performed in the same order as the user did
inverted_list = reversed(commits)

loop = asyncio.get_event_loop()
prefect_deployments = loop.run_until_complete(get_prefect_deployments())
loop.close()
script_directory = os.getcwd()

visited = []

# Iterate over commit hashes
for commit in inverted_list:
    modified_files = get_modified_files(commit)

    for path, operation in modified_files.items():
        if path not in visited:
            visit_deployment(path, operation, prefect_deployments, script_directory)
            visited.append(path)