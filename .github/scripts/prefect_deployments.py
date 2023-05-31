import asyncio
import os
import subprocess
from typing import Dict, List

from packaging import version
from prefect import __version__
from prefect.deployments import Deployment
from prefect.settings import PREFECT_API_KEY, PREFECT_API_URL

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
        print(f"Creating deployment {file_name} ...")
        subprocess.run(["python3", file_name], check=True)
        print(f"Successfully completed {file_name} deployment creation")
    except subprocess.CalledProcessError as e:
        print(f"FAILED to create deployment {file_name}")
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
        print(f"Deleting deployment {deployment_name} ...")
        delete_command = ["prefect", "deployment", "delete", "--id", deployment_id]
        subprocess.run(delete_command, capture_output=True, text=True)
        print(f"Successfully completed {deployment_name} deployment deleting")
    except subprocess.CalledProcessError as e:
        print(f"FAILED to delete deployment {deployment_name}")
        raise e


def get_current_branch() -> str:
    current_branch_command = ["git", "branch", "--show-current"]
    current_branch_result = subprocess.run(
        current_branch_command, capture_output=True, text=True
    )
    current_branch = current_branch_result.stdout.strip()

    return current_branch


def get_commits(main_branch: str, current_branch: str) -> List[str]:
    # Get the commit hashes for the given branch
    commit_hashes_command = [
        "git",
        "log",
        "--no-merges",
        current_branch,
        f"^{main_branch}",
        "--format=%H",
    ]
    commit_hashes_result = subprocess.run(
        commit_hashes_command, capture_output=True, text=True
    )
    commit_hashes = commit_hashes_result.stdout.strip().split()
    return commit_hashes


def get_modified_files(commit_hash: str) -> Dict[str, str]:
    changed_files_command = [
        "git",
        "diff-tree",
        "--no-commit-id",
        "--name-status",
        "-r",
        commit_hash,
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
    os.chdir(script_directory)
    file_name = os.path.basename(path)

    if path.startswith("prefect/flows/deployments/") and operation in [
        "A",
        "M",
        "R",
        "C",
    ]:
        print(f"Creating deployment from {path}")
        os.chdir("../../prefect/flows/deployments/")
        if os.path.exists(file_name):
            create_deployment(file_name)

    elif path.startswith("prefect/flows/deployments/") and operation == "D":
        print(f"Deleting deployment from {path}")
        for deployment in prefect_deployments:
            if file_name in deployment.tags:
                delete_deployment(str(deployment.id), deployment.name)


current_branch = get_current_branch()

commits = get_commits(main_branch="master", current_branch=current_branch)

loop = asyncio.get_event_loop()
prefect_deployments = loop.run_until_complete(get_prefect_deployments())
loop.close()
script_directory = os.getcwd()

visited = []

# Iterate over commit hashes
for commit in commits:
    modified_files = get_modified_files(commit)

    for path, operation in modified_files.items():
        if path not in visited:
            visit_deployment(path, operation, prefect_deployments, script_directory)
            visited.append(path)
