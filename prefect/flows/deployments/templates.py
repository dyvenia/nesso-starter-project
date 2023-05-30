"""
Templates for Prefect deployments. 
"""

import importlib
import os
import pathlib

import __main__
from prefect.blocks.core import Block
from prefect.deployments import Deployment
from prefect.orion.schemas.schedules import CronSchedule


def get_deployment(
    name: str,
    flow_name: str,
    is_flow_custom: bool = False,
    params: dict = None,
    schedule: str = None,
    queue: str = None,
    infra_block: str = None,
    storage_block: str = None,
    version: int = 1,
    tags: list[str] = [],
) -> Deployment:
    """
    Build and return a Prefect deployment.

    Args:
        name (str): Name of the deployment.
        flow_name (str): Name of the flow.
        is_flow_custom (bool): Whether flow is custom or pre-defined.
            If True, deployment will reference to the named flow from '$NESSO_REPO_HOME/prefect/flows/custom' folder.
            If False, deployment will reference to the named flow from 'prefect_viadot.flows' package. Defaults to False.
        params (dict, optional): Dictionary of parameters for flow runs scheduled by the deployment. Defaults to None.
        schedule (str, optional): Schedule for the deployment defined using the unix-cron string format ( * * * * * ). Defaults to None.
        queue (str, optional): Work queue that will handle the deployment's run. Defaults to None.
        infra_block (str, optional): Infrastructure block configured for the deployment. Defaults to None.
        storage_block (str, optional): Storage block configured for the deployment. Defaults to None.
        version (int, optional): Version of the deployment. Defaults to 1.
        tags (list[str], optional): List of tags for the deployment. Defaults to [].

    Returns:
        Deployment: Prefect deployment that stores flow's metadata.
    """
    if is_flow_custom:
        flow_module = importlib.import_module(f"custom.{flow_name}")
        flow = getattr(flow_module, flow_name)
        flow_filepath = getattr(flow_module, "__file__", None)
    else:
        flow_module = importlib.import_module(f"prefect_viadot.flows.{flow_name}")
        flow = getattr(flow_module, flow_name)
        flow_filepath = getattr(flow_module, "__file__", None)

    infrastructure = Block.load(infra_block)
    storage = Block.load(storage_block)

    deployment = Deployment.build_from_flow(
        flow=flow,
        name=name,
        entrypoint=f"{flow_filepath}:{flow.fn.__name__}",
        parameters=params,
        schedule=schedule,
        work_queue_name=queue,
        infrastructure=infrastructure,
        storage=storage,
        version=version,
        tags=tags,
    )

    return deployment


def extract_and_load(
    name: str,
    flow_name: str,
    is_flow_custom: bool = False,
    params: dict = None,
    schedule: str = None,
    schedule_timezone: str = None,
    queue: str = "default",
    infra_block: str = None,
    storage_block: str = None,
    version: int = 1,
    tags: list[str] = [],
) -> None:
    """
    Build and apply the Prefect deployment.

    Args:
        name (str): Name of the deployment.
        flow_name (str): Name of the flow.
        is_flow_custom (bool): Whether flow is custom or pre-defined.
            If True, deployment will reference to the named flow from '$NESSO_REPO_HOME/prefect/flows/custom' folder.
            If False, deployment will reference to the named flow from 'prefect_viadot.flows' package. Defaults to False.
        params (dict, optional): Dictionary of parameters for flow runs scheduled by the deployment. Defaults to None.
        schedule (str, optional): Schedule for the deployment defined using the unix-cron string format ( * * * * * ).
            Defaults to None.
        schedule_timezone (str, optional): The timezone for the schedule. Defaults to the value of the
            `NESSO_PREFECT_DEFAULT_SCHEDULE_TIMEZONE` environment variable.
        queue (str, optional): Work queue that will handle the deployment's run. Defaults to "default".
        infra_block (str, optional): Infrastructure block configured for the deployment. Defaults to the value of the
            `NESSO_PREFECT_DEFAULT_INFRA_BLOCK` environment variable.
        storage_block (str, optional): Storage block configured for the deployment. Defaults to the value of the
            `NESSO_PREFECT_DEFAULT_STORAGE_BLOCK` environment variable.
        version (int, optional): Version of the deployment. Defaults to 1.
        tags (list[str], optional): List of tags for the deployment. Defaults to [].

    """

    if infra_block is None:
        infra_block = os.environ.get("NESSO_PREFECT_DEFAULT_INFRA_BLOCK")

    if storage_block is None:
        storage_block = os.environ.get("NESSO_PREFECT_DEFAULT_STORAGE_BLOCK")

    if schedule:
        if schedule_timezone is None:
            schedule_timezone = os.environ.get(
                "NESSO_PREFECT_DEFAULT_SCHEDULE_TIMEZONE"
            )

        schedule = CronSchedule(cron=schedule, timezone=schedule_timezone)

    params = params or {}
    params_final = {**EXTRACT_DEFAULT_PARAMS, **params}

    deployment = get_deployment(
        name=name,
        flow_name=flow_name,
        is_flow_custom=is_flow_custom,
        params=params_final,
        schedule=schedule,
        queue=queue,
        infra_block=infra_block,
        storage_block=storage_block,
        version=version,
        tags=tags,
    )
    deployment.apply(upload=True)


def extract_and_load_custom(
    flow_name: str = None,
    schedule: str = None,
    **kwargs,
):
    params = {}
    params.update(**kwargs)

    extract_and_load(
        name=pathlib.Path(__main__.__file__).stem,
        flow_name=flow_name,
        is_flow_custom=True,
        params=params,
        schedule=schedule,
    )


# Here we can specify default flow parameters to be passed to the `params` parameter of the
# `extract_and_load()` deployment template.
bucket = os.getenv("NESSO_BUCKET_NAME")
schema_name = os.getenv("NESSO_LANDING_SCHEMA")
EXTRACT_DEFAULT_PARAMS = {"to_path": f"s3://{bucket}/nesso/{schema_name}"}
