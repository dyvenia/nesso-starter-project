import os

from dotenv import load_dotenv
from templates import extract_and_load

load_dotenv()

deployment_name = "example_deployment"
flow_name = "my_prefect_viadot_flow"
schema_name = os.getenv("NESSO_LANDING_SCHEMA")
table_name = "my_table"
flow_params = {
    "schema_name": schema_name,
    "table": table_name,
}
schedule = "07 04 * * 01"

extract_and_load(
    name=deployment_name,
    flow_name=flow_name,
    params=flow_params,
    schedule=schedule,
)
