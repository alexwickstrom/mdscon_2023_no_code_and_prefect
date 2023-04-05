import asyncio
import logging
import os
import uuid
from time import sleep

import pandas as pd
import pendulum
from google.cloud import bigquery
from prefect import flow, get_run_logger, task
from prefect_fivetran import FivetranCredentials
from prefect_fivetran.connectors import (
    trigger_fivetran_connector_sync_and_wait_for_completion,
)
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

slack_token = os.environ.get("SLACK_BOT_TOKEN", "slack_api_key_12345")
client = WebClient(token=slack_token)

from auth_config import FivetranAuth, bq_dataset


@flow(name="step3_custompipeline")
async def custom_pipeline(custom_job_id: str) -> None:
    """This function simulates our custom pipeline"""
    logger = get_run_logger()
    bq = bigquery.Client()

    logger.info(f"Job ID is: {custom_job_id}")

    records = []

    for i in range(10):
        item = {
            "id": str(uuid.uuid4()),
            "timestamp": pendulum.now(),
            "value": str(uuid.uuid4()),
            "job_id": custom_job_id,
        }
        sleep(0.5)
        records.append(item)

    dataframe = pd.DataFrame(records, columns=["id", "timestamp", "value", "job_id"],)

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("timestamp", bigquery.enums.SqlTypeNames.TIMESTAMP),
            bigquery.SchemaField("value", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("job_id", bigquery.enums.SqlTypeNames.STRING),
        ],
        write_disposition="WRITE_APPEND",
    )

    job = bq.load_table_from_dataframe(
        dataframe=dataframe, destination=f"{bq_dataset}.record3", job_config=job_config,
    )
    job.result()


@task(name="dbt")
async def run_dbt_models() -> None:
    """This function is a stub that represents running dbt models"""
    logger = get_run_logger()
    logger.info("Running dbt models...")
    logger.info("Models ran successfully!")


@task(name="slack")
async def send_slack_notification() -> None:
    """This function is a stub that represents sending a Slack notification"""
    try:
        response = client.chat_postMessage(
            channel="C0XXXXXX",
            blocks=[
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": "Your fivetran sync is done."},
                },
            ],
        )
    except SlackApiError as e:
        print(e.response)
    logger = get_run_logger()
    logger.info("Running dbt models...")
    logger.info("Models ran successfully!")


@flow(name="step3_datapipeline")
async def data_pipeline(custom_job_id: str) -> None:
    logger = get_run_logger()

    logger.info(f"Custom Job ID is: {custom_job_id}")

    custom_pipeline_result = await custom_pipeline(custom_job_id=custom_job_id)

    fivetran_credentials = FivetranCredentials(
        api_key=FivetranAuth.api_key, api_secret=FivetranAuth.api_secret
    )
    fivetran_sync_result = await trigger_fivetran_connector_sync_and_wait_for_completion(
        fivetran_credentials=fivetran_credentials, connector_id="avidity_readiness",
    )

    # TODO: run the dbt model stub and then the slack notification stub. We want
    # to wait for the dbt model to finish before sending the slack notification.
    # https://docs.prefect.io/tutorials/execution/
    dbt_result = await run_dbt_models.submit()
    slack_result = await send_slack_notification.submit(wait_for=[dbt_model_result])


if __name__ == "__main__":
    asyncio.run(data_pipeline(custom_job_id=str(uuid.uuid4())))
    # asyncio.gather()
