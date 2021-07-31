import os

import great_expectations as ge

from airflow import AirflowException
from airflow.hooks.S3_hook import S3Hook


def local_to_s3(bucket_name, key, file_name, remove_local=False):
    """Loads file from local machine to S3. If file needs to be removed,
    specify `remove_local=True`.

    Args:
        bucket_name (str): S3 bucket name
        key (str): Directory on S3 where file is going to be loaded
        file_name (str): Directory on local system where file is located.
        remove_local (bool, optional): To delete local file. Defaults to False.
    """

    s3_hook = S3Hook()
    s3_hook.load_file(
        filename=file_name, bucket_name=bucket_name, replace=True, key=key
    )
    if remove_local:
        if os.path.isfile(file_name):
            os.remove(file_name)


def validate_data(**kwargs):

    # Retrieve your data context
    context = ge.data_context.DataContext("/opt/airflow/great_expectations")

    # Create your batch_kwargs
    batch_kwargs_file = {
        "path": "/data",
        "datasource": "retail",
        "table": "ecommerce.retail_profiling",
    }

    # Create your batch (batch_kwargs + expectation suite)
    batch_file = context.get_batch(batch_kwargs_file, "retail_suite")

    # Run the validation
    results = context.run_validation_operator(
        "action_list_operator",
        assets_to_validate=[batch_file],
        # This run_id can be whatever you choose
        run_id=f"airflow: {kwargs['dag_run'].run_id}:{kwargs['dag_run'].start_date}",
    )

    # Handle result of validation
    if not results["success"]:
        raise AirflowException("Validation of the data is not successful ")
