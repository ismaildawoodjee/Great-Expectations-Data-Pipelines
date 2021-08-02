# Running this script will populate the great_expectations.yml file
# After that this is no longer required

from ruamel import yaml

import great_expectations as ge
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest

# should be stored as an environment variable
CONNECTION_STRING = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"

context = ge.get_context()

datasource_config = {
    "name": "retail_source",
    "class_name": "Datasource",
    "execution_engine": {
        "class_name": "SqlAlchemyExecutionEngine",
        "connection_string": CONNECTION_STRING,
    },
    "data_connectors": {
        "default_runtime_data_connector_name": {
            "class_name": "RuntimeDataConnector",
            "batch_identifiers": ["default_identifier_name"],
        },
        "default_inferred_data_connector_name": {
            "class_name": "InferredAssetSqlDataConnector",
            "name": "whole_table",
            "include_schema_name": True,  # to specify schema name when calling BatchRequest
        },
    },
}


context.test_yaml_config(yaml.dump(datasource_config))

context.add_datasource(**datasource_config)

batch_request = BatchRequest(
    datasource_name="retail_source",
    data_connector_name="default_inferred_data_connector_name",
    data_asset_name="ecommerce.retail_profiling",  # this is the name of the table you want to retrieve
)

batch_request = RuntimeBatchRequest(
    datasource_name="retail_source",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="ecommerce.retail_profiling",  # this can be anything that identifies this data
    runtime_parameters={"query": "SELECT * from ecommerce.retail_profiling LIMIT 1000"},
    batch_identifiers={"default_identifier_name": "First 1000 rows for profiling"},
)

validator = context.get_validator(
    batch_request=batch_request, expectation_suite_name="retail_suite"
)
