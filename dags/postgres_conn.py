from ruamel import yaml

import great_expectations as ge
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest

CONNECTION_STRING = "postgresql+psycopg2://airflow:airflow@localhost:5432/airflow"

context = ge.get_context()

datasource_config = {
    "name": "retail",
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
    datasource_name="retail",
    data_connector_name="default_inferred_data_connector_name",
    data_asset_name="ecommerce.retail_profiling",  # this is the name of the table you want to retrieve
)

# # Next create an expectation suite, but not the empty one here
# context.create_expectation_suite(
#     expectation_suite_name="retail_suite", overwrite_existing=True
# )

# validator = context.get_validator(
#     batch_request=batch_request, expectation_suite_name="retail_suite"
# )
# print(validator.head())
