from ruamel import yaml

import great_expectations as ge
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest

context = ge.get_context()

datasource_config = {
    "name": "retail_load",
    "class_name": "Datasource",
    "execution_engine": {"class_name": "PandasExecutionEngine"},
    "data_connectors": {
        "default_runtime_data_connector_name": {
            "class_name": "RuntimeDataConnector",
            "batch_identifiers": ["default_identifier_name"],
            "runtime_parameters": {"path": "s3://greatex-bucket/raw/retail/2021-08-02/retail_profiling.csv"},  # Add your S3 path here.
            "batch_spec_passthrough": {
                "reader_method": "read_csv",
                "reader_options": {
                "nrows": 1000
                }
            }
        },
        "default_inferred_data_connector_name": {
            "class_name": "InferredAssetS3DataConnector",
            "bucket": "greatex-bucket",
            "prefix": "raw/retail/",
            "default_regex": {
                "group_names": ["prefix", "datestamp", "data_asset_name"],
                "pattern": "(.*)\\.csv",
            },
        },
    },
}

context.test_yaml_config(yaml.dump(datasource_config))

context.add_datasource(**datasource_config)

# batch_request = BatchRequest(
#     datasource_name="retail_load",
#     data_connector_name="default_inferred_data_connector_name",
#     data_asset_name="retail_profiling",
#     data_connector_query={
#         "index": 0
#     },
#     batch_spec_passthrough={
#         "reader_method": "read_csv",
#         "reader_options": {
#             "nrows": 1000
#         }
#     }
# )

batch_request = RuntimeBatchRequest(
    datasource_name="retail_load",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="retail_profiling",  # this can be anything that identifies this data_asset for you
    runtime_parameters={"path": "s3://greatex-bucket/raw/retail/2021-08-02/retail_profiling.csv"},  # Add your S3 path here.
    batch_identifiers={"default_identifier_name": "validate_retail_load"},
    batch_spec_passthrough={
        "reader_method": "read_csv",
        "reader_options": {
            "nrows": 1000
        }
    }
)

validator = context.get_validator(
    batch_request=batch_request, expectation_suite_name="retail_source_suite"
)
