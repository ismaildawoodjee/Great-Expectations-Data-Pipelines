from ruamel import yaml
import great_expectations as ge
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest

context = ge.get_context()

datasource_config = {
    "name": "retail_raw",
    "class_name": "Datasource",
    "execution_engine": {"class_name": "PandasExecutionEngine"},
    "data_connectors": {
        "default_runtime_data_connector_name": {
            "class_name": "RuntimeDataConnector",
            "batch_identifiers": ["default_identifier_name"],
        },
        "default_inferred_data_connector_name": {
            "class_name": "InferredAssetS3DataConnector",
            "bucket": "greatextestbucket",
            "prefix": "greatexfolder/",
            "default_regex": {
                "group_names": ["data_asset_name"],
                "pattern": "(.*)\\.csv",
            },
        },
    },
}

context.test_yaml_config(yaml.dump(datasource_config))

context.add_datasource(**datasource_config)

batch_request = RuntimeBatchRequest(
    datasource_name="retail_raw",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="retail_profiling_raw",  # this can be anything that identifies this data_asset for you
    runtime_parameters={"path": "s3a://greatextestbucket/greatexfolder/retailprofiling.csv"},  # Add your S3 path here.
    batch_identifiers={"default_identifier_name": "retail_raw_batch"},
)

validator = context.get_validator(
    batch_request=batch_request, expectation_suite_name="retail_suite"
)
print(validator.head())
