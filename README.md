# Testing Airflow DAGs with Great Expectations

## Evaluation

- GreatExpectationsOperator is experimental and not fully supported yet
- Version 3 API doesn't work with GreatExpectationsOperator on Airflow
- Great Expectations needs to be installed on the Airflow Worker
- Great Expectations folder has to be mounted
- When connecting to Postgres container on local machine, the host name should be `localhost`,
  **BUT** when connecting from the Airflow Worker to run a DAG, the host name should be `postgres`.
- To run the BashOperator using GE CLI commands, `cd` into the directory where `great_expectations` folder is located

## Recommendations

- Use Version 2 API when you want to use Great Expectations with Airflow
- Specify the library to be installed (`great_expectations`) in the `PIP_ADDITIONAL_REQUIREMENTS` in the `docker-compose` file
- **Prerequisites:** Create a Data Context, connect to a Datasource, create an Expectations Suite, create a Checkpoint, and have an Airflow DAG ready
- Recommended to use BashOperator to validate data batches since it is the most intuitive and straightforward. Also works with both V2 and V3 APIs.
- Can also use PythonOperator for validation but additional setup is necessary
- Simply plug in the BashOperator as a Task within the Airflow DAG
- **DO NOT** use GreatExpectationsOperator, use BashOperator/PythonOperator instead.
- Keep in mind the host names to be used:
  - when connecting to Postgres from local machine or when connecting from Airflow Worker container
  - when running Airflow on local machine or when running Airflow on Docker
  - specify/edit the credentials in the `config-variables.yml` file in the `great_expectations/uncommitted` folder

## Debugging

- Version 3 API in Airflow is not supported: cannot find table, cannot get batches, cannot connect to Datasources (`KeyError`)
- Connectivity issues to Postgres container
- BashOperator cannot find Great Expectations folder
- **Very useful for debugging** to enter the Airflow Worker container (with `docker exec -it`) and directly run commands inside
  - helps with finding where folders are located
  - helps with comparison between running GE CLI commands in the container vs running CLI commands in local machine. You can see the error logs without having to run the Airflow DAG.
  - helps with connectivity issues and resolving host names

## Debugging S3 Connector

- Default regex applies to the entire S3 bucket URI other than `s3://bucket-name` and file extension, e.g. `.csv`
- Two common errors:
  1. List index out of range - the above point and the second link below should fix this
  2. Says invalid bucket name "greatex-bucket\raw" even though my bucket name "greatex-bucket" is completely valid
    This has a suspicious backslash which might indicate something to do with Windows,
    so I tried out the same operation on Linux, and there was no error!
- Have to redo the whole pipeline on Linux and raise an issue.

[Airflow macros](https://www.datafold.com/blog/3-most-underused-features-of-apache-airflow/)
[Regex for inferring filenames](https://docs.greatexpectations.io/en/latest/guides/how_to_guides/configuring_datasources/how_to_configure_an_inferredassetdataconnector.html)
