
$USER_NAME = 'ismaildawoodjee'
$REGION = 'ap-southeast-1'
$PROJECT_NAME = 'great-expectations-data-pipelines'
$BUCKET_NAME = 'idawoodjee-batch-bucket'

$AWS_ACCESS_KEY_ID = aws configure get aws_access_key_id --profile $USER_NAME
$AWS_SECRET_ACCESS_KEY = aws configure get aws_secret_access_key --profile $USER_NAME


docker exec -d $PROJECT_NAME'_airflow-webserver_1' `
    airflow variables set BUCKET_NAME $BUCKET_NAME

docker exec -d $PROJECT_NAME'_airflow-webserver_1' `
    airflow connections add 'aws_default' `
    --conn-type 'aws' `
    --conn-login $AWS_ACCESS_KEY_ID `
    --conn-password $AWS_SECRET_ACCESS_KEY `
    --conn-extra "{\`"region_name\`": \`"$REGION\`"}"

# # Either specify AIRFLOW_CONN_POSTGRES_DEFAULT in `docker-compose` file or add it using Shell command below
# docker exec -d $PROJECT_NAME'_airflow-webserver_1' `
#     airflow connections add 'postgres_default' `
#     --conn-type 'postgres' `
#     --conn-login 'airflow' `
#     --conn-password 'airflow' `
#     --conn-host 'postgres' `
#     --conn-port 5432 `
#     --conn-schema 'ecommerce'
