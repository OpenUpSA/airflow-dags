from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.models.connection import Connection
from datetime import datetime

with DAG(
    'gov_za_scrape',
    description='A simple tutorial DAG',
    catchup=False,
) as dag:

    postgres_connection = Connection.get_connection_from_secrets("gov_za_scraper_postgres")
    postgresql_connection = postgres_connection.get_uri().replace("postgres", "postgresql")
    aws_connection = Connection.get_connection_from_secrets("gov_za_scraper_aws")
    aws_scheme = aws_connection.conn_type
    aws_host = aws_connection.host
    aws_port = aws_connection.port

    environment = {
        "AWS_S3_BUCKET_NAME": aws_connection.schema,
        "AWS_ACCESS_KEY_ID": aws_connection.login,
    }
    if aws_scheme and aws_host and aws_port:
        environment["AWS_S3_ENDPOINT_URL"] = f"{aws_scheme}://{aws_host}:{aws_port}"

    private_environment = {
        "DATABASE_URL": postgresql_connection,
        "AWS_SECRET_ACCESS_KEY": aws_connection.password,
    }

    t1 = DockerOperator(
        task_id="scrape-gov-za",
        start_date=datetime(2021, 1, 1),
        image="openup/domain-scraper:latest",
        environment=environment,
        private_environment=private_environment,
        command="poetry run scrapy crawl govza"
    )

    t1
