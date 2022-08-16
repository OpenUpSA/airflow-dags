from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime
from airflow.models import Variable

with DAG(
    'gov_za_scrape',
    description='A simple tutorial DAG',
    catchup=False,
) as dag:

    environment = {
        "AWS_S3_BUCKET_NAME": "{{ var.value.gov_za_scrape_aws_s3_bucket_name }}",
        "AWS_ACCESS_KEY_ID": "{{ var.value.gov_za_scrape_aws_acces_key_id }}",
    }

    aws_s3_endpoint_url = Variable.get("gov_za_scrape_aws_s3_endpoint_url", default_var=None)
    if aws_s3_endpoint_url:
        environment["AWS_S3_ENDPOINT_URL"] = aws_s3_endpoint_url

    private_environment = {
        "DATABASE_URL": Variable.get("gov_za_scrape_database_url"),
        "AWS_SECRET_ACCESS_KEY": Variable.get("gov_za_scrape_aws_secret_access_key"),
    }

    t1 = DockerOperator(
        task_id="scrape-gov-za",
        start_date=datetime(2021, 1, 1),
        image="openup/domain-scraper:latest",
        environment=environment,
        private_environment=private_environment,
        command="poetry run scrapy crawl govza -s JOBDIR=jobdir -s MEMUSAGE_ENABLED=True -s MEMUSAGE_WARNING_MB=500 -s MEMDEBUG_ENABLED=True",
        force_pull=True,
    )

    t1
