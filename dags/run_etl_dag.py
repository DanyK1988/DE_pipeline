from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

with DAG(
    dag_id='docker_etl_job',
    start_date=datetime(2026, 2, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    run_etl = DockerOperator(
        task_id='execute_etl',
        image='my_etl_image:latest', # Название контейнера
        api_version='auto',
        auto_remove=True,            # Удаление контейнера после завершения работы
        docker_url='unix://var/run/docker.sock', # Путь к сокету
        force_pull=False, # Сообщаем, что не надо скачивать файл с репозитория
        network_mode='airflow_network', # Запускаем в той же сети, где Mongo и Kafka
        environment={
            'MONGO_HOST': 'mongo',
            'KAFKA_BOOTSTRAP_SERVERS': 'kafka:9092'
        }
    )