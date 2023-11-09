import os
import yaml

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from box import Box

from task_definitions import ScrapingProcess

ABSOLUTE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

with open(f'{ABSOLUTE_PATH}/data_input/config.yaml', 'r') as file:
    structure = Box(yaml.safe_load(file))

scraper = ScrapingProcess(structure)

with DAG(
    "query_scraper",
    start_date=pendulum.datetime(2022, 1, 30, tz="America/Sao_Paulo"),
    schedule="0 9 11 * *",
    catchup=False,
    max_active_runs=2,
):
    prepare_task = PythonOperator(
        task_id="prepare",
        provide_context=True,
        python_callable=scraper.prepare,
    )

    collect_task = PythonOperator(
        task_id="collect_items",
        provide_context=True,
        python_callable=scraper.collect,
    )

    specs_task = PythonOperator(
        task_id="collect_items_specifications",
        provide_context=True,
        python_callable=scraper.scrape_specifications,
    )

    validade_collected = PythonOperator(
        task_id="validate_item_descriptions",
        provide_context=True,
        python_callable=scraper.validade_collected_items,
    )

    print_task = PythonOperator(
        task_id="print_items",
        provide_context=True,
        python_callable=scraper.print_items,
    )

    export_task = PythonOperator(
        task_id="export",
        provide_context=True,
        python_callable=scraper.export,
    )

    (prepare_task >> collect_task >> specs_task >> validade_collected >> print_task >> export_task)
