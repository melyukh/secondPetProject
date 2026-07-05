from datetime import datetime
from airflow.decorators import dag, task                                            #type: ignore
from airflow.operators.bash import BashOperator                                     #type: ignore
from airflow.operators.email import EmailOperator                                   #type: ignore
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator      #type: ignore
from pathlib import Path

DAG_ROOT = Path(__file__).parent.parent

args = {
    "owner": "dzhyar"
}

@dag(
    dag_id='etldag',
    description="etlprocessdag",
    tags=["pyspark", "postgres"],
    
    schedule=None,
    start_date=datetime(2026, 6, 30),
    catchup=False,

    default_args=args,
    render_template_as_native_obj=True,
    template_searchpath=[str(DAG_ROOT / "scripts" / "sql")]
)
def flights_processing_dag():

    download_files_into_data_folder = BashOperator(
        task_id="download_files_into_data_folder",
        bash_command=f"python3 {DAG_ROOT}/scripts/python/downloader.py",
        do_xcom_push=True
    )

    @task.branch
    def check_for_new_files(result_of_downloading):
        if int(result_of_downloading) == 0:
            return ['send_responce_of_skipping_dag']
        return ['extract_and_load_into_buffer']
    
    extract_and_load_into_buffer = BashOperator(
        task_id="extract_and_load_into_buffer",
        bash_command=f'python3 {DAG_ROOT}/scripts/spark/extract_and_load.py'
    )

    postgres_commiting_data = SQLExecuteQueryOperator(
        task_id="postgres_commiting_data",
        conn_id="postgres_avialines_connection",
        sql="commitig_data.sql"
    )

    get_statistics_of_conflicted_rows = SQLExecuteQueryOperator(
        task_id="get_statistics_of_conflicted_rows",
        conn_id="postgres_avialines_connection",
        sql="get_conflict_stat.sql"
    )

    get_airline_statistics = BashOperator(
        task_id="get_airline_statistics",
        bash_command=f"python3 {DAG_ROOT}/scripts/spark/airlineStatsFromPostgres.py",
        do_xcom_push=True
    )

    send_visualization_onto_email = EmailOperator(
        task_id="send_visualization_onto_email",
        to="{{ var.json.email_recipients }}",
        subject="Информация о проделанной работе по обработке данных",
        files=["{{ ti.xcom_pull(task_ids='get_airline_statistics') }}"],
        html_content="<p>Визуализация статистики на сегодня</p>"
    )

    send_responce_of_skipping_dag = EmailOperator(
        task_id="send_responce_of_skipping_dag",
        to="{{ var.json.email_recipients }}",
        subject="Информация о проделанной работе по обработке данных",
        html_content="<p>Выполнение дага было пропущено</p>"
    )

    result = download_files_into_data_folder.output
    branch_task = check_for_new_files(result_of_downloading=result)

    branch_task >> [send_responce_of_skipping_dag, extract_and_load_into_buffer]

    extract_and_load_into_buffer >> \
    postgres_commiting_data >> \
    [get_airline_statistics, get_statistics_of_conflicted_rows]

    get_airline_statistics >> send_visualization_onto_email

flights_processing_dag()
