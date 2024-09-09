# import the libraries
from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow.models import DAG
# Operators; you need this to write tasks!
from airflow.operators.bash_operator import BashOperator # type: ignore
# This makes scheduling easy
from airflow.utils.dates import days_ago

#defining DAG arguments

default_args = {
    'owner' : 'reirivero',
    'start_date' : days_ago(0),
    'email' : '[reinaldo.rivero.ro@gmail.com]',
    'email_on_failure' : True,
    'email_on_retry' : True,
    'retries' : 1,
    'retry_delay' : timedelta(minutes=5),
}

# defining the DAG

dag = DAG(
    'ETL_toll_data',
    schedule_interval=timedelta(days=1),
    default_args=default_args,
    description='Apache Airflow - bashOperator',
)

# define tasks

# define the task "unzip_data"

unzip_data= BashOperator(
    task_id='unzip_data',
    bash_command='tar -xzf /opt/airflow/data/tolldata.tgz -C /opt/airflow/data',
    dag=dag,
)

# defining the task "extract_data_from_csv"

extract_data_from_csv= BashOperator(
    task_id='extract_data_from_csv',
    bash_command='cut -d"," -f1-4 < /opt/airflow/data/vehicle-data.csv > /opt/airflow/data/csv_data.csv',
    dag=dag,                        
)

# defining the task "extract_data_from_tsv"

extract_data_from_tsv= BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='tr "\t" "," < /opt/airflow/data/tollplaza-data.tsv | cut -d"," -f5-7 > /opt/airflow/data/tsv_data.csv',
    dag=dag,
)

# defining the task "extract_data_from_fixed_width"

extract_data_from_fixed_width= BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command="""
    awk '{print substr($0, 1, 6) "," substr($0, 7, 20) "," substr($0, 27, 5) "," substr($0, 32, 12) "," substr($0, 44, 4) "," substr($0, 48, 10) "," substr($0, 58, 4) "," substr($0, 62, 5)}' /opt/airflow/data/payment-data.txt | sed 's/ *, */,/g' | cut -d"," -f7-8 > /opt/airflow/data/fixed_width_data.csv
    """,
    dag=dag,
)

consolidate_data= BashOperator(
    task_id='consolidate_data',
    bash_command='',
    dag=dag,
)

transform_data= BashOperator(
    task_id='transform_data',
    bash_command='',
    dag=dag,
)

# task pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data