from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator

# Define the S3 bucket and file details
s3_prefix = 's3://airflow-aws-snow/folder/DS1_C7_S6_Hackathon_Sales_Data.csv'
s3_bucket = None

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 8),
    'email': ['myemail@domain.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

with DAG('airflow-aws-snow_with_email_notification_etl',
        default_args=default_args,
        schedule_interval = '@daily',
        catchup=False) as dag:

        is_file_in_s3_available = S3KeySensor(
        task_id='tsk_is_file_in_s3_available',
        bucket_key=s3_prefix,
        bucket_name=s3_bucket,
        aws_conn_id='aws_s3_conn',
        wildcard_match=False,  # Set this to True if you want to use wildcards in the prefix
        # timeout=60,  # Optional: Timeout for the sensor (in seconds)
        poke_interval=3,  # Optional: Time interval between S3 checks (in seconds)
        )

        create_table = SnowflakeOperator(
            task_id = "create_table",
            snowflake_conn_id = 'conn_id_snowflake',           
            sql = '''DROP TABLE IF EXISTS SALES;
                    CREATE TABLE IF NOT EXISTS SALES(
                    ORDERNUMBER varchar(255) default NULL,
					QUANTITYORDERED varchar(255) default NULL,
					PRICEEACH varchar(255) default NULL,
					ORDERLINENUMBER varchar(255) default NULL,
					SALES varchar(255) default NULL,
					ORDERDATE varchar(255) default NULL,
					STATUS varchar(255) default NULL,
					QTR_ID varchar(255) default NULL,
					MONTH_ID varchar(255) default NULL,
					YEAR_ID	 varchar(255) default NULL,
					PRODUCTLINE	 varchar(255) default NULL,
					MSRP	 varchar(255) default NULL,
					PRODUCTCODE	 varchar(255) default NULL,
					CUSTOMERNAME	 varchar(255) default NULL,
					PHONE	 varchar(255) default NULL,
					ADDRESSLINE1 varchar(255) default NULL,	
					ADDRESSLINE2	 varchar(255) default NULL,
					CITY	 varchar(255) default NULL,
					STATE	 varchar(255) default NULL,
					POSTALCODE	 varchar(255) default NULL,
					COUNTRY	 varchar(255) default NULL,
					TERRITORY	 varchar(255) default NULL,
					CONTACTLASTNAME	 varchar(255) default NULL,
					CONTACTFIRSTNAME	 varchar(255) default NULL,
					DEALSIZE varchar(255) default NULL
                )
                 '''
        )

        is_file_in_s3_available >> create_table

        
        copy_csv_into_snowflake_table = SnowflakeOperator(
            task_id = "tsk_copy_file_into_snowflake_table",
            snowflake_conn_id = 'conn_id_snowflake',
            sql = '''COPY INTO sales_database.new_sales_schema.SALES from @sales_database.new_sales_schema.snowflake_ext_stage_yml FILE_FORMAT = csv_format
                   '''
        )

        is_file_in_s3_available >> create_table >> copy_csv_into_snowflake_table




