from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import requests
from datetime import datetime, timedelta

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

# DAG Creation
with DAG(
    'stock_data_etl_dag',  # DAG name
    default_args=default_args,
    description='ETL DAG for fetching stock data from Alpha Vantage and storing it in Snowflake',
    schedule_interval=None,
    start_date=datetime.today(),
    tags=['stock_data'],
) as dag:

    # Task to fetch stock data from Alpha Vantage API
    @task
    def fetch_stock_data():
        alpha_vantage_api = Variable.get('api_key')  # Fetch API key from Airflow Variables
        stock_symbol = 'C'

        date_90_days_ago = datetime.now() - timedelta(days=90)
        formatted_date = date_90_days_ago.strftime('%Y-%m-%d')

        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={stock_symbol}&apikey={alpha_vantage_api}&outputsize=compact"
        response = requests.get(url)
        data = response.json()

        results = []
        for date, daily_data in data['Time Series (Daily)'].items():
            if date >= formatted_date:
                stock_info = {
                    'date': date,
                    'open': float(daily_data['1. open']),
                    'high': float(daily_data['2. high']),
                    'low': float(daily_data['3. low']),
                    'close': float(daily_data['4. close']),
                    'volume': int(daily_data['5. volume'])
                }
                results.append(stock_info)

        return results  # Return the fetched data

    # Task to insert stock data into Snowflake
    @task
    def insert_into_snowflake(stock_data):
        snowflake_conn_id = 'my_snowflake_conn'  # Reference to Airflow Snowflake connection
        hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)

        conn = hook.get_conn()
        cur = conn.cursor()

        # Create table if it doesn't exist
        create_table_query = """
        CREATE OR REPLACE TABLE NPS (
            stock_date DATE,
            open_price FLOAT,
            high_price FLOAT,
            low_price FLOAT,
            close_price FLOAT,
            volume INTEGER,
            symbol VARCHAR(10),
            PRIMARY KEY (stock_date, symbol)
        );
        """
        cur.execute(create_table_query)

        # Insert data with idempotency check
        for record in stock_data:
            insert_query = """
            INSERT INTO NPS (stock_date, open_price, high_price, low_price, close_price, volume, symbol)
            SELECT %s, %s, %s, %s, %s, %s, 'C'
            WHERE NOT EXISTS (
                SELECT 1 FROM NPS WHERE stock_date = %s AND symbol = 'C'
            );
            """
            cur.execute(insert_query, (
                record['date'], record['open'], record['high'], record['low'], record['close'], record['volume'], record['date']))

        conn.commit()
        cur.close()

    # Task to count records in Snowflake (used for idempotency check)
    @task
    def count_records(symbol):
        snowflake_conn_id = 'my_snowflake_conn'
        hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)

        conn = hook.get_conn()
        cur = conn.cursor()

        count_query = "SELECT COUNT(*) FROM NPS WHERE symbol = %s"
        cur.execute(count_query, (symbol,))
        count = cur.fetchone()[0]

        cur.close()
        return count

    # Idempotency check
    @task
    def check_idempotency(initial_count, final_count):
        if initial_count == final_count:
            print("Idempotency achieved")
        else:
            print("Not idempotent")

    # Define task variables to capture each task's output
    stock_data_task = fetch_stock_data()
    insert_data_task = insert_into_snowflake(stock_data_task)
    initial_count_task = count_records('C')
    second_insert_task = insert_into_snowflake(stock_data_task)
    final_count_task = count_records('C')
    idempotency_task = check_idempotency(initial_count_task, final_count_task)

    # Define task dependencies
    stock_data_task >> insert_data_task >> initial_count_task >> second_insert_task >> final_count_task >> idempotency_task
