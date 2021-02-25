import re
import os
import mysql.connector
import pandas as pd

from decimal import Decimal
from selenium.webdriver import Chrome, ChromeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from sqlalchemy import create_engine
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago



DB_HOST = os.environ.get('MYSQL_HOST')
DB_USER = os.environ.get('MYSQL_USER')
DB_PASS = os.environ.get('MYSQL_PASS')
DB_PORT = os.environ.get('MYSQL_PORT')
DB_NAME = 'crypto_etl'
TABLE_NAME = 'coins'
URL = 'https://coinmarketcap.com/watchlist/60321ee5b01cab343e1e37d6'


def currency_to_number(value) -> Decimal:
    '''Convert string value to number'''
    return Decimal(re.sub('[^0-9\.-]', '', value))


def get_crypto_data() -> None:
    options = ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--incognito')
    options.add_argument('--disable-dev-shm-usage')

    driver = Chrome('/usr/local/bin/chromedriver', options=options)
    driver.get(URL)

    try:
        table = WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.TAG_NAME, 'table'))
        )
    except:
        print('------------ Error scraping the page')
    finally:
        table_body = table.find_element_by_tag_name('tbody')
        cells = table_body.find_elements_by_class_name('sc-1eb5slv-0')
        prices = []
        for price in table_body.find_elements_by_class_name('price___3rj7O'):
            prices.append(currency_to_number(price.text))

        N = 7  # number of items per row
        data = []
        
        for i in range(0, len(cells), N):
            name, symbol, market_cap, vol_usd, vol_coin, supply = [
                cell.text for cell in cells[i+1:i+N]]
            
            market_cap = currency_to_number(market_cap)
            vol_usd = currency_to_number(vol_usd)
            vol_coin = currency_to_number(vol_coin.split()[0])
            supply = currency_to_number(supply.split()[0])

            data.append([name, symbol, market_cap, vol_usd, vol_coin, supply])

        print('------------ Scrap completed')
        

        # -----------------------------------------------
        # Connect to DB and store data
        mysql_db = mysql.connector.connect(
            host = DB_HOST,
            user = DB_USER,
            passwd = DB_PASS,
            database = DB_NAME
        )

        # SQLALchemy setup
        db = f"mysql+mysqlconnector://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(db)

        col_names = ('name', 'symbol', 'market_cap', 'volume_usd', 'volume_coin', 'supply')

        df = pd.DataFrame(columns=col_names, data=data)
        df.insert(loc=2, column='price_usd', value=pd.Series(prices))
        df.to_sql(TABLE_NAME, engine, if_exists='append', index=False)

        mysql_db.commit()
        mysql_db.close()

        print('------------ Data inserted')
    

# -----------------------------------------------
# DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(0),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


dag = DAG(
    dag_id='crypto_dag',
    default_args=default_args,
    description='Get and stores crypto into a MySQL DB',
    schedule_interval=timedelta(minutes=30)
)


get_crypto_data_task = PythonOperator(
    task_id='get_crypto_data',
    python_callable=get_crypto_data,
    dag=dag
)


notify = BashOperator(
    task_id='notify',
    bash_command='echo "Operation Completed!"',
    dag=dag
)


get_crypto_data_task >> notify