# basic_crypto_scrapping

On my quest to learn Apache Airflow and ETL pipelines, I've create a simple DAG to scrap data from https://coinmarketcap.com. I setup a watchlist url of selected coins to keep the data consistent to avoid future changes from the standard top 10 list from the site.
I use Airflow to schedule a 30mins data extraction using Selenium, then transform the data to comply to my DB table setup and load the data to a my local MySQL DB. Had it running for about 24hrs and got about 820 records. 
