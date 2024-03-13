# import apache-airflow important libs
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# importing libs for api
import json
import requests
import datetime
import logging
import psycopg2

from transformer import transform_weatherAPI

# extract task
def my_extract(**kwargs):
    # fetch data from api
    payload = {'Key':'d06bb98e60c14ed4a7e24905241003',
               'q':'Delhi',
               'aqi':'no'}
    r = requests.get('http://api.weatherapi.com/v1/current.json', params=payload)

    # get json data
    r_string = r.json()

    # json ==> string
    ex_string = json.dumps(r_string)

    #  push the result inot X_com
    task_instance = kwargs['ti']
    task_instance.xcom_push(key='api_result', value=ex_string)
    # goes into the xcom
    return ex_string

# transform task
def my_transform(**kwargs):
    task_instance = kwargs['ti']
    api_data = task_instance.xcom_pull(key='api_result', task_ids='extract')

    ex_json = transform_weatherAPI(api_data)
    task_instance.xcom_push(key='transformed_weather', value=ex_json)
    return ex_json

# load task
def my_load(**kwargs):
    try:
        task_instance = kwargs['ti']
        weather_json = task_instance.xcom_pull(key='transformed_weather', task_ids='transform')
        # str ==> json
        weather_json = json.loads(weather_json)
        print(weather_json)

        conn = psycopg2.connect(host='postgres',
                                user='airflow',
                                password='airflow',
                                port='5432',
                                database='WeatherData')
        cursor = conn.cursor()  

        insert_query = """INSERT INTO temperature (location, temp_c, wind_kph, time, region) VALUES ( %s , %s, %s, %s, %s);"""
        record_to_insert = (weather_json[0]["location"], 
                            weather_json[0]["temp_c"], 
                            weather_json[0]["wind_kph"], 
                            weather_json[0]["timestamp"], 
                            weather_json[0]["region"])
        cursor.execute(insert_query, record_to_insert)

        conn.commit()
        count = cursor.rowcount
        print(count, "Record inserted successfully into temperature table.")

    except (Exception, psycopg2.Error) as error:
        print("Failed to insert record into temperature table", error)

        if conn:
            cursor.close()
            conn.close()
            print("PostgreSQL connection is closed")
        raise Exception(error)
    finally:
        if conn:
            cursor.close()
            conn.close()
            print("PostgreSQL connection is closed")

# log task
def my_print(**kwargs):
    print(kwargs)


# dag implement
with DAG("etlWeatherData",
         description="ETL Weather Data",
         start_date=datetime.datetime(2024, 2, 12),
         schedule_interval="0 * * * *",
         catchup=False,
         tags=['etlWeatherData']) as dag:
    
    extract = PythonOperator(
        task_id='extract',
        python_callable=my_extract,
        provide_context=True,
    )

    transform = PythonOperator(
        task_id='transform',
        python_callable=my_transform,
        provide_context=True,
    )

    load = PythonOperator(
        task_id='load',
        python_callable=my_load,
        provide_context=True,
    )

    print_ = PythonOperator(
        task_id='print',
        python_callable=my_print,
        provide_context=True,
    )

    extract >> transform >> load >> print_