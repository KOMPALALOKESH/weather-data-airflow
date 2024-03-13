# Weather api data- - Airflow

## contents

## snapshot - workflow
<div style="text-align:center;">
  <img src="/media_files/workflow.jpg" alt="etl workflow">
</div><br>

## Docker init
start the docker

## Install airflow
pull the airflow image from the docker hub with [docker-compose.yml](docker-compose.yml)<br>
run command:
```
docker compose up
```
## Create DAG
After airflow install , create DAG for weather data etl process, this includes:
* Extract
  * Get weather data from weatherapi.com
  * Push into xcom task="extract"
* Transform
  * Transform operation on weather data ([transformer.py](dags/transformer.py)), we got by xcom pull task="extract".
  * push the transformed data into xcom task="transform"
* Load
  * Get the data from xcom task="transform"
  * Load the data into postgreSQL
* Logging
  * Log the information of above etl process.
 
## DAG
* ON the etlWeatherDataDAG.
* update DAG if necessary
* Trigger the DAG and see the output follows:
    <div style="text-align:center;">
      <img src="/media_files/etlWeatherData_graph.png" alt="etl workflow">
    </div><br>
* DAG tree:
    <div style="text-align:center;">
      <img src="/media_files/etlWeatherData_tree.png" alt="etl workflow">
    </div><br>
* Data loaded into postgresDB::
    <div style="text-align:center;">
      <img src="/media_files/workflow.jpg" alt="etl workflow">
    </div><br>
  
