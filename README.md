# NBA Daily Scoreboard
This is my quickstart project to learn Apache Airflow. It will fetch yesterday's games and save to SQLite.

# Tech
Python 3.11.5  
Airflow 2.9.3  
nba_api 1.10.0

# Learnings
Medallion Architecture: data design pattern to logically organize data in a lakehouse. Incrementally improves the structure and quality of data as it flows.
* Bronze = raw data
* Silver = filtered, cleaned, and/or augmented data
* Gold = business-level aggregates  

DAG files are placed in the `airflow/` folder (default=`~/airflow/`) rather than the respective project folder.  
`PythonOperator`: Allows for tasks to be set up with Python functions  
`default_args["depends_on_past"]` for DAG context manager: enable/disable whether this task needs to check if the previous instance of the task failed or not.  
`catchup` for DAG context manager: will run the task multiple times based on the `start_date` if it it needs to "catch up"  
`>>` operator to direct one task to the next  
`Skipped` is an alternative task status that we can utlize  

# Install Airflow
```
export AIRFLOW_VERSION=2.9.3
export PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
export CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
export AIRFLOW_HOME=~/airflow
airflow db init
```
Find DAGs in `~/airflow/dags/`

# Airflow
```
export AIRFLOW_HOME=~/airflow
airflow db init  # create metadata. only need to run again if major changes are needed for metadata (e.g. switch from SQlite to PostgreSQL)
airflow scheduler  # turn on scheduler
airflow webserver --port 8080  # turn on webserver
airflow dags trigger nba_daily_scoreboard  # manually trigger dag
```

# SQLite3
```
sqlite3 ~/airflow/data/nba_data.db "SELECT * FROM raw_scoreboard;"
sqlite3 ~/airflow/data/nba_data.db "SELECT * FROM games;"
```