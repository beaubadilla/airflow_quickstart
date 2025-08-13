# NBA Daily Scoreboard
This is my quickstart project to learn Apache Airflow. It will fetch yesterday's games and save to SQLite.

# Install Airflow
```
export AIRFLOW_VERSION=2.9.3
export PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
export CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
export AIRFLOW_HOME=~/airflow
airflow db init
```