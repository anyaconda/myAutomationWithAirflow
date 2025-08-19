podman run --name airflow-standalone \
  -d \
  -p 8080:8080 \
  -v "$PWD/dags":/opt/airflow/dags \
  -v "$PWD/requirements.txt":/requirements.txt \
  apache/airflow:2.9.1 bash -c "pip install -r /requirements.txt && airflow standalone"