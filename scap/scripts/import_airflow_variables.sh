#!/bin/sh

echo "Importing variables to airflow"
env
AIRFLOW_CONFIG="${SCAP_REV_PATH}/airflow/config"
echo "Sourcing files from $AIRFLOW_CONFIG"
ls "$AIRFLOW_CONFIG"/*.json

for config_path in "${AIRFLOW_CONFIG}"/*.json; do
    echo "Importing ${config_path}"
    sudo -u analytics-search /usr/local/bin/airflow variables --import "${config_path}"
done
