#!/bin/sh

echo "Importing variables to airflow"
env

for config_path in "${SCAP_REV_DIR}/airflow/config/"*.json; do
    sudo -u analytics-search /usr/local/bin/airflow variables --import "${config_path}"
done
