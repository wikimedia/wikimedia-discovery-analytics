#!/bin/bash
#
# As of late 2020 the analytics cluster is running debian stretch, but all
# hosts accessing it are running buster. This means we can no longer build the
# venv's necessary from hosts like an-airflow1001 or stat1007.  Instead we are
# going to build venvs inside the cluster itself, using skein to submit a
# simple bash script that runs the same process as a scap deploy.  Results are
# uploaded to a timestamped directory in hdfs:///wmf/discovery/spark_venv/
set -e
set -o errexit
set -o nounset
set -o pipefail

REPO_PATH="${REPO_PATH:-/srv/deployment/wikimedia/discovery/analytics}"
SKEIN="${SKEIN:-/srv/deployment/search/airflow/venv/bin/skein}"

TMP_PATH="$(mktemp --directory)"
trap 'rm -rf -- "$TMP_PATH"' EXIT

REPO_GIT_HASH="$(git -C "$REPO_PATH" rev-parse --short HEAD)"
REPO_TGZ="${TMP_PATH}/src.tgz"
SKEIN_CONF_PATH="${TMP_PATH}/skein.yaml"

OUTPUT_PATH="hdfs:///wmf/discovery/spark_venv/$(date --utc '+%Y-%m-%dT%H.%M.%S')-${REPO_GIT_HASH}"


debug_print_path() {
    echo
    echo ----
    echo
    echo $1
    echo
    cat $1
    echo
    echo ----
    echo
}

if [ "$(whoami)" != "analytics-search" ]; then
    echo "Must be run as analytics-search"
    echo "but you are $(whoami)"
    exit 1
fi


# Package up the repo for shipping. Shipped files are always read-only,
# while directories are writable. Exclude venv.zip so we don't fail trying
# to overwrite a read-only file.
tar -C "$REPO_PATH" --exclude-vcs --exclude=venv.zip -czf "$REPO_TGZ" .

# Be extra careful trying to use variables here, it's expanded once
# when writing and the script: key is expanded a second time when
# executing. Use \$ for execution time variables.

cat >$SKEIN_CONF_PATH <<EOD
name: build_discolytics_venv

services:
    build:
        resources:
            vcores: 1
            memory: 512 MiB
        files:
            src: $REPO_TGZ
        script: |
            set -e
            set -o errexit
            set -o nounset
            set -o pipefail

            SCAP_REV_PATH="\${PWD}/src" src/scap/scripts/build_deployment_virtualenvs.sh

            hdfs dfs -mkdir -p $OUTPUT_PATH
            for env_zip in src/environments/*/venv.zip; do
                env_name="\$(basename \$(dirname "\$env_zip"))"
                hdfs dfs -put "\$env_zip" "${OUTPUT_PATH}/\${env_name}.venv.zip"
            done
EOD

debug_print_path "$SKEIN_CONF_PATH"

ls -l "$TMP_PATH"

echo "Attempting to build venvs. Will save to $OUTPUT_PATH"

YARN_APP_ID=$(/usr/bin/env SKEIN_CONFIG=/var/run/airflow/skein "$SKEIN" application submit "$SKEIN_CONF_PATH")

echo "found id: $YARN_APP_ID"

# Unfortunately this isn't the typical skein use case, submit through the cli
# is fire-and-forget. Ping yarn cli to figure out when its done.

echo -n 'Waiting...'
while [ $(yarn application -status "$YARN_APP_ID" 2>/dev/null | awk '/\tState :/ { print $3 }') != "FINISHED" ]; do
    echo -n '.'
    sleep 15s
done

FINAL_STATE=$(yarn application -status "$YARN_APP_ID" 2>/dev/null | awk '/\tFinal-State :/ { print $3 }')
if [ "$FINAL_STATE" == "FAILED" ]; then
    echo
    echo "WARNING: Failed build. For further details see yarn logs."
    echo
    echo "  yarn logs -applicationId $YARN_APP_ID"
    echo
    exit 1
fi

echo
echo
echo "Final results of $OUTPUT_PATH:"
hdfs dfs -ls "$OUTPUT_PATH"
