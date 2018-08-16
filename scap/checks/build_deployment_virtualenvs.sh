#!/usr/bin/env bash

set -e
set -o errexit
set -o nounset
set -o pipefail

BASE_DIR="$(dirname $(dirname $(dirname $(realpath $0))))"
OOZIE_DIR="${BASE_DIR}/oozie"
WHEEL_DIR="${BASE_DIR}/artifacts"

echo Looking for tasks in $OOZIE_DIR
for TASK_DIR in ${OOZIE_DIR}/*; do
    REQUIREMENTS="${TASK_DIR}/requirements-frozen.txt"
    if [ ! -f "$REQUIREMENTS" ]; then
        echo No requirements found at $REQUIREMENTS
    else
        TASK_NAME="$(basename $TASK_DIR)"
        ZIP_PATH="${TASK_DIR}/venv.zip"
        VENV="${TASK_DIR}/venv"
        PIP="${VENV}/bin/pip"

        # Ensure we have a virtualenv
        if [ ! -x "$PIP" ];then
            mkdir -p "$VENV"
            virtualenv --never-download --python python3 "$VENV"
        fi

        # Debian jessie needs these for proper wheel handling
        $PIP install \
            -vv \
            --no-index \
            --find-links "${WHEEL_DIR}" \
            --upgrade \
            --force-reinstall \
            pip wheel
        # Install or upgrade our packages
        $PIP install \
            -vv \
            --no-index \
            --find-links "$WHEEL_DIR" \
            --upgrade \
            --force-reinstall \
            -r "$REQUIREMENTS"
        # Wrap it all up to be deployed by spark to executors
        ( cd "$VENV" && zip -qr "${ZIP_PATH}" . )
        # When using oozie (or spark-submit --master yarn --deploy-mode cluster)
        # the actual venv directory is unnecessary, only the zip which is
        # deployed and decompressed where needed.
        rm -rf "$VENV"
        echo Saved virtualenv to $ZIP_PATH
    fi
done
