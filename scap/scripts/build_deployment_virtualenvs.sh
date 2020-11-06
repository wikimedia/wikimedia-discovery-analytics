#!/usr/bin/env bash

set -e
set -o errexit
set -o nounset
set -o pipefail

# This base dir is unique per deployment, always a fresh repository
BASE_DIR="${SCAP_REV_PATH}"
ENVIRONMENTS_DIR="${BASE_DIR}/environments"
WHEEL_DIR="${BASE_DIR}/artifacts"
# Changing python versions also requires updating PYSPARK_PYTHON_VERSION
# in airflow dags. Debian buster (an-airflow1001) only has python3.7, while
# stat hosts have 3.5 and 3.7.
PYTHON="python3.7"

echo Looking for environments in $ENVIRONMENTS_DIR
for ENV_DIR in ${ENVIRONMENTS_DIR}/*; do
    REQUIREMENTS="${ENV_DIR}/requirements-frozen.txt"
    if [ ! -f "$REQUIREMENTS" ]; then
        echo No requirements found at $REQUIREMENTS
    else
        ENV_NAME="$(basename $ENV_DIR)"
        ZIP_PATH="${ENV_DIR}/venv.zip"
        VENV="${ENV_DIR}/venv"
        # The shebang line in linux has a limit of 128 bytes, which
        # we can overrun. Call python directly with pip to avoid shebang
        PIP="${VENV}/bin/python ${VENV}/bin/pip"

        # If something already exists, blow it away. We delete it on a clean
        # exit, and this should always be run by scap in a clean repo, so this
        # shouldn't occur outside testing.
        if [ -e "$VENV" ]; then
            echo "WARNING: Deleting unexpected virtualenv at $VENV"
            rm -rf "$VENV"
        fi
        # This doesn't really matter, but seems it might be useful info
        if [ -e "$ZIP_PATH" ]; then
            echo "WARNING: venv already packaged. Re-packaging."
            rm "$ZIP_PATH"
        fi

        # Make a fresh virtualenv
        mkdir -p "$VENV"
        virtualenv --never-download --python "$PYTHON" "$VENV"

        pipargs=""
        if [ "${PIP_ALLOW_INDEX:-}" != "yes" ]; then
            pipargs="--no-index"
        fi

        # Install our packages
        $PIP install \
            -vv \
            $pipargs \
            --find-links "$WHEEL_DIR" \
            --upgrade \
            --force-reinstall \
            -r "$REQUIREMENTS"
        # Wrap it all up to be deployed by spark to executors
        ( cd "$VENV" && zip -qr "${ZIP_PATH}" . )
        echo Saved virtualenv to $ZIP_PATH
    fi
done
