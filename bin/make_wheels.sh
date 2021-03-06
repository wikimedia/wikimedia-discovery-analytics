#!/usr/bin/env bash
#
# Prepare python dependencies for deployment.
#
# Scans defined oozie tasks for requirements.txt. For each task that defines
# a requirements.txt collect all dependencies as wheel files into artifacts/.
# Write out a requirements-frozen.txt next to the original specifying exact
# versions to install when deploying.
#

set -e
set -o errexit
set -o nounset
set -o pipefail

BASE="$(dirname $(dirname $(realpath $0)))"
BUILD="${BASE}/_build"
VENV="${BUILD}/venv"
WHEEL_DIR="${BASE}/artifacts"
PIP="${VENV}/bin/pip"


# Used by wheel >= 0.25 to normalize timestamps. Timestamp
# taken from original debian patch:
# https://bugs.debian.org/cgi-bin/bugreport.cgi?att=1;bug=776026;filename=wheel_reproducible.patch;msg=5
export SOURCE_DATE_EPOCH=315576060

echo "Looking for environments in ${BASE}/environments/*"
for ENV_DIR in ${BASE}/environments/*; do
    ENV_NAME="$(basename "$ENV_DIR")"
    REQUIREMENTS="${ENV_DIR}/requirements.txt"
    REQUIREMENTS_FROZEN="${ENV_DIR}/requirements-frozen.txt"
    if [ ! -f "${REQUIREMENTS}" ]; then
        echo "No python packaging needed for $ENV_NAME"
    else
        echo "Building python packaging for $ENV_NAME"
        rm -rf "${BUILD}"
        mkdir -p "${VENV}"
        virtualenv --python "${PYTHON_PATH:-python3.7}" "${VENV}"
        # Assert that $PYTHON_PATH referred to python3.7, it is the only
        # version supported by debian buster which is run on an-airflow
        test -x ${VENV}/bin/python3.7

        # Install the frozen requirements first to avoid unnecessary upgrades.
        # To remove a package it must be deleted from both requirements files.
        if [ -e "${REQUIREMENTS_FROZEN}" ]; then
            $PIP install -r "${REQUIREMENTS_FROZEN}"
        fi
        $PIP install -r "${REQUIREMENTS}"
        $PIP freeze --local | grep -v pkg-resources > "${REQUIREMENTS_FROZEN}"
        $PIP install wheel
        $PIP wheel --find-links "${WHEEL_DIR}" \
                --wheel-dir "${WHEEL_DIR}" \
                --requirement "${REQUIREMENTS_FROZEN}"
    fi
done

