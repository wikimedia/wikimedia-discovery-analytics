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

header() {
    echo
    echo '--------------'
    echo
    echo $*
    echo
    echo '--------------'
    echo
}

EXPORTED_PIP="no"

echo "Looking for environments in ${1:-${BASE}/environments/*}"
for ENV_DIR in ${1:-${BASE}/environments/*}; do
    ENV_NAME="$(basename "$ENV_DIR")"
    REQUIREMENTS="${ENV_DIR}/requirements.txt"
    REQUIREMENTS_FROZEN="${ENV_DIR}/requirements-frozen.txt"
    if [ ! -f "${REQUIREMENTS}" ]; then
        header "No python packaging needed for $ENV_NAME"
    else
        header "Building python packaging for $ENV_NAME"
        rm -rf "${BUILD}"
        mkdir -p "${VENV}"
        virtualenv --python "${PYTHON_PATH:-python3.7}" "${VENV}"
        # Assert that $PYTHON_PATH referred to python3.7, it is the only
        # version supported by debian buster which is run on an-airflow
        test -x ${VENV}/bin/python3.7

        # Install pip and wheel to ensure we export a modern version.
        $PIP install --find-links "${WHEEL_DIR}" \
            --upgrade --force-reinstall pip wheel

        # Install the frozen requirements first to avoid unnecessary upgrades.
        # To remove a package it must be deleted from both requirements files.
        if [ -e "${REQUIREMENTS_FROZEN}" ]; then
            $PIP install --find-links "${WHEEL_DIR}" \
                --requirement "${REQUIREMENTS_FROZEN}"
        fi
        $PIP install --find-links "${WHEEL_DIR}" \
            --requirement "${REQUIREMENTS}"
        $PIP freeze --local | grep -v pkg-resources > "${REQUIREMENTS_FROZEN}"
        $PIP install wheel
        $PIP wheel --find-links "${WHEEL_DIR}" \
                --wheel-dir "${WHEEL_DIR}" \
                --requirement "${REQUIREMENTS_FROZEN}"

        # When installing to a debian 10.9 system it will have pip 18 which can't
        # properly install some modern wheels with binaries. Ensure we package up
        # pip and wheel to be used on deployment.
        if [ "$EXPORTED_PIP" = "no" ]; then
            EXPORTED_PIP="yes"
            $PIP wheel --find-links "${WHEEL_DIR}" \
                --wheel-dir "${WHEEL_DIR}" \
                pip wheel
        fi
    fi
done

