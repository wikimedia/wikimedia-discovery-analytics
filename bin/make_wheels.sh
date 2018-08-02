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

echo "Looking for tasks in ${BASE}/oozie/*"
for TASK_DIR in ${BASE}/oozie/*; do
    TASK_NAME="$(basename "$TASK_DIR")"
    REQUIREMENTS="${TASK_DIR}/requirements.txt"
    REQUIREMENTS_FROZEN="${TASK_DIR}/requirements-frozen.txt"
    if [ ! -f "${REQUIREMENTS}" ]; then
        echo "No python packaging needed for $TASK_NAME"
    else
        echo "Building python packaging for $TASK_NAME"
        rm -rf "${BUILD}"
        mkdir -p "${VENV}"
        virtualenv --python "${PYTHON_PATH:-python3}" "${VENV}"

        $PIP install -r "${REQUIREMENTS}"
        $PIP freeze --local | grep -v pkg-resources > "${REQUIREMENTS_FROZEN}"
        $PIP install pip wheel
        # Debian jessie based hosts require updated pip and wheel packages or they will
        # refuse to install some packages (numpy, scipy, maybe others)
        $PIP wheel --find-links "${WHEEL_DIR}" \
                --wheel-dir "${WHEEL_DIR}" \
                pip wheel
        $PIP wheel --find-links "${WHEEL_DIR}" \
                --wheel-dir "${WHEEL_DIR}" \
                --requirement "${REQUIREMENTS_FROZEN}"
    fi
done

