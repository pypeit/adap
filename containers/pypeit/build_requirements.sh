#!/usr/bin/sh
# Build a requirements.txt file for pypeit
# Args:
#    venv    -  The virtual environment to use
#    version -  The PypeIt version to use
#    output  -  The output requirements file to create.

venv=$1
version=$2
output=$3


. ${venv}/bin/activate

# Make sure pip is up to date
pip install --upgrade pip

# Purge any pip caches so that we get the latest dependency
pip cache purge

pip install pypeit[dev]==$version

pip freeze > $output

