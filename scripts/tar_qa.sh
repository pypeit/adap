#!/bin/bash

echo Making tarball of "$1/QA"
cd $1
if [[ $? != 0 ]]; then exit 1; fi

tar czf "QA.tar.gz" "QA"
if [[ $? != 0 ]]; then exit 2; fi

echo Removing "$1/QA"
rm -rf "QA"
if [[ $? != 0 ]]; then exit 3; fi
