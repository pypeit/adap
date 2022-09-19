#!/bin/bash

cd $1

rel_path="$2/complete/raw/"
s3_path="s3://pypeit/adap/raw_data_reorg/$rel_path"

mkdir -p $rel_path
aws --endpoint $ENDPOINT_URL s3 cp --no-progress --recursive $s3_path $rel_path
