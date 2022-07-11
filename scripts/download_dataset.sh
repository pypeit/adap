#!/bin/bash

rel_path = "$1/complete/"
s3_path = "s3://pypeit/adap/raw_data_reorg/$rel_path/"

mkdir -p $rel_path

aws --endpoint http://rook-ceph-rgw-nautiluss3.rook s3 cp --no-progress --recursive $s3_path $rel_path