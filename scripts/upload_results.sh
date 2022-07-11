#!/bin/bash

rel_path = "$1/complete/reduce/"
s3_path = "s3://pypeit/adap/raw_data_reorg/$rel_path/"

aws --endpoint http://rook-ceph-rgw-nautiluss3.rook s3 cp --no-progress --recursive $rel_path $s3_path
