#!/bin/bash

for rel_path in $1/complete/reduce*
do
   s3_path="s3://pypeit/adap/raw_data_reorg/$rel_path/"
   echo Uploading $rel_path/ to $s3_path
   aws --endpoint http://rook-ceph-rgw-nautiluss3.rook s3 cp --no-progress --recursive $rel_path/ $s3_path
done
