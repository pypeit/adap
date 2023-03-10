#!/usr/bin/env bash


adap_remote_root="s3://pypeit/adap/raw_data_reorg"
nautilus_endpoint=$ENDPOINT_URL


if [[ $(type -t aws) == "" ]]
then
    echo "The 'aws' command is not installed. It can be installed with 'pip install awscli' or via your favorite packager (apt, yum, brew, snap, etc)."
    exit 2
fi

for dataset in $(cat backup_list.txt)
do
    adap_dataset_complete=${adap_remote_root}/${dataset}/complete
    local_dest=${dataset}/complete
    mkdir -p $local_dest
    echo ""
    echo Looking for reduce directories in $adap_dataset_complete

    results=($(aws --endpoint $nautilus_endpoint s3 ls ${adap_dataset_complete}/reduce))

    if [[ $? != 0 ]]
    then
        echo Failed to find reduce directories
        exit 2
    fi


    reduce_dirs=()
    for result in ${results[@]}
    do
        if [[ ${result:0:6} == "reduce" ]]
        then
            reduce_dirs+=($result)
        fi
    done

    for reduce_dir in ${reduce_dirs[@]}
    do
        echo ""
        echo Downloading ${dataset}/complete/${reduce_dir} to $local_dest
        aws --endpoint $nautilus_endpoint s3 cp --no-progress "${adap_dataset_complete}/${reduce_dir}" "${local_dest}/${reduce_dir}" --recursive
        echo ""
        echo Uploading ${dataset} to Google Drive
        rclone --config ./rclone.conf sync -v --stats-one-line --stats-unit bits "${local_dest}/${reduce_dir}" "gdrive:backups/${local_dest}/${reduce_dir}"
        echo ""
        echo Clearing local data
        rm -rf $local_dest
    done
done