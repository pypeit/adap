#!/usr/bin/env bash


adap_remote_root="s3://pypeit/adap/raw_data_reorg"
nautilus_endpoint="https://s3-west.nrp-nautilus.io"
logs=("keck_deimos_A/keck_deimos_A.log" "keck_deimos_A/keck_deimos_A_useful_warns.log" "keck_deimos_A/run_pypeit_stdout.txt" "reduce_from_queue.log")


usage() {
    echo ""
    echo "Usage: download_datasets.sh [-l|--logs] [-h|--help] <dataset> [<dest>]"
    echo "Downloads the ADAP results for <dataset> to <dest>"
    echo ""
    echo "If -l or --logs is given, only the logs will be downloaded"
    echo "If no destination is given, the current directory is used."
    echo ""
    echo "Examples:"
    echo "    # Download 041615B1/1200G_8800_OG550/2015-10-16 results to dest_dir"
    echo "    download_datasets.sh 041615B1/1200G_8800_OG550/2015-10-16 dest_dir"
    echo ""
    echo "    # Download 041615B1/1200G_8800_OG550/2015-10-16 logs to the current directory"
    echo "    download_datasets.sh -l 041615B1/1200G_8800_OG550/2015-10-16"
    echo ""

    exit 1
}


only_logs=false
dataset=""
dest=""
while [[ ! -z $1 ]]
do
    case "$1" in
        --help | -h)
            usage
            ;;
        --logs | -l)
            only_logs=true
            ;;
        *)
            if [[ -z $dataset ]]
            then
                dataset=$1
            elif [[ -z $dest ]]
            then
                dest=$1
            else
                echo Unexpected extra argument "$1"
                usage
            fi
            ;;
        esac
    shift
done

if [[ -z $dataset ]]
then
    echo Missing '<dataset>'
    usage
fi

if [[ -z $dest ]]
then
    dest=$PWD
fi



echo $only_logs $dataset $dest

if [[ $(type -t aws) == "" ]]
then
    echo "The 'aws' command is not installed. It can be installed with 'pip install awscli' or via your favorite packager (apt, yum, brew, snap, etc)."
    exit 2
fi

adap_dataset_complete=${adap_remote_root}/${dataset}/complete
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
        # reduce_old was used for backups, which was a mistake but oh well
        if [[ $result !=  "reduce_old/" ]]
        then
            reduce_dirs+=($result)
        fi
    fi
done

for reduce_dir in ${reduce_dirs[@]}
do
    echo ""
    if [[ $only_logs == true ]]
    then
        echo Downloading logs for ${dataset}/complete/${reduce_dir} to $dest
        for log in ${logs[@]}
        do            
            aws --endpoint $nautilus_endpoint s3 cp "${adap_remote_root}/${dataset}/complete/${reduce_dir}$log" "${dest}/${reduce_dir}${log##*/}"
        done
    else
        echo Downloading results for ${dataset}/complete/${reduce_dir} to $dest
        aws --endpoint $nautilus_endpoint s3 cp "${adap_remote_root}/${dataset}/complete/${reduce_dir}" "${dest}/${reduce_dir}" --recursive
    fi
done