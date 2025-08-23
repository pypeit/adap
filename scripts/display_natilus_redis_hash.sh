#!/usr/bin/env bash

REDIS_POD="adap-workqueue"

if [[ $(type -t kubectl) == "" ]]
then
    echo "The 'kubectl' command is not installed."
    exit 1
fi


# Get hash name
hash=$1
if [[ -z $hash ]]
then
    echo "You must specify a hash"
fi


# Get pod name of redis server
output=(`kubectl -n pypeit get pods | grep $REDIS_POD`)
redis_pod=${output[0]}

if [[ -z $redis_pod ]] 
then
    
    exit 1
fi

# Get the keys for the hash
hash_keys=`kubectl -n pypeit exec $redis_pod -- redis-cli hkeys $hash`

for hash_key in $hash_keys
do
    hash_value=`kubectl -n pypeit exec $redis_pod -- redis-cli hget $hash $hash_key`
    printf "%16s %s\n" $hash_key $hash_value
done


