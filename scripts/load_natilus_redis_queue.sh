#!/usr/bin/env bash

REDIS_POD="adap-workqueue"

if [[ $(type -t kubectl) == "" ]]
then
    echo "The 'kubectl' command is not installed."
    exit 1
fi

# Get queue name
queue=$1
if [[ -z $queue ]]
then
    echo "You must specify a queue"
fi

shift

declare -a entries

if [[ $# > 0 ]]
then
    entries=$*
else
    readarray entries
fi


# Get pod name of redis server
output=(`kubectl -n pypeit get pods | grep $REDIS_POD`)
redis_pod=${output[0]}

if [[ -z $redis_pod ]] 
then
    
    exit 1
fi

for entry in ${entries[*]}
do
    echo Loading $entry
    kubectl -n pypeit exec $redis_pod -- redis-cli lpush $queue $entry
done


