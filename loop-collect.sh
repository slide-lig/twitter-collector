#!/bin/bash

THIS_DIR=$(cd "$(dirname "$0")"; pwd)

if [ "$1" = "step2" ]
then
    while [ 1 ]
    do
        $THIS_DIR/collector.py
        sleep 3
    done
else
    cd $THIS_DIR
    screen -S collector -d -m ./loop-collect.sh step2
fi

