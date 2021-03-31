#!/bin/bash
#
# Start dump1090 first
# ./dump1090 --interactive --net
#################################

cmd="nc localhost 30002 | nc data.adsbhub.org 5001"
eval "${cmd}" &
child_pid=$!

while true; do
    if kill -0 $child_pid; then
        echo "State: connected (child pid $child_pid)"
    else
        echo "State: not connected"
        eval "${cmd}" &
        child_pid=$!
    fi

    sleep 10 
done
