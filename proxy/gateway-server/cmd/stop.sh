#!/bin/sh

PIDFILE="./startup.pid"
echo $PIDFILE
if [ ! -f "$PIDFILE" ]
then
    echo "no gateway-server to stop (could not find file $PIDFILE)"
else
    kill -9 $(cat "$PIDFILE")
    rm -f "$PIDFILE"
    echo STOPPED
fi