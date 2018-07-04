#!/bin/sh

BASEPATH=$(cd `dirname $0`; pwd)
echo current path:$BASEPATH

PIDFILE="./startup.pid"
if [ -f $PIDFILE ]; then
    if kill -9 `cat $PIDFILE` > /dev/null 2>&1; then
        echo gateway-server already running as process `cat $PIDFILE`.
        exit 0
    fi
fi

#sh build.sh
nohup "$BASEPATH"/gateway-server -config config.toml \
  > /dev/null &

# ------ wirte pid to file
if [ $? -eq 0 ]
then
    if /bin/echo -n $! > "$PIDFILE"
    then
        sleep 1
        echo STARTED SUCCESS
    else
        echo FAILED TO WRITE PID
        exit 1
    fi
#    tail -100f logs/master_debug.log
else
    echo SERVER DID NOT START
    exit 1
fi
