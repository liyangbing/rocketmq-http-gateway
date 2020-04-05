#!/bin/sh
source /etc/profile

BASEDIR=`dirname $0`/..
BASEDIR=`(cd "$BASEDIR"; pwd)`
ROOTDIR=`basename $BASEDIR`

PIDPROC=`ps -ef | grep "$ROOTDIR/conf" | grep -v 'grep'| awk '{print $2}'`

if [ -z "$PIDPROC" ];then
 echo "$ROOTDIR is not running"
 exit 0
fi

echo "PIDPROC: "$PIDPROC
for PID in $PIDPROC
do
if kill -9 $PID
   then echo "process $ROOTDIR(Pid:$PID) was force stopped at " `date`
fi
done
echo stop finished.
