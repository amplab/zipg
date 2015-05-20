#!/bin/bash
PFPREV=-1
pid=$1
echo "Measuring Page Faults..."
while [ 1 == 1 ] ; do
        PF=`ps -o maj_flt $pid | awk '{ if(NR == 2) print $1 }'`
        if [ $PFPREV -ne -1 ] ; then
                let PFPS=$PF-$PFPREV
                echo "Page Faults: $PFPS /s"
        fi
        PFPREV=$PF
        sleep 1
done
