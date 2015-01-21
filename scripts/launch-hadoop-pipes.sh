#!/bin/bash
# 
# Script to start Hadoop Pipes (CUDA) benchmarks
#
# Copyright (c) 2014 Dumi Loghin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -x

# supported benchmarks
BENCH_LIST="km bs gr"
TARGET_LIST="cpu gpu"
SIZE_LIST="s m l"

# Get benchmark argument
BENCH=""
if [ $# -lt 3 ]; then
	echo "Usage: $0 <benchmark> <target> <size> [logdir] [opt]"
	echo "Supported benchmarks: $BENCH_LIST"
	echo "Supported input sizes: $SIZE_LIST"
	exit 1
else
	BENCH=$1
	TARGET=$2
	SIZE=$3
fi

timestamp=`date +%F-%H-%M-%S`
tmpdir="hadoop/logtmp/hadoop-$BENCH-$TARGET-$SIZE-$timestamp"

# Set logs directory
if [ $# -gt 3 ]; then
	logdir=$4
else
	logdir="logs/hadoop-$BENCH-$TARGET-$SIZE-$timestamp"
fi

# Set global variables
# ! Replace with hadoop-set-env-[i7|kayla].sh or use symlink.
. ./hadoop-set-env.sh

# Create directories for dstat logs
mkdir -p $logdir
for hst in $HOSTS ; do
    ssh $hst$DOMAIN  "mkdir -p $tmpdir" & 
done
wait

# *** job preparation
INDIR="$BENCH-in-$SIZE"
OUTDIR="$BENCH-out-$SIZE"
EXE="/user/$HUSER/bin/$BENCH-$TARGET"
LIBS=$CUDA_LIBS

case $BENCH in
# Kmeans 
	"km")
	NMAP=$(($HNODES * $HSLOTS))
	NRED=1
	CENTROIDS="/home/$HUSER/mars_kmeans_centroids.txt"
	EXTRA_FLAGS="-D hadoop.kmeans.centroidsfile=$CENTROIDS"
	;;

# BlackScholes
	"bs")
	NMAP=$(($HNODES * $HSLOTS))
	NRED=1
	;;

# Word Count
	"wc")
	NMAP=$(($HNODES * $HSLOTS))
	NRED=$(($HNODES * $HSLOTS))
	;;

# Grep
	"gr")
	NMAP=$(($HNODES * $HSLOTS))
	NRED=1
	REGEX="the"
	EXTRA_FLAGS="-D mapred.mapper.regex=$REGEX"
	;;

	*)
	echo "Unsupported benchmark $BENCH"
	exit
	;;
esac

# Remove existing output folder
if [ -z $HMASTER ]; then
	$HBIN dfs -rmr $OUTDIR
else
	ssh $HMASTER$DOMAIN "$HBIN dfs -rmr $OUTDIR"
fi

sleep 5

# launch dstat
for hst in $HOSTS ; do
    ssh $hst$DOMAIN "$DSTAT_CMD --output $tmpdir/dstat.$hst.log > /dev/null" &
done

# measure power
if ! [ -z "$READ_POWER_CMD" ]; then
	$READ_POWER_CMD | tee -a "$logdir/power.log" > /dev/null &
fi

# *** job execution
HCMD="$CTIME $HBIN pipes -D mapred.map.tasks=$NMAP -D mapred.reduce.tasks=$NRED -D hadoop.pipes.java.recordreader=true -D hadoop.pipes.java.recordwriter=true $EXTRA_FLAGS -files $LIBS -program $EXE -input $INDIR -output $OUTDIR"
if [ -z $HMASTER ]; then
	$HCMD 2>&1 | tee $logdir/master-out.log
else
	ssh $HMASTER$DOMAIN "$HCMD 2>&1 | tee $tmpdir/master-out.log"
fi

sync

# kill power measurement
if ! [ -z "$READ_POWER_APP" ]; then
	killall -SIGINT $READ_POWER_APP
fi

# kill remote dstats
for hst in $HOSTS ; do
	ssh $hst$DOMAIN "killall -9 dstat" 
	ssh $hst$DOMAIN "killall -9 python"
done

# get output
if [ -z $HMASTER ]; then
	$HBIN dfs -copyToLocal $OUTDIR $logdir/output
else
	ssh $HMASTER$DOMAIN "$HBIN dfs -copyToLocal $OUTDIR $tmpdir/output"
	scp -r $HMASTER$DOMAIN:$tmpdir/output $logdir/
fi

# get master log
if ! [ -z $HMASTER ]; then
	scp $HMASTER$DOMAIN:$tmpdir/master-out.log $logdir/
fi

# get user logs
hjob=`cat $logdir/master-out.log | grep "Running job:" | cut -d ' ' -f 7`
if ! [ -z $hjob ]; then
	for hst in $HOSTS ; do
		scp -r $hst$DOMAIN:$HDFSDIR/mapred/local/userlogs/$hjob $logdir/$hjob-$hst
	done
fi 

for hst in $HOSTS ; do
    scp $hst$DOMAIN:$tmpdir/dstat.$hst.log $logdir/
    scp $hst$DOMAIN:$HDIR/logs/*-tasktracker-*log $logdir/
    scp $hst$DOMAIN:$HDIR/logs/*-jobtracker-*log $logdir/
    scp $hst$DOMAIN:$HDIR/logs/*-datanode-*log $logdir/
    scp $hst$DOMAIN:$HDIR/logs/*-namenode-*log $logdir/
    scp $hst$DOMAIN:$HDIR/logs/*-secondarynamenode-*log $logdir/
    scp $hst$DOMAIN:$HDIR/conf/mapred-site.xml $logdir/mapred-site.xml.$hst
    scp $hst$DOMAIN:$HDIR/conf/hadoop-env.sh $logdir/hadoop-env.sh.$hst
done

# remove remote tmpdir
for hst in $HOSTS ; do
    ssh $hst$DOMAIN  "rm -rf $tmpdir"
done

# copy config script
cp hadoop-set-env.sh $logdir/

echo "LOGS $logdir"
