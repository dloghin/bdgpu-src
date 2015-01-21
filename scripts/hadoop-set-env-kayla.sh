#!/bin/bash
#
# Set common variables for Hadoop benchmarks on Kayla Tegra 3
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

# Hadoop
export HUSER="hadoop"
export HDFSDIR="/mnt/ssd/hadoop/tmp"
export HDIR="/mnt/ssd/hadoop/nvidia-poster/hadoop-1.2.1"
export HBIN="$HDIR/bin/hadoop"
export HJAR="$HDIR/hadoop-examples-1.2.1.jar"
export HTESTJAR="$HDIR/hadoop-test-1.2.1.jar"
export HDFSDIR="/mnt/ssd/hadoop/tmp"
export DATADIR="/mnt/ssd/hadoop/Datasets"

# CUDA (6.0) on Kayla Tegra 3
export CUDA_LIBS="/usr/local/cuda-6.0/lib/libcudart.so,/usr/local/cuda-6.0/lib/libcudart.so.6.0,/usr/local/cuda-6.0/lib/libcudart.so.6.0.26"

# Java
export JAVADIR="/mnt/ssd/hadoop/jdk1.8.0"

# Cluster
export HOSTS="myhost"
export DOMAIN=".mydomain"
export HNODES=`echo $HOSTS | wc -w`
export HSLOTS=4

# ! If HMASTER is defined, launch scripts will launch mapred task from remote
export HMASTER="mymaster"

# Others
export CTIME="/usr/bin/time"
export DSTAT_CMD="dstat -t -c -C 0,1,2,3 -d --disk-tps -m -n --integer --noheader --nocolor"
export READ_POWER_APP="rdserial"
export READ_POWER_CMD="rdserial /dev/ttyS0"
