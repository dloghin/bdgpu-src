#
# Copyright (c) 2013-2014 Dumi Loghin
#
UNAME := $(shell uname)
ARCH := $(shell uname -m)
HOST := $(shell hostname)

CUDA_DIR=/usr/local/cuda

HADOOP_DIR=/home/hadoop/hadoop/nvidia-poster/hadoop-1.2.1
JAVA_DIR=/home/hadoop/hadoop/jdk1.8.0
CUDA_ARCH=sm_50
# on x86-64 is lib64
CUDA_LIB=lib

# Define libraries
# with HDFS
# LIBS=-ljvm -lhdfs -lhadooppipes -lhadooputils -lcrypto -lpthread -lrt
# w/o HDFS
LIBS=-lhadooppipes -lhadooputils -lcrypto -lpthread -lrt

#### Building on Linux for ARM or x86_64 ####
ifeq ($(UNAME), Linux)

### x86_64 ###
ifeq ($(ARCH), x86_64)
HPLATFORM="Linux-amd64-64"
CC=g++
CCOPT=-Wall -std=c++11 -O2 -g3 -m64 -I$(JAVA_DIR)/include -I$(HADOOP_DIR)/src/c++/install/include -I../common
LD=$(CC)
LDOPT=-O2 -g3 -m64 -L$(JAVA_DIR)/jre/lib/amd64/server -L$(HADOOP_DIR)/src/c++/install/lib $(LIBS)
OBJDMP=objdump
STRIP=strip
CUDA_LIB=lib64
endif

### ARM ###
ifeq ($(ARCH), armv7l) 
CC=g++
# CCOPT=-Wall -O3 -marm -mcpu=cortex-a9 -mtune=cortex-a9 -mfpu=neon -mfloat-abi=hard -ffast-math -fstrict-aliasing -I$(HADOOP_DIR)/src/c++/libhdfs -I$(HADOOP_DIR)/c++/$(HPLATFORM)/include
# CCOPT=-Wall -std=c++11 -O3 -marm -mcpu=cortex-a15 -mtune=cortex-a15 -mfpu=neon -mfloat-abi=hard -ffast-math -fstrict-aliasing -I$(JAVA_DIR)/include -I$(JAVA_DIR)/include/linux -I$(HADOOP_DIR)/src/c++/install/include
CCOPT=-Wall -O3 -marm -mcpu=cortex-a9 -mtune=cortex-a9 -mfpu=neon -mfloat-abi=hard -ffast-math -fstrict-aliasing -I$(HADOOP_DIR)/src/c++/install/include -I../common/
# CCOPT=-Wall -O3 -marm -mcpu=cortex-a9 -mtune=cortex-a9 -mfpu=vfpv3 -mfloat-abi=hard -fstrict-aliasing -I$(HADOOP_DIR)/src/c++/install/include -I../common/
LD=$(CC)
LDOPT= -O2 -g -mfloat-abi=hard -L$(JAVA_DIR)/jre/lib/arm/server -L$(HADOOP_DIR)/src/c++/install/lib $(LIBS)
OBJDMP=objdump
STRIP=strip
endif

#### i686 ####
ifeq ($(ARCH), i686)
HPLATFORM="Linux-i386-32"
CC=g++
CCOPT=-Wall -std=c++11 -O2 -g3 -m32 -I$(JAVA_DIR)/include -I$(HADOOP_DIR)/src/c++/libhdfs -I$(HADOOP_DIR)/c++/$(HPLATFORM)/include
LD=$(CC)
LDOPT=-O2 -g $(LIBS)
OBJDMP=objdump
STRIP=strip
endif

endif

NVCC=$(CUDA_DIR)/bin/nvcc -arch=$(CUDA_ARCH) -G -g 
NVCOPT=-I$(HADOOP_DIR)/src/c++/install/include -I../common $(XFLAGS)
LDCUDA=-L$(CUDA_DIR)/$(CUDA_LIB) -lcudart

all: $(PRG) $(PRG)-cu

%.o:	%.cpp
	$(CC) $(CCOPT) -c $^ -o $@

%.oc:	%.cu
	$(NVCC) $(NVCOPT) -c $^ -o $@

$(PRG): $(PRG).o ../common/common.o
	$(LD) $^ -o $@ $(LDOPT)
	cp $@ $(ALIAS)-cpu
#	cp $@ $@-$(ARCH)

$(PRG)-cu: $(PRG).oc ../common/common.o ../common/common.oc
	$(LD) $^ -o $@ $(LDOPT) $(LDCUDA)
	cp $@ $(ALIAS)-gpu
#	cp $@ $@-$(ARCH)

$(ALIAS)-cpu: $(PRG)

$(ALIAS)-gpu: $(PRG)-cu

clean:
	rm -rf *.o *.oc *.s $(PRG) $(PRG)-cu $(ALIAS)-* *-armv7l *-x86_64 *-i386 *-i686
