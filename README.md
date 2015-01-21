Big Data on Heterogeneous Systems with GPUs - Source Code

This repository contains the code for:
- Hadoop (1.2.1) with C++/CUDA for lazy processing of <key,value> pairs
- scripts to launch Hadoop Pipes CUDA programs

The code is under the following folders:
- hadoop-1.2.1/src/examples/pipes/blackscholes
- hadoop-1.2.1/src/examples/pipes/kmeans
- hadoop-1.2.1/src/examples/pipes/grep
- hadoop-1.2.1/src/examples/pipes/common

For more details, visit:
http://www.comp.nus.edu.sg/~dumitrel/bdgpu.html.

CODE LICENSE

The code is licensed under the Apache License, Version 2.0 
(see APACHE-LICENSE-2.0.txt or http://www.apache.org/licenses/LICENSE-2.0), 
with the exception of: 
- parts of blackscholes are taken from PARSEC 3.0 and licensed under PARSEC Framework License 
(see PARSEC-LICENSE.txt) 
- parts of kmeans code are adapted from Mars and licensed to HKUST 
(see MARS-LICENSE.txt)
