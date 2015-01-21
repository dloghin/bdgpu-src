/**
 * Copyright 2014 Dumi Loghin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Parts of this code are adapted from Mars Kmeans code. Please see
 * MARS-LICENCE.txt under root folder.
 *
 *** 
 * Kmeans Hadoop Pipes with lazy evaluated CUDA map function.
 */

#include "hadoop/Pipes.hh"
#include "hadoop/TemplateFactory.hh"
#include "hadoop/StringUtils.hh"
#include "common.hu"
#include <stdio.h>
#include <sstream>

using namespace std;

#define FDEBUG

#define F_PROFILE

/*** CUDA ***/
#ifndef MaxCudaBlocks
#define MaxCudaBlocks			16
#endif

#ifndef MaxCudaThreadsBlock
#define MaxCudaThreadsBlock 		1024
#endif

#define MaxCudaThreads			(MaxCudaBlocks * MaxCudaThreadsBlock)

#define MaxRes 					MaxCudaThreads

#define MaxFeatures				34

#define MaxClusters 			16

#define MaxLine 				(4 * MaxFeatures)	// 4 chars per feature

#define MAXINT 					1073741824

typedef struct {
	int id;
	int dim;
	int pt[MaxFeatures];
} t_cluster;

/*** Hadoop ***/
#define StrModelKey 			"hadoop.kmeans.centroidsfile"

#define CounterGroup 			"KmeansCounters"
#define MapCounter 				"WORDS"
#define RedCounter 				"VALUES"
#define CudaCounter 			"CUDA"

enum Counter {WORDS, VALUES, CUDA};

/**
 * Parse a string containing one pint with <dim> features.
 * @return - point dimension or negative values in case of errors
 */
int parse_point(char* str, int len, int dim, int* point) {
	if (point == NULL)
		return -1;

	char* start = str;
	char* end = str + len;
	int val = 0;
	int n = 0;
	while (start < end) {
		val = 0;
		while (start < end && *start != ' ') {
			val = 10 * val + (*start - '0');
			start++;
		}
		start++;
		if (n >= dim)
			return -2;
		point[n] = val;
		n++;
	}

	if (n != dim)
		return -2;

	return dim;
}

string print_point(int* point, int dim) {
	int i;
	stringstream ss;
	for (i=0; i<dim; i++)
		ss << point[i] << " ";
	ss << endl;
	return ss.str();
}

/**
 * CUDA k-means map
 */
__global__ void cuda_map(int n_clusters, int dim, char* lines, int* lens, t_cluster* clusters, int* clusterId) {
	int pt[MaxFeatures];
	int minDist, curDist, delta, cid, i, k;

	int tid = blockIdx.x * blockDim.x + threadIdx.x;	
	// int tid = threadIdx.x;

	clusterId[tid] = -1;

	char* start = &lines[tid * MaxLine];
	char* end = start + lens[tid];
	int val = 0;
	int n = 0;
	while (start < end) {
		val = 0;
		while (start < end && *start != ' ') {
			val = 10 * val + (*start - '0');
			start++;
		}
		start++;
		if (n >= dim)
			return;
		pt[n] = val;
		n++;
	}

	if (n != dim)
		return;

	minDist = MAXINT;
	cid = -1;
	for (k = 0; k < n_clusters; k++) {
		curDist = 0;
		for (i=0; i< dim; i++) {
			delta = pt[i] - clusters[k].pt[i];
			curDist += (delta * delta);
		}
		if (minDist > curDist) {
			cid = clusters[k].id;
			minDist = curDist;
		}
	}
	clusterId[tid] = cid;
};

class KmeansMapper : public HadoopPipes::Mapper {
public:
	HadoopPipes::TaskContext* mapContext;

	int n_clusters, dim;
	t_cluster clusters[MaxClusters];

	int cthreads, ccurr;	// CUDA threads, current CUDA thread

	/*** CUDA pointers ***/
	// line
	char* c_lines;
	int* c_lens;
	// clusters
	t_cluster* c_clusters;
	// results (cluster ids)
	int *c_cid;

	/*** Host pointers ***/
	// results on host
	int* h_cid;
	char* h_lines;
	int* h_lens;

	KmeansMapper(HadoopPipes::TaskContext& context) {
		mapContext = &context;

		HadoopPipes::JobConf* conf = (HadoopPipes::JobConf*)context.getJobConf();
		if (conf != NULL) {
			string filename = conf->get(StrModelKey);
			n_clusters = initializeCentroids((char*)filename.c_str());
		}
		else
			n_clusters = 0;

#ifdef FDEBUG
		cout << "Using " << n_clusters << " clusters!" << endl;
#endif

		initializeCuda();
	}

	/**
	 * Read cluster centroids from file.
	 */
	int initializeCentroids(char* filename) {
		int i, k;

		FILE* f = fopen(filename, "rt");
		if (!f)
			return -1;

		fscanf(f, "%d %d", &n_clusters, &dim);
		for (k=0; k<n_clusters; k++) {
			fscanf(f, "%d", &clusters[k].id);
			clusters[k].dim = dim;
			for (i=0; i<dim; i++)
				fscanf(f, "%d", &clusters[k].pt[i]);
		}
		fclose(f);

		return n_clusters;
	}

	/**
	 * Initialize CUDA buffers.
	 */
	int initializeCuda() {
		// for error checking
		int src_line;
		cudaError_t rc;

#ifdef FDEBUG
		print_info();
#endif

		cthreads = MaxCudaThreads;
		ccurr = 0;

		// lines
		src_line = __LINE__;
		rc = cudaMalloc((void **)&c_lines, cthreads * MaxLine * sizeof(char));
		if (rc != cudaSuccess)
			goto lbl_init_error;
		src_line = __LINE__;
		rc = cudaMalloc((void **)&c_lens, cthreads * sizeof(int));
		if (rc != cudaSuccess)
			goto lbl_init_error;

		// clusters
		src_line = __LINE__;
		rc = cudaMalloc((void **)&c_clusters, n_clusters * sizeof(t_cluster));
		if (rc != cudaSuccess)
			goto lbl_init_error;

		// results
		src_line = __LINE__;
		rc = cudaMalloc((void **)&c_cid, MaxRes * sizeof(int));
		if (rc != cudaSuccess)
			goto lbl_init_error;

		// results on host
		src_line = __LINE__;
		rc = myHostAlloc((void**)&h_cid, MaxRes * sizeof(int));
		if (rc != cudaSuccess)
			goto lbl_init_error;

		// lines buffer on host
		src_line = __LINE__;
		rc = myHostAlloc((void**)&h_lines, cthreads * MaxLine * sizeof(char));
		if (rc != cudaSuccess)
			goto lbl_init_error;
		src_line = __LINE__;
		rc = myHostAlloc((void**)&h_lens, cthreads * sizeof(int));
		if (rc != cudaSuccess)
			goto lbl_init_error;

		// store the clusters
		src_line = __LINE__;
		rc = cudaMemcpy(c_clusters, clusters, n_clusters * sizeof(t_cluster), cudaMemcpyHostToDevice);
		if (rc != cudaSuccess)
			goto lbl_init_error;

		return 0;

lbl_init_error:
		cout << "Map Initialize CUDA error |" << cudaGetErrorString(cudaGetLastError()) << "| at line " << src_line << endl;
		return -1;
	}

	/**
	 * Destroy CUDA buffers.
	 */
	void finishCuda() {
		cudaFree(c_lines);
		cudaFree(c_clusters);
		cudaFree(c_cid);
		myHostFree(h_cid);
		myHostFree(h_lines);
		myHostFree(h_lens);		
	}

	/**
	 * Launch CUDA kernel.
	 */
	cudaError_t launchCuda(HadoopPipes::TaskContext* context, int threads) {
		double t0, t1;		
		int i, src_line;
		cudaError_t rc;

		HadoopPipes::TaskContext::Counter* cudaCounter = context->getCounter(CounterGroup, CudaCounter);
		HadoopPipes::TaskContext::Counter* counter = context->getCounter(CounterGroup, MapCounter);
		context->incrementCounter(cudaCounter, 1);

		// copy the data to GPGPU
		// TODO - copy all or just threads * size ?
#ifdef F_PROFILE
		t0 = get_time();
#endif
		src_line = __LINE__;
		rc = cudaMemcpy(c_lines, h_lines, threads * MaxLine * sizeof(char), cudaMemcpyHostToDevice);
		if (rc != cudaSuccess)
			goto lbl_launch_err;
		src_line = __LINE__;
		rc = cudaMemcpy(c_lens, h_lens, threads * sizeof(int), cudaMemcpyHostToDevice);
		if (rc != cudaSuccess)
			goto lbl_launch_err;
#ifdef F_PROFILE
		t1 = get_time();
		cout << "CUDA copy data took " << (t1-t0) << " seconds" << endl;
#endif

		// launch
#ifdef FDEBUG	
		cout << "CUDA launch " << n_clusters << endl;
#endif
#ifdef F_PROFILE
		t0 = get_time();
#endif
		cuda_map<<<MaxCudaBlocks, MaxCudaThreadsBlock>>>(n_clusters, dim, c_lines, c_lens, c_clusters, c_cid);
		cudaDeviceSynchronize();
#ifdef F_PROFILE
		t1 = get_time();
		cout << "CUDA kernel <" << threads << "> : " << cudaGetErrorString(cudaGetLastError()) << " took " << (t1-t0) << " seconds" << endl;
#endif
		// get the results from GPGPU
#ifdef F_PROFILE
		t0 = get_time();
#endif
		src_line = __LINE__;			
		rc = cudaMemcpy((void*)h_cid, c_cid, MaxRes * sizeof(int), cudaMemcpyDeviceToHost);
		if (rc != cudaSuccess)
			goto lbl_launch_err;
#ifdef F_PROFILE
		t1 = get_time();
		cout << "Map CUDA copy finished in " << (t1-t0) << " seconds" << endl;
#endif

		// emit the results
		for (i = 0; i < threads; i++) {
			if (h_cid[i] != -1) {
				context->emit(HadoopUtils::toString(h_cid[i]), string(h_lines + (i * MaxLine)));
				context->incrementCounter(counter, 1);
			}
#ifdef FDEBUG
			else
				cout << "Error cluster id outsde bounds " << h_cid[i] << endl;
#endif
		}

		return cudaSuccess;

lbl_launch_err:
		cout << "Launch CUDA error " << cudaGetErrorString(cudaGetLastError()) << " at line " << src_line << endl;
		return rc;
	}

	/**
	 * Pipes map().
	 */
	void map(HadoopPipes::MapContext& context) {
		// for error checking
		cudaError_t rc = cudaSuccess;
		int len;
		string line;

		mapContext = &context;

		line = context.getInputValue();
		len = line.length();
		memcpy(h_lines + (ccurr * MaxLine), (char*)line.c_str(), len);
		memcpy(h_lens + ccurr, &len, sizeof(int));

		// if threshold is reached, launch CUDA kernel
		ccurr++;
		if (ccurr < cthreads) {
			return;
		}
		ccurr = 0;

		rc = launchCuda(mapContext, cthreads);
		if (rc != cudaSuccess)
			cout << "Map CUDA error |" << cudaGetErrorString(rc) << endl;
	}

	/**
	 * On close(), launch the remaining CUDA threads.
	 */
	void close() {
		cudaError_t rc;
		if (ccurr <= 0)
			return;
#ifdef FDEBUG
		cout << "Launching the last chunk with " << ccurr << endl;
#endif
		rc = launchCuda(mapContext, ccurr);
		if (rc != cudaSuccess)
			cout << "Launch CUDA error " << cudaGetErrorString(rc) << endl;
	}

	~KmeansMapper() {
#ifdef FDEBUG
		cout << "Cleaning map context" << endl;
#endif
		finishCuda();
	}	
};

class KmeansReducer : public HadoopPipes::Reducer {
public:
	int dim;	// no of features

	KmeansReducer(HadoopPipes::TaskContext& context) {
		// initialize centroids
		HadoopPipes::JobConf* conf = (HadoopPipes::JobConf*)context.getJobConf();
		if (conf != NULL) {
			string s = conf->get(StrModelKey);
			dim = initializeDim(s);
		}
		else
			dim = 0;
#ifdef FDEBUG
		cout << "Reduce using dim: " << dim << endl;
#endif
	}

	int initializeDim(string filename) {
		int dim, k;

		FILE* f = fopen(filename.c_str(), "rt");
		if (!f)
			return -1;

		fscanf(f, "%d %d", &k, &dim);
		fclose(f);
		return dim;
	}

	void reduce(HadoopPipes::ReduceContext& context) {
		string key, val, retval;
		int clusters[MaxFeatures] = {0};
		int pt[MaxFeatures];
		int valCount = 0;

		HadoopPipes::TaskContext::Counter* counter = context.getCounter(CounterGroup, RedCounter);

		key = context.getInputKey();

		// iterate through all the points
		while (context.nextValue()) {
			val = context.getInputValue();
			valCount++;
			parse_point((char*)val.c_str(), val.length(), dim, pt);
			for (int j = 0; j < dim; j++)
				clusters[j] += pt[j];
		}

		for (int i = 0; i < dim; i++)
			clusters[i] /= (int)valCount;

#ifdef FDEBUG
		retval = print_point(clusters, dim);
#endif

		context.emit(key, retval);
		context.incrementCounter(counter, 1);
	}
};

int main(int argc, char** argv) {
#ifdef F_LOCAL_DEBUG
	if (argc < 3) {
		cout << "kmeans <in_centroids> <input> <output>" << endl;
	}
#endif

	//start a map/reduce job
#ifdef F_PROFILE
	cout << "Starting Kmeans Pipes CUDA" << endl;
	double t0 = get_time();
#endif
	int res = HadoopPipes::runTask(HadoopPipes::TemplateFactory<KmeansMapper, KmeansReducer>());
#ifdef F_PROFILE
	double t1 = get_time();
	cout << "Kmeans Pipes CUDA finished in " << (t1 - t0) <<  " seconds." << endl;
#endif
	return res;
}
