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
 */

#include "hadoop/Pipes.hh"
#include "hadoop/TemplateFactory.hh"
#include "hadoop/StringUtils.hh"
#include "common.hu"

using namespace std;

#define F_GPU_DEBUG

// #define F_LOCAL_DEBUG

#define F_DEBUG

#define F_PROFILE

#ifndef MaxCudaBlocks
#define MaxCudaBlocks			16
#endif

#ifndef MaxCudaThreadsBlock
#define MaxCudaThreadsBlock 		1024
#endif

#define MaxCudaThreads			(MaxCudaBlocks * MaxCudaThreadsBlock)

#define MaxLine				8192

#define MaxRegex			8

#define GrepMapKey			"match"

#define GrepRegexKey			"mapred.mapper.regex"

#ifdef F_LOCAL_DEBUG
char* regex;
#endif

__global__ void cuda_map(int threads, char* lines, char* exp, int* res) {
	// char exp[MaxRegex];
	char *str, *p1, *p2;
	int found = 0;

	int tid = blockIdx.x * blockDim.x + threadIdx.x;

	if (tid >= threads)
		return;

	str = lines + tid * MaxLine;
	while (*str != '\0') {
		p1 = str;
		p2 = exp;
		found = 1;
		while (*p1 != '\0' && *p2 != '\0') {
			if (*p1 == *p2) {
				p1++;
				p2++;
			}
			else {
				found = 0;
				break;
			}
		}
		if (*p1 == '\0' && *p2 != '\0')
			found = 0;
		if (found)
			break;
		str++;
	}
	res[tid] = found;

//	res[tid] = (tid % 7 == 0) ? 1 : 0;
}

class GrepMapper : public HadoopPipes::Mapper {
public:
	HadoopPipes::TaskContext* mapContext;
	string exp;

	/* CUDA vars */
	int cthreads, ccurr;	// CUDA threads, current CUDA thread

	/* CUDA device pointers */
	char* c_lines;		// lines
	char* c_exp;		// regexp
	int* c_res;			// results

	/* Host pointers */
	char* h_lines;
	int* h_res;

	/**
	 * Initialize CUDA buffers.
	 */
	int initializeCuda() {
		// for error checking
		int src_line;
		cudaError_t rc;

#ifdef F_GPU_DEBUG
		print_info();
#endif

		cthreads = MaxCudaThreads;
		ccurr = 0;

		// lines
		src_line = __LINE__;
		rc = cudaMalloc((void **)&c_lines, cthreads * MaxLine * sizeof(char));
		if (rc != cudaSuccess)
			goto lbl_init_error;

		// lines buffer on host
		src_line = __LINE__;
		rc = myHostAlloc((void**)&h_lines, cthreads * MaxLine * sizeof(char));
		if (rc != cudaSuccess)
			goto lbl_init_error;

		// regexp
		src_line = __LINE__;
		rc = cudaMalloc((void **)&c_exp, MaxRegex * sizeof(char));
		if (rc != cudaSuccess)
			goto lbl_init_error;

		// results
		src_line = __LINE__;
		rc = cudaMalloc((void **)&c_res, cthreads * sizeof(int));
		if (rc != cudaSuccess)
			goto lbl_init_error;

		// results on host
		src_line = __LINE__;
		rc = myHostAlloc((void**)&h_res, cthreads * sizeof(int));
		if (rc != cudaSuccess)
			goto lbl_init_error;

		// copy the regexp
		src_line = __LINE__;
		rc = cudaMemcpy(c_exp, exp.c_str(), exp.size() + 1, cudaMemcpyHostToDevice);
		if (rc != cudaSuccess)
			goto lbl_init_error;

		return 0;

lbl_init_error:
		cout << "Map Initialize CUDA error |" << cudaGetErrorString(cudaGetLastError()) << "| at line " << src_line << endl;
		return -1;
	}

	/**
	 * Initialize CUDA buffers in Unified Memory.
	 */
	int initializeCudaUnified() {
		// for error checking
		int src_line;
		cudaError_t rc;

#ifdef F_GPU_DEBUG
		print_info();
#endif

		cthreads = MaxCudaThreads;
		ccurr = 0;

		// lines
		src_line = __LINE__;
		rc = cudaMallocManaged((void **)&c_lines, cthreads * MaxLine * sizeof(char));
		if (rc != cudaSuccess)
			goto lbl_init_unified_error;

		// regexp
		src_line = __LINE__;
		rc = cudaMallocManaged((void **)&c_exp, MaxRegex * sizeof(char));
		if (rc != cudaSuccess)
			goto lbl_init_unified_error;

		// results
		src_line = __LINE__;
		rc = cudaMallocManaged((void **)&c_res, cthreads * sizeof(int));
		if (rc != cudaSuccess)
			goto lbl_init_unified_error;

		// copy the regexp
		memcpy(c_exp, exp.c_str(), exp.size() + 1);

		return 0;

lbl_init_unified_error:
		cout << "Map Initialize Unified Mem CUDA error |" << cudaGetErrorString(cudaGetLastError()) << "| at line " << src_line << endl;
		return -1;
	}

	/**
	 * Destroy CUDA buffers.
	 */
	void finishCuda() {
		cudaFree(c_lines);
		cudaFree(c_exp);
		cudaFree(c_res);
		myHostFree(h_lines);
		myHostFree(h_res);
	}

	/**
	 * Launch CUDA kernel.
	 */
	cudaError_t launchCuda(HadoopPipes::TaskContext* context, int threads) {
		double t0, t1;
		int i, src_line;
		cudaError_t rc;

		// copy the data to GPGPU
		// TODO - copy all or just threads * size ?
#ifdef F_PROFILE
		t0 = get_time();
#endif
		src_line = __LINE__;
		rc = cudaMemcpy(c_lines, h_lines, threads * MaxLine * sizeof(char), cudaMemcpyHostToDevice);
		if (rc != cudaSuccess)
			goto lbl_launch_err;
#ifdef F_PROFILE
		t1 = get_time();
		cout << "CUDA copy data took " << (t1-t0) << " seconds" << endl;
#endif

#ifdef F_PROFILE
		t0 = get_time();
#endif
		cuda_map<<<MaxCudaBlocks, MaxCudaThreadsBlock>>>(threads, c_lines, c_exp, c_res);
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
		rc = cudaMemcpy((void*)h_res, c_res, threads * sizeof(int), cudaMemcpyDeviceToHost);
		if (rc != cudaSuccess)
			goto lbl_launch_err;
#ifdef F_PROFILE
		t1 = get_time();
		cout << "Map CUDA copy finished in " << (t1-t0) << " seconds" << endl;
#endif

		// emit the results
		for (i = 0; i < threads; i++) {
			if (h_res[i] == 1) {
				context->emit(exp, "1");
			}
		}

		return cudaSuccess;

lbl_launch_err:
		cout << "Launch CUDA error " << cudaGetErrorString(cudaGetLastError()) << " at line " << src_line << endl;
		return rc;
	}

	/**
	 * Launch CUDA kernel in Unified Memory.
	 */
	cudaError_t launchCudaUnified(HadoopPipes::TaskContext* context, int threads) {
		double t0, t1;
		int i, src_line;
		cudaError_t rc;

#ifdef F_PROFILE
		t0 = get_time();
#endif
		cuda_map<<<MaxCudaBlocks, MaxCudaThreadsBlock>>>(threads, c_lines, c_exp, c_res);
		cudaDeviceSynchronize();
#ifdef F_PROFILE
		t1 = get_time();
		cout << "CUDA kernel <" << threads << "> : " << cudaGetErrorString(cudaGetLastError()) << " took " << (t1-t0) << " seconds" << endl;
#endif

		// emit the results
		for (i = 0; i < threads; i++) {
			if (c_res[i] == 1) {
				context->emit(exp, "1");
			}
		}

		return cudaSuccess;

lbl_launch_unified_err:
		cout << "Launch Unified CUDA error " << cudaGetErrorString(cudaGetLastError()) << " at line " << src_line << endl;
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

		// buffer current line
		line = context.getInputValue();
		len = line.length();
#ifdef F_UNIFIED_MEM
		memcpy(c_lines + (ccurr * MaxLine), (char*)line.c_str(), len + 1);
#else
		memcpy(h_lines + (ccurr * MaxLine), (char*)line.c_str(), len + 1);
#endif

		// if we reached the threshold, we launch CUDA kernel
		ccurr++;
		if (ccurr < cthreads) {
			return;
		}
		ccurr = 0;

#ifdef F_UNIFIED_MEM
		rc = launchCudaUnified(mapContext, cthreads);
#else
		rc = launchCuda(mapContext, cthreads);
#endif
		if (rc != cudaSuccess)
			cout << "Map CUDA error |" << cudaGetErrorString(rc) << endl;
	}

	/**
	 * On close(), we launch the remaining CUDA threads.
	 */
	void close() {
		cudaError_t rc;
		if (ccurr <= 0)
			return;
#ifdef F_GPU_DEBUG
		cout << "Launching the last chunk with " << ccurr << endl;
#endif
#ifdef F_UNIFIED_MEM
		rc = launchCudaUnified(mapContext, ccurr);
#else
		rc = launchCuda(mapContext, ccurr);
#endif
		if (rc != cudaSuccess)
			cout << "Launch CUDA error " << cudaGetErrorString(rc) << endl;
	}

	GrepMapper( HadoopPipes::TaskContext& context ) {
		mapContext = &context;

#ifdef F_LOCAL_DEBUG
		exp = string(regex);
#else
		HadoopPipes::JobConf* conf = (HadoopPipes::JobConf*)context.getJobConf();
		if (conf != NULL) {
			exp = conf->get(GrepRegexKey);
#ifdef F_DEBUG
			cout << "Searching for |" << exp << "|" << endl;
#endif
		}
		else {
			exp = "";
#ifdef F_DEBUG
			cout << "No regex to search for!" << endl;
#endif
		}
#endif	/* F_LOCAL_DEBUG */
#ifdef F_UNIFIED_MEM
		initializeCudaUnified();
#else
		initializeCuda();
#endif
	}

	~GrepMapper() {
#ifdef F_GPU_DEBUG
		cout << "Cleaning map context" << endl;
#endif
		finishCuda();
	}
};

class GrepReducer : public HadoopPipes::Reducer {
public:

	GrepReducer(HadoopPipes::TaskContext& context) {
	}

	// reduce function
	void reduce( HadoopPipes::ReduceContext& context ) {
		string key = context.getInputKey();
		long cnt = 0;
		while (context.nextValue()) {
			// context.emit(key, context.getInputValue());
			cnt++;
		}
#ifdef F_LOCAL_DEBUG
		cout << "Reduce " << cnt << " " << key << endl;
#endif
		context.emit(HadoopUtils::toString(cnt), key);
	}
};

int main(int argc, char *argv[]) {
#ifdef F_LOCAL_DEBUG
	if (argc < 3) {
		cout << "Usage: " << argv[0] << " <input_file> <regex>!" << endl;
		return -1;
	}
#endif

	//start the map/reduce job
#ifdef F_PROFILE
	cout << "Starting Grep Pipes CUDA" << endl;
	double t0 = get_time();
#endif
	int res = HadoopPipes::runTask(HadoopPipes::TemplateFactory<GrepMapper,GrepReducer>());
#ifdef F_PROFILE
	double t1 = get_time();
	cout << "Grep Pipes CUDA finished in " << (t1 - t0) <<  " seconds." << endl;
#endif
	return res;
}
