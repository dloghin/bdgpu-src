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
 * Copyright (c) 2007 Intel Corp.
 *
 * Black-Scholes
 * Analytical method for calculating European Options
 * 
 * Reference Source: Options, Futures, and Other Derivatives, 3rd Edition, 
 * Prentice Hall, John C. Hull,
 *
 * Parts of the code are licesed under PARSEC 3.0 License.
 */

#include "hadoop/Pipes.hh"
#include "hadoop/TemplateFactory.hh"
#include "hadoop/StringUtils.hh"
#include "common.hu"
#include <stdio.h>

using namespace std;

// #define F_GPU_DEBUG

// #define F_LOCAL_DEBUG

#define F_PROFILE

#define F_GPU_DEBUG

#ifndef MaxCudaBlocks
#define MaxCudaBlocks		16
#endif

#ifndef MaxCudaThreadsBlock
#define MaxCudaThreadsBlock 	1024
#endif

#define MaxCudaThreads		(MaxCudaBlocks * MaxCudaThreadsBlock)

#define MaxLine			128

#define MaxKey			16

#define CounterGroup 		"BSCounters"

#define EmitCounter 		"EMITS"

#define NUM_RUNS 		100

typedef float fptype;

//
// Taken from Parsec 3.0 Blackscholes App - blackscholes.c
//
// Cumulative Normal Distribution Function
// See Hull, Section 11.8, P.243-244
#define inv_sqrt_2xPI 0.39894228040143270286

__device__ fptype CNDF ( fptype InputX )
{
	int sign;

	fptype OutputX;
	fptype xInput;
	fptype xNPrimeofX;
	fptype expValues;
	fptype xK2;
	fptype xK2_2, xK2_3;
	fptype xK2_4, xK2_5;
	fptype xLocal, xLocal_1;
	fptype xLocal_2, xLocal_3;

	// Check for negative value of InputX
	if (InputX < 0.0) {
		InputX = -InputX;
		sign = 1;
	} else
		sign = 0;

	xInput = InputX;

	// Compute NPrimeX term common to both four & six decimal accuracy calcs
	expValues = exp(-0.5f * InputX * InputX);
	xNPrimeofX = expValues;
	xNPrimeofX = xNPrimeofX * inv_sqrt_2xPI;

	xK2 = 0.2316419 * xInput;
	xK2 = 1.0 + xK2;
	xK2 = 1.0 / xK2;
	xK2_2 = xK2 * xK2;
	xK2_3 = xK2_2 * xK2;
	xK2_4 = xK2_3 * xK2;
	xK2_5 = xK2_4 * xK2;

	xLocal_1 = xK2 * 0.319381530;
	xLocal_2 = xK2_2 * (-0.356563782);
	xLocal_3 = xK2_3 * 1.781477937;
	xLocal_2 = xLocal_2 + xLocal_3;
	xLocal_3 = xK2_4 * (-1.821255978);
	xLocal_2 = xLocal_2 + xLocal_3;
	xLocal_3 = xK2_5 * 1.330274429;
	xLocal_2 = xLocal_2 + xLocal_3;

	xLocal_1 = xLocal_2 + xLocal_1;
	xLocal = xLocal_1 * xNPrimeofX;
	xLocal = 1.0 - xLocal;

	OutputX = xLocal;

	if (sign) {
		OutputX = 1.0 - OutputX;
	}

	return OutputX;
}

__device__ fptype BlkSchlsEqEuroNoDiv( fptype sptprice,
		fptype strike, fptype rate, fptype volatility,
		fptype time, int otype, float timet )
{
	fptype OptionPrice;

	// local private working variables for the calculation
	fptype xStockPrice;
	fptype xStrikePrice;
	fptype xRiskFreeRate;
	fptype xVolatility;
	fptype xTime;
	fptype xSqrtTime;

	fptype logValues;
	fptype xLogTerm;
	fptype xD1;
	fptype xD2;
	fptype xPowerTerm;
	fptype xDen;
	fptype d1;
	fptype d2;
	fptype FutureValueX;
	fptype NofXd1;
	fptype NofXd2;
	fptype NegNofXd1;
	fptype NegNofXd2;

	xStockPrice = sptprice;
	xStrikePrice = strike;
	xRiskFreeRate = rate;
	xVolatility = volatility;

	xTime = time;
	xSqrtTime = sqrt(xTime);

	logValues = log( sptprice / strike );

	xLogTerm = logValues;


	xPowerTerm = xVolatility * xVolatility;
	xPowerTerm = xPowerTerm * 0.5;

	xD1 = xRiskFreeRate + xPowerTerm;
	xD1 = xD1 * xTime;
	xD1 = xD1 + xLogTerm;

	xDen = xVolatility * xSqrtTime;
	xD1 = xD1 / xDen;
	xD2 = xD1 - xDen;

	d1 = xD1;
	d2 = xD2;

	NofXd1 = CNDF( d1 );
	NofXd2 = CNDF( d2 );

	FutureValueX = strike * ( exp( -(rate)*(time) ) );
	if (otype == 0) {
		OptionPrice = (sptprice * NofXd1) - (FutureValueX * NofXd2);
	} else {
		NegNofXd1 = (1.0 - NofXd1);
		NegNofXd2 = (1.0 - NofXd2);
		OptionPrice = (FutureValueX * NegNofXd2) - (sptprice * NegNofXd1);
	}

	return OptionPrice;
}

__device__ inline void str_to_fp(char* str, fptype* z) {
	char* p = str;
	fptype sgn = 1.0;
	int64_t n = 0, decs = 1;
	*z = 0.0;

	if (p == NULL)
		return;

	if (*p == '-') {
		sgn = -1.0;
		p++;
	}

	while ((*p >= '0') && (*p <= '9')) {
		n = 10 * n + (*p - '0');
		p++;
	}
	// suppose that '.' follows
	p++;
	while ((*p >= '0') && (*p <= '9')) {
		n = 10 * n + (*p - '0');
		p++;
		decs *= 10;
	}
	*z = sgn * (fptype)n / (fptype)decs;
}

__device__ inline char* next_tok(char** pstr, char delim) {
	char* p = *pstr;
	char* word = NULL;

	if (p == NULL)
		return NULL;

	if (*p == '\0')
		return NULL;

	while (*p == delim) p++;
	word = p;
	while (*p != delim && *p != '\0') p++;
	if (*p == '\0') {
		*pstr = NULL;
	}
	else {
		*p = '\0';
		p++;
		*pstr = p;
	}
	return word;
}

__global__ void cuda_map(int threads, char* lines, char* keys, fptype* prices) {
	// input
	char *key, *line, *tok;
	fptype spotPrice, strike, rate, volatility, time;
	int otype;
	// output
	fptype price;

	int k;

	int tid = blockIdx.x * blockDim.x + threadIdx.x;

	if (tid >= threads)
		return;

	line = lines + tid * MaxLine;
	key = keys + tid * MaxKey;

	// copy the key
	tok = next_tok(&line, ' ');
	for (; *tok != '\0'; tok++, key++) {
		*key = *tok;
	}
	*key = '\0';

	// parse line
	tok = next_tok(&line, ' ');
	str_to_fp(tok, &spotPrice);
	tok = next_tok(&line, ' ');
	str_to_fp(tok, &strike);
	tok = next_tok(&line, ' ');
	str_to_fp(tok, &rate);
	tok = next_tok(&line, ' ');
	tok = next_tok(&line, ' ');
	str_to_fp(tok, &volatility);
	tok = next_tok(&line, ' ');
	str_to_fp(tok, &time);
	tok = next_tok(&line, ' ');
	otype = (tok[0] == 'P') ? 1 : 0;

	for (k=0; k<NUM_RUNS; k++) {
		price = BlkSchlsEqEuroNoDiv(spotPrice, strike, rate, volatility, time, otype, 0);
	}

	prices[tid] = price;
}

class BlackScholesMapper : public HadoopPipes::Mapper {
public:
	HadoopPipes::TaskContext* mapContext;

	/* CUDA vars */
	int cthreads, ccurr;	// CUDA threads, current CUDA thread

	/* CUDA device pointers */
	char* c_lines;		// lines
	char* c_keys;		// key results
	fptype* c_res;		// price results

	/* Host pointers */
	char* h_lines;
	char* h_keys;
	fptype* h_res;

#ifdef F_PROFILE
	double tKernel;
#endif

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

		// keys
		src_line = __LINE__;
		rc = cudaMalloc((void **)&c_keys, cthreads * MaxKey * sizeof(char));
		if (rc != cudaSuccess)
			goto lbl_init_error;

		// keys buffer on host
		src_line = __LINE__;
		rc = myHostAlloc((void**)&h_keys, cthreads * MaxKey * sizeof(char));
		if (rc != cudaSuccess)
			goto lbl_init_error;

		// results
		src_line = __LINE__;
		rc = cudaMalloc((void **)&c_res, cthreads * sizeof(fptype));
		if (rc != cudaSuccess)
			goto lbl_init_error;

		// results on host
		src_line = __LINE__;
		rc = myHostAlloc((void**)&h_res, cthreads * sizeof(fptype));
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

		// keys
		src_line = __LINE__;
		rc = cudaMallocManaged((void **)&c_keys, cthreads * MaxKey * sizeof(char));
		if (rc != cudaSuccess)
			goto lbl_init_unified_error;

		// results
		src_line = __LINE__;
		rc = cudaMallocManaged((void **)&c_res, cthreads * sizeof(fptype));
		if (rc != cudaSuccess)
			goto lbl_init_unified_error;

		h_lines = h_keys = NULL;
		h_res = NULL;

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
		cudaFree(c_keys);
		cudaFree(c_res);
		myHostFree(h_lines);
		myHostFree(h_keys);
		myHostFree(h_res);
	}

	/**
	 * Launch CUDA kernel.
	 */
	cudaError_t launchCuda(HadoopPipes::TaskContext* context, int threads) {
		double t0, t1;
		int i, src_line;
		cudaError_t rc;
		char result[16];

		HadoopPipes::TaskContext::Counter* counter = context->getCounter(CounterGroup, EmitCounter);

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
		cuda_map<<<MaxCudaBlocks, MaxCudaThreadsBlock>>>(threads, c_lines, c_keys, c_res);
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
		rc = cudaMemcpy((void*)h_keys, c_keys, threads * MaxKey * sizeof(char), cudaMemcpyDeviceToHost);
		if (rc != cudaSuccess)
			goto lbl_launch_err;
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
			sprintf(result, "%.2lf", h_res[i]);
			// sprintf(result, "%X", *(int*)&h_res[i]);
			context->emit(string(h_keys + i * MaxKey), result);
			context->incrementCounter(counter, 1);
		}

		return cudaSuccess;

lbl_launch_err:
		cout << "Launch CUDA error " << cudaGetErrorString(cudaGetLastError()) << " at line " << src_line << endl;
		return rc;
	}

	/**
	 * Launch CUDA kernel.
	 */
	cudaError_t launchCudaUnified(HadoopPipes::TaskContext* context, int threads) {
		double t0, t1;
		int i, src_line;
		cudaError_t rc;
		char result[16];

		HadoopPipes::TaskContext::Counter* counter = context->getCounter(CounterGroup, EmitCounter);

#ifdef F_PROFILE
		t0 = get_time();
#endif
		cuda_map<<<MaxCudaBlocks, MaxCudaThreadsBlock>>>(threads, c_lines, c_keys, c_res);
		rc = cudaDeviceSynchronize();
		if (rc != cudaSuccess)
                        goto lbl_launch_unified_err;
#ifdef F_PROFILE
		t1 = get_time();
		// cout << "CUDA kernel <" << threads << "> : " << cudaGetErrorString(cudaGetLastError()) << " took " << (t1-t0) << " seconds" << endl;
		tKernel += (t1-t0);
#endif

		// emit the results
		for (i = 0; i < threads; i++) {
			sprintf(result, "%.2lf", c_res[i]);
			// sprintf(result, "%X", *(int*)&h_res[i]);
			context->emit(string(c_keys + i * MaxKey), result);
			context->incrementCounter(counter, 1);
		}

		return cudaSuccess;

lbl_launch_unified_err:
		cout << "Launch Unified Mem CUDA error " << cudaGetErrorString(cudaGetLastError()) << " at line " << src_line << endl;
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
		memcpy(c_lines + (ccurr * MaxLine), (char*)line.c_str(), len);
#else
		memcpy(h_lines + (ccurr * MaxLine), (char*)line.c_str(), len);
#endif

		// if the threshold is reached, launch CUDA kernel
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
	 * On close(), launch the remaining CUDA threads.
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

	BlackScholesMapper(HadoopPipes::TaskContext& context) {
		mapContext = &context;
#ifdef F_UNIFIED_MEM
		initializeCudaUnified();
#else
		initializeCuda();
#endif
#ifdef F_PROFILE
		tKernel = 0.0;
#endif
	}

	~BlackScholesMapper() {
#ifdef F_GPU_DEBUG
		cout << "Cleaning map context" << endl;
#endif
#ifdef F_PROFILE
		cout << "CUDA kernel total time " << tKernel << endl;
#endif
		finishCuda();
	}
};

class BlackScholesReducer : public HadoopPipes::Reducer {
public:

	BlackScholesReducer(HadoopPipes::TaskContext& context) {}

	// reduce function
	void reduce( HadoopPipes::ReduceContext& context ) {
		while (context.nextValue()) {
			context.emit(context.getInputKey(), context.getInputValue());
		}
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
#ifdef F_UNIFIED_MEM
	cout << "Using Unified Memory" << endl;
#endif
#ifdef F_PROFILE
	cout << "Starting BlackScholes Pipes CUDA with " << MaxCudaBlocks << " blocks and " << MaxCudaThreadsBlock << " threads per block" << endl;
	double t0 = get_time();
#endif
	int res = HadoopPipes::runTask(HadoopPipes::TemplateFactory<BlackScholesMapper,BlackScholesReducer>());
#ifdef F_PROFILE
	double t1 = get_time();
	cout << "BlackScholes Pipes CUDA finished in " << (t1 - t0) <<  " seconds." << endl;
#endif
	return res;
}
