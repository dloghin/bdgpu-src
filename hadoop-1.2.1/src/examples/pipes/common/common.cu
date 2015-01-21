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

#include "common.hu"

using namespace std;

cudaError_t myHostAlloc(void** ptr, size_t size) {
// 1. CUDA pinned memmory
//	return cudaHostAlloc(ptr, size, cudaHostAllocDefault << endl;

// 2. malloc()
	*ptr = malloc(size);
	if (!(*ptr))
		return cudaErrorMemoryAllocation;
	return cudaSuccess;
}

cudaError_t myHostFree(void* ptr) {
// 1. CUDA free
//	return cudaFreeHost(ptr);

// 2. free
	free(ptr);
	return cudaSuccess;
}

void print_dev_prop(cudaDeviceProp devProp) {
	cout << "*** CUDA device properties *** " << endl;
	cout << "Major revision number:         " <<  devProp.major << endl;
	cout << "Minor revision number:         " <<  devProp.minor << endl;
	cout << "Name:                          " <<  devProp.name << endl;
	cout << "Total global memory:           " <<  devProp.totalGlobalMem << endl;
	cout << "Total shared memory per block: " <<  devProp.sharedMemPerBlock << endl;
	cout << "Total registers per block:     " <<  devProp.regsPerBlock << endl;
	cout << "Warp size:                     " <<  devProp.warpSize << endl;
	cout << "Maximum memory pitch:          " <<  devProp.memPitch << endl;
	cout << "Maximum threads per block:     " <<  devProp.maxThreadsPerBlock << endl;
	for (int i = 0; i < 3; ++i)
		cout << "Maximum dimension " << i << " of block:  " << devProp.maxThreadsDim[i] << endl;
	for (int i = 0; i < 3; ++i)
		cout << "Maximum dimension " << i << " of grid:   " << devProp.maxGridSize[i] << endl;
	cout << "Clock rate:                    " <<  devProp.clockRate << endl;
	cout << "Total constant memory:         " <<  devProp.totalConstMem << endl;
	cout << "Texture alignment:             " <<  devProp.textureAlignment << endl;
	cout << "Concurrent copy and execution: " <<  (devProp.deviceOverlap ? "Yes" : "No") << endl;
	cout << "Number of multiprocessors:     " <<  devProp.multiProcessorCount << endl;
	cout << "Kernel execution timeout:      " <<  (devProp.kernelExecTimeoutEnabled ? "Yes" : "No") << endl;
	cout << endl << endl;
}

void print_func_attr(cudaFuncAttributes* attr) {
	cout << "Binary Version        " << attr->binaryVersion << endl;
	cout << "Cache Mode CA         " << attr->cacheModeCA << endl;
	cout << "Const Size Bytes      " << attr->constSizeBytes << endl;
	cout << "Local Size Bytes      " << attr->localSizeBytes << endl;
	cout << "Max Threads per Block " << attr->maxThreadsPerBlock << endl;
	cout << "Num Regs              " << attr->numRegs << endl;
	cout << "PTX Version           " << attr->ptxVersion << endl;
	cout << "Shared Size Bytes     " << attr->sharedSizeBytes << endl << endl;
}

void print_info() {
	// Host memory
	long pages = sysconf(_SC_PHYS_PAGES);
	long page_size = sysconf(_SC_PAGE_SIZE);
	cout << "Host free memory " << pages * page_size << endl;
	// Device memory
	size_t mem_free, mem_tot;
	cudaMemGetInfo(&mem_free, &mem_tot);
	cout << "CUDA device total/free memory " << mem_tot << " / " << mem_free << endl;
	int deviceIndex;
	cudaGetDevice(&deviceIndex);
	cudaDeviceProp deviceProp;
	cudaGetDeviceProperties(&deviceProp, deviceIndex);
	int version = deviceProp.major * 10 + deviceProp.minor;
	cout << "Cuda Device " << deviceIndex << ", Version " << version << endl;
	print_dev_prop(deviceProp);
//	cudaFuncAttributes attr;
//	cudaFuncGetAttributes (&attr, (const void*)cuda_map);
//	print_func_attr(&attr);
}
