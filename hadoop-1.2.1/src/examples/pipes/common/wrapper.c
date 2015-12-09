/**
 * 2015 Added by Dumi Loghin
 * Wrapper over pipes workloads to enable nvprof profiling.
 */

#include <stdlib.h> 
#include <stdio.h>

/**
 * APP_PATH must be defined! (pass it with -D)
 */

int main() {
	char cmd[256];
	sprintf(cmd, "/usr/local/cuda/bin/nvprof -m ipc %s", APP_PATH);	
	// printf("%s\n", cmd);
	return system(cmd);
}

