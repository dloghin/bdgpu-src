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
 * K-means Hadoop Pipes C++.
 */

#include "hadoop/Pipes.hh"
#include "hadoop/TemplateFactory.hh"
#include "hadoop/StringUtils.hh"
#include "common.hh"
#include <stdio.h>
#include <sstream>

using namespace std;

#define FDEBUG

#define F_PROFILE

// #define F_LOCAL_DEBUG

#define MaxFeatures		34

#define MaxClusters 	16

#define MaxLine 		(4 * MaxFeatures)	// 4 chars per feature

#define MAXINT 			1073741824

#define StrModelKey 	"hadoop.kmeans.centroidsfile"

#define CounterGroup 	"KmeansCounters"
#define MapCounter 		"WORDS"
#define RedCounter 		"VALUES"

enum Counter {WORDS, VALUES};

typedef struct {
	int id;
	int dim;
	int pt[MaxFeatures];
} t_cluster;

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
			return n;
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

class KmeansMapper : public HadoopPipes::Mapper {
public:
	t_cluster centroids[MaxClusters];
	int totalClusters;
	int dim;

	KmeansMapper(HadoopPipes::TaskContext& context) {
		// initialize centroids
		HadoopPipes::JobConf* conf = (HadoopPipes::JobConf*)context.getJobConf();
		if (conf != NULL) {
			string s = conf->get(StrModelKey);
			totalClusters = initializeCentroids(s);
		}
		else
			totalClusters = 0;
#ifdef FDEBUG
		cout << "Using " << totalClusters << " clusters!" << endl;
#endif
	}

	int initializeCentroids(string filename) {
		int i, k;

		FILE* f = fopen(filename.c_str(), "rt");
		if (!f)
			return -1;

		fscanf(f, "%d %d", &totalClusters, &dim);
		for (k=0; k<totalClusters; k++) {
			fscanf(f, "%d", &centroids[k].id);
			centroids[k].dim = dim;
			for (i=0; i<dim; i++)
				fscanf(f, "%d", &centroids[k].pt[i]);
		}
		fclose(f);

		return totalClusters;
	}

	void map(HadoopPipes::MapContext& context) {
		string val;
		int pt[MaxFeatures];
		int i, k, minDist, curDist, clusterId, delta;

		// get context data
		HadoopPipes::TaskContext::Counter* counter = context.getCounter(CounterGroup, MapCounter);

		val = context.getInputValue();

		i = parse_point((char*)val.c_str(), val.length(), dim, pt);
		if (i != dim) {
			cout << "Wrong point ( " << i << " != " << dim << ") " << val << endl;
			return;
		}

		minDist = MAXINT;
		clusterId = -1;
		for (k = 0; k < totalClusters; k++) {
			curDist = 0;
			for (i=0; i< dim; i++) {
				delta = pt[i] - centroids[k].pt[i];
				curDist += (delta * delta);
			}
			if (minDist > curDist) {
				clusterId = centroids[k].id;
				minDist = curDist;
			}
		}

#ifdef FDEBUG
		// cout << "Total " << totalClusters << " Min " << minDist << " Cluster Id " << clusterId << endl;
		if (clusterId < 1 && clusterId > totalClusters)
			cout << "Error cluster id outside bounds " << clusterId << endl;
#endif

		context.emit(HadoopUtils::toString(clusterId), val);
		context.incrementCounter(counter, 1);
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

		retval = print_point(clusters, dim);

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
	cout << "Starting Kmeans Pipes" << endl;
	double t0 = get_time();
#endif
	int res = HadoopPipes::runTask(HadoopPipes::TemplateFactory<KmeansMapper, KmeansReducer>());
#ifdef F_PROFILE
	double t1 = get_time();
	cout << "Kmeans Pipes finished in " << (t1 - t0) <<  " seconds." << endl;
#endif
	return res;
}
