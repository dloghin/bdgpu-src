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
#include "common.hh"

using namespace std;

#define F_PROFILE

#define GrepMapKey			"match"

#define GrepRegexKey			"mapred.mapper.regex"

class GrepMapper : public HadoopPipes::Mapper {
public:
	string exp;

	GrepMapper( HadoopPipes::TaskContext& context ) {
		HadoopPipes::JobConf* conf = (HadoopPipes::JobConf*)context.getJobConf();
		if (conf != NULL) {
			exp = conf->get(GrepRegexKey);
		}
		else {
			exp = "";
		}
	}

	// map function
	void map(HadoopPipes::MapContext& context) {
		string line = context.getInputValue();
		int pos = line.find(exp);
		if (pos >= 0 && pos < line.size()) {
			// context.emit(exp, line);
			context.emit(exp, "1");
		}
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
	//start the map/reduce job
#ifdef F_PROFILE
	cout << "Starting Grep Pipes" << endl;
	double t0 = get_time();
#endif
	int res = HadoopPipes::runTask(HadoopPipes::TemplateFactory<GrepMapper,GrepReducer>());
#ifdef F_PROFILE
	double t1 = get_time();
	cout << "Grep Pipes finished in " << (t1 - t0) <<  " seconds." << endl;
#endif
	return res;
}
