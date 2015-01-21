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
#include "common.hh"
#include <stdio.h>
#include <math.h>

using namespace std;

#define F_PROFILE

#define MaxKey				16

#define NUM_RUNS 			100

typedef float fptype;

//
// Taken from Parsec 3.0 Blackscholes App - blackscholes.c
//
// Cumulative Normal Distribution Function
// See Hull, Section 11.8, P.243-244
#define inv_sqrt_2xPI 0.39894228040143270286

fptype CNDF ( fptype InputX )
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

fptype BlkSchlsEqEuroNoDiv( fptype sptprice,
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

class BlackScholesMapper : public HadoopPipes::Mapper {
public:
	string exp;

	BlackScholesMapper( HadoopPipes::TaskContext& context ) {}

	// map function
	void map(HadoopPipes::MapContext& context) {
		char key[MaxKey], result[MaxKey];
		char* value = (char*)context.getInputValue().c_str();
		int k;

		// input
		fptype spotPrice, strike, rate, divq, volatility, time;
		int otype;
		char ochar;

		// output
		fptype price;

		sscanf(value, "%s %f %f %f %f %f %f %c", key, &spotPrice, &strike, &rate, &divq, &volatility, &time, &ochar);
		otype = (ochar == 'P') ? 1 : 0;

		for (k=0; k<NUM_RUNS; k++) {
			price = BlkSchlsEqEuroNoDiv(spotPrice, strike, rate, volatility, time, otype, 0);
		}

		sprintf(result, "%.2lf", price);
		// sprintf(result, "%X", *(int*)&price);
		// sprintf(result, "%e", price);
		context.emit(key, string(result));
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
	//start the map/reduce job
#ifdef F_PROFILE
	cout << "Starting BlackScholes Pipes" << endl;
	double t0 = get_time();
#endif
	int res = HadoopPipes::runTask(HadoopPipes::TemplateFactory<BlackScholesMapper,BlackScholesReducer>());
#ifdef F_PROFILE
	double t1 = get_time();
	cout << "BlackScholes Pipes finished in " << (t1 - t0) <<  " seconds." << endl;
#endif
	return res;
}
