#define _CRT_SECURE_NO_DEPRECATE
#include "parallel-sort-tweets.h"
#include "omp.h"
#include <time.h>
#include <windows.h>

enum SortMode {
	IMPERATIVE_SERIAL,
	IMPERATIVE_PARALLEL,
	RECURSIVE_SERIAL,
	RECURSIVE_PARALLEL
};

enum Direction {
	ASCENDING = 1,
	DESCENDING = -1
};


int compareTweets(const void * a, const void * b) {
	for (int i = 0; i < 10000; i++) {
		int j = 0;
	}
	tweet * ta = (tweet *)a;
	tweet * tb = (tweet *)b;
	if (*ta > *tb) {
		return 1;
	}
	if (*tb > *ta) {
		return -1;
	}
	return 0;
}

tweet * readTweets(int n, char * filename) {
	FILE * fp = fopen(filename, "r");
	tweet * tweets = (tweet *)malloc(n * sizeof(tweet));
	char buffer[255];
	int i;
	for (i = 0; i < n; i++) {
		fgets(buffer, 255, fp);
		tweets[i] = atoi(buffer);
	}
	fclose(fp);
	return tweets;
}

void writeTweets(tweet * tweets, int n, char * filename) {
	FILE * fp = fopen(filename, "w");
	int i;
	for (i = 0; i < n; i++) {
		fprintf(fp, "%d\n", tweets[i]);
	}
	fclose(fp);
}

void exchangeElements(tweet * tweets, int i, int j) {
	tweet temp = tweets[i];
	tweets[i] = tweets[j];
	tweets[j] = temp;
}

void bitonicSortImperative_Serial(tweet * tweets, int n) {
	int i, j, k;
	for (k = 2; k <= n; k = 2*k) {
		printf("IMP_SERIAL... k=%d\n", k);
		for (j = k>>1; j > 0; j = j>>1) {
			for (i = 0; i < n; i++) {
				int ij = i^j;
				if ((ij) > i) {
					// asc
					if ((i&k) == 0 && compareTweets(&tweets[i], &tweets[ij]) == 1) {
						exchangeElements(tweets, i, ij);
						//printf("Swaping elements: %d -  %d\n", i, ij);
					}
					// dsc
					if ((i&k) != 0 && compareTweets(&tweets[i], &tweets[ij]) == -1) {
						exchangeElements(tweets, i, ij);
						//printf("Swaping elements: %d -  %d\n", i, ij);
					}
				}
			}
		}
	}
}

void bitonicSortImperative_Parallel(tweet * tweets, int n) {
	int numThreads = omp_get_max_threads();
	int i, j, k;

	for (k = 2; k <= n; k = 2 * k) {
		printf("REC_SERIAL... k=%d\n", k);
		for (j = k >> 1; j > 0; j = j >> 1) {
			#pragma omp parallel for num_threads(numThreads) schedule(dynamic)
			for (i = 0; i < n; i++) {
				int ij = i ^ j;
				if ((ij) > i) {
					if ((i&k) == 0 && compareTweets(&tweets[i], &tweets[ij]) == 1) {
						exchangeElements(tweets, i, ij);
						//printf("Swaping elements: %d -  %d\n", i, ij);
					}
					if ((i&k) != 0 && compareTweets(&tweets[i], &tweets[ij]) == -1) {
						exchangeElements(tweets, i, ij);
						//printf("Swaping elements: %d -  %d\n", i, ij);
					}
				}
			}
		}
	}
}

void bitonicMergeRecursive_Serial(tweet * tweets, int lo, int n, enum Direction dir) {
 	if (n > 1) {
		int m = n / 2;
		for (int i = lo; i < lo + m; i++) {
			if (compareTweets(&tweets[i], &tweets[i + m]) == dir) {
				exchangeElements(tweets, i, i + m);
				//printf("Swaping elements: %d -  %d\n", i, i + m);
			}
		}
		bitonicMergeRecursive_Serial(tweets, lo, m, dir);
		bitonicMergeRecursive_Serial(tweets, lo + m, m, dir);
	}
}

void bitonicSortRecursive_Serial(tweet * tweets, int lo, int n, enum Direction dir) {
	if (n > 1) {
		int m = n / 2;
		bitonicSortRecursive_Serial(tweets, lo, m, ASCENDING);
		bitonicSortRecursive_Serial(tweets, lo + m, m, DESCENDING);
		bitonicMergeRecursive_Serial(tweets, lo, n, dir);
	}
}

void bitonicMergeRecursive_Parallel(tweet * tweets, int lo, int n, enum Direction dir) {
	if (n > 1) {
		int m = n / 2;
		for (int i = lo; i < lo + m; i++) {
			if (compareTweets(&tweets[i], &tweets[i + m]) == dir) {
				exchangeElements(tweets, i, i + m);
				//printf("Swaping elements: %d -  %d\n", i, i + m);
			}
		}
		int numThreads = omp_get_max_threads();
		#pragma omp parallel sections num_threads(numThreads)
		{
			#pragma omp section
			bitonicMergeRecursive_Parallel(tweets, lo, m, dir);
			#pragma omp section
			bitonicMergeRecursive_Parallel(tweets, lo + m, m, dir);
		}
		
	}
}

void bitonicSortRecursive_Parallel(tweet * tweets, int lo, int n, enum Direction dir) {
	if (n > 1) {
		int m = n / 2;
		int numThreads = omp_get_max_threads();
		#pragma omp parallel sections num_threads(numThreads)
		{
			#pragma omp section
			bitonicSortRecursive_Parallel(tweets, lo, m, ASCENDING);
			#pragma omp section
			bitonicSortRecursive_Parallel(tweets, lo + m, m, DESCENDING);
		}
		bitonicMergeRecursive_Parallel(tweets, lo, n, dir);
	}
}

float bitonicSort(tweet * tweets, int n, enum SortMode mode) {
	if (n < 2) {
		return 0.0;
	}

	clock_t time = clock();

	switch (mode) {
		case IMPERATIVE_SERIAL: bitonicSortImperative_Serial(tweets, n); break;
		case RECURSIVE_SERIAL: bitonicSortRecursive_Serial(tweets, 0, n, ASCENDING); break;
		case RECURSIVE_PARALLEL: bitonicSortRecursive_Parallel(tweets, 0, n, ASCENDING); break;
		case IMPERATIVE_PARALLEL: bitonicSortImperative_Parallel(tweets, n); break;
		default: printf("Not implemented...");
	}

	time = clock() - time;
	return (((float)time) / CLOCKS_PER_SEC);
}

int main() {
	char c;
	float z1, z2, z3, z4;

	int n = 4096;
	char *filenameInput = "unsorted4096.tweets";

	// recursive serial
	tweet * tweets = readTweets(n, filenameInput);
	z1 = bitonicSort(tweets, n, RECURSIVE_SERIAL);
	writeTweets(tweets, n, "sorted16_rec_serial.tweets");

	// imperative serial
	tweets = readTweets(n, filenameInput);
	z2 = bitonicSort(tweets, n, IMPERATIVE_SERIAL);
	writeTweets(tweets, n, "sorted4096_imp_serial.tweets");


	// recursive parallel
	tweets = readTweets(n, filenameInput);
	z3 = bitonicSort(tweets, n, RECURSIVE_PARALLEL);
	writeTweets(tweets, n, "sorted4096_rec_parallel.tweets");

	//imperative parallel
	tweets = readTweets(n, filenameInput);
	z4 = bitonicSort(tweets, n, IMPERATIVE_PARALLEL);
	writeTweets(tweets, n, "sorted4096_imp_parallel.tweets");

	printf("Runtime of RECURSIVE_SERIAL in seconds:    %f\n", z1);
	printf("Runtime of IMPERATIVE_SERIAL in seconds:   %f\n", z2);
	printf("Runtime of RECURSIVE_PARALLEL in seconds:  %f\n", z3);
	printf("Runtime of IMPERATIVE_PARALLEL in seconds: %f\n", z4);

	c = getchar();

	//qsort((void *)tweets, 4096, sizeof(tweet), compareTweets);
	return 0;
}
