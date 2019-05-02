#define _CRT_SECURE_NO_DEPRECATE
#include "parallel-sort-tweets.h"
#include <time.h>

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
	int temp = tweets[i];
	tweets[i] = tweets[j];
	tweets[j] = temp;
}

void bitonicSortImperative_Serial(tweet * tweets, int n) {
	int i, j, k;

	printf("Started imperative bitonic search...");

	for (k = 2; k <= n; k = 2 * k) {
		for (j = k >> 1; j > 0; j = j >> 1) {
			for (i = 0; i < n; i++) {
				int ij = i ^ j;
				if ((ij) > i) {
					if ((i&k) == 0 && compareTweets(&tweets[i], &tweets[ij]) == 1) {
						exchangeElements(tweets, i, ij);
						printf("Swaping elements: %d -  %d\n", i, ij);
					}
					if ((i&k) != 0 && compareTweets(&tweets[i], &tweets[ij]) == -1) {
						exchangeElements(tweets, i, ij);
						printf("Swaping elements: %d -  %d\n", i, ij);
					}
				}
			}
		}
	}

	printf("Sorting done...");
}

void bitonicMergeRecursive_Serial(tweet * tweets, int lo, int n, enum Direction dir) {
	if (n > 1) {
		int m = n / 2;
		for (int i = lo; i < lo + m; i++) {
			if (compareTweets(&tweets[i], &tweets[i + m]) == dir) {
				exchangeElements(tweets, i, i + m);
				printf("Swaping elements: %d -  %d\n", i, i + m);
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

/*void bitonicMergeRecursive_Parallel(tweet * tweets, int lo, int n, enum Direction dir) {
	if (n > 1) {
		int m = n / 2;
		for (int i = lo; i < lo + m; i++) {
			if (compareTweets(&tweets[i], &tweets[i + m]) == dir) {
				exchangeElements(tweets, i, i + m);
				printf("Swaping elements: %d -  %d\n", i, i + m);
			}
		}
		bitonicMergeRecursive_Parallel(tweets, lo, m, dir);
		bitonicMergeRecursive_Parallel(tweets, lo + m, m, dir);
	}
}

void bitonicSortRecursive_Parallel(tweet * tweets, int lo, int n, enum Direction dir) {
	if (n > 1) {
		int m = n / 2;
		omp_set_nested(1);
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
}*/

void bitonicSort(tweet * tweets, int n, enum SortMode mode) {
	switch (mode) {
		case IMPERATIVE_SERIAL: bitonicSortImperative_Serial(tweets, n); break;
		case RECURSIVE_SERIAL: bitonicSortRecursive_Serial(tweets, 0, n, ASCENDING); break;
		//case RECURSIVE_PARALLEL: bitonicSortRecursive_Paralell(tweets, 0, n, ASCENDING); break;
		default: printf("Not implemented...");
	}
}

int main() {
	clock_t begin, end;
	float z1, z2, z3, z4;
	char c;

	int n = 4096;
	char *filenameInput = "unsorted4096.tweets";

	// recursive serial
	tweet * tweets = readTweets(n, filenameInput);

	begin = clock();
	bitonicSort(tweets, n, RECURSIVE_SERIAL);
	end = clock();

	writeTweets(tweets, n, "sorted4096_rec_serial.tweets");

	z1 = (end - begin) / CLOCKS_PER_SEC;

	printf("\n##############################################\n\n");

	// imperative serial
	tweets = readTweets(n, filenameInput);

	begin = clock();
	bitonicSort(tweets, n, IMPERATIVE_SERIAL);
	end = clock();

	writeTweets(tweets, n, "sorted4096_imp_serial.tweets");

	z2 = (end - begin) / CLOCKS_PER_SEC;

	printf("\n##############################################\n\n");

	printf("Runtime of RECURSIVE_SERIAL in seconds: %f\n", z1);
	printf("Runtime of IMPERATIVE_SERIAL in seconds: %f\n", z2);
	c = getchar();

	qsort((void *)tweets, 4096, sizeof(tweet), compareTweets);
	return 0;
}
