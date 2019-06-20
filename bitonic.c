/*
	Parallel Systems, Sommersemester 2019
	Sorting Tweets
	(c) A. Fortenbacher
	2019-05-26
*/

#define _CRT_SECURE_NO_WARNINGS

#include "tweets.h"
#include "mpi.h"

enum Direction {
	ASCENDING = 1,
	DESCENDING = -1
};

void exchange(tweet_buffer* tb0, tweet_buffer* tb1, int n4) {
	int i, k;
	k = 0;
	for (i = 0; i < n4; i++) {
		tb0->y[k++] = tb0->x[i];
	}
	for (i = 0; i < n4; i++) {
		tb0->y[k++] = tb1->x[i];
	}
	k = 0;
	for (i = 0; i < n4; i++) {
		tb1->y[k++] = tb0->x[i + n4];
	}
	for (i = 0; i < n4; i++) {
		tb1->y[k++] = tb1->x[i + n4];
	}
}

void merge(tweet* ta, tweet* tb, int n2, int n4) {
	int i, j, k;
	i = 0;
	j = n4;
	k = 0;
	while (k < n2) {
		if (i == n4) {
			tb[k++] = ta[j++];
		}
		else if (j == n2) {
			tb[k++] = ta[i++];
		}
		else if (compare_tweets((void*) &(ta[i]), (void*) &(ta[j])) == -1) {
			tb[k++] = ta[i++];
		}
		else {
			tb[k++] = ta[j++];
		}
	}
}

void exchangeElements(tweet * tweets, int i, int j) {
	tweet temp = tweets[i];
	tweets[i] = tweets[j];
	tweets[j] = temp;
}

/*
 * function return -1 if x is zero or not power 2; returns exponent of power 2
 */
int powerOfTwo(int x) {
	if (x == 0 || x == 1) {
		return -1;
	}

	int pow = 0;
	while (x != 1) {
		pow++;
		if (x % 2 != 0) {
			// is not a power of two
			return -1;
		}
		x /= 2;
	}
	return pow;
}

void mergeBuffers(tweet_buffer * buffer, int dir, int start, int end) {
	for (int i = start; i < end; i++) {
		int result = compare_tweets(&buffer->x[i], &buffer->y[i]);
		//printf("rank %d: comp %s with  %s - res: %d\n", rank, buffer->x[i].tw_date, buffer->y[i].tw_date, result);
		if (result == dir) {
			//printf("Exc: %s - %s\n", buffer->x[i].tw_date, buffer->y[i].tw_date);
			tweet* temp = (tweet*)malloc(sizeof(tweet));
			bcopy(&buffer->x[i], temp, sizeof(tweet));
			bcopy(&buffer->y[i], &buffer->x[i], sizeof(tweet));
			bcopy(temp, &buffer->y[i], sizeof(tweet));
			free(temp);

			/*tweet temp = buffer->x[i];
			buffer->x[i] = buffer->y[i];
			buffer->y[i] = temp;*/
		}
	}
}

void bitonicSortImperative(tweet * tweets, int n, enum Direction dir) {
	int i, j, k;

	for (k = 2; k <= n; k = 2 * k) {
		for (j = k >> 1; j > 0; j = j >> 1) {
			for (i = 0; i < n; i++) {
				int ij = i ^ j;
				if ((ij) > i) {
					if (dir == ASCENDING) {
						// sort ASCENDING
						if ((i&k) == 0 && compare_tweets(&tweets[i], &tweets[ij]) == 1) {
							exchangeElements(tweets, i, ij);
							printf("Swaping elements: %d -  %d\n", i, ij);
						}
						if ((i&k) != 0 && compare_tweets(&tweets[i], &tweets[ij]) == -1) {
							exchangeElements(tweets, i, ij);
							printf("Swaping elements: %d -  %d\n", i, ij);
						}
					}
					else {
						// sort DESCENDING
						if ((i&k) != 0 && compare_tweets(&tweets[i], &tweets[ij]) == 1) {
							exchangeElements(tweets, i, ij);
							printf("Swaping elements: %d -  %d\n", i, ij);
						}
						if ((i&k) == 0 && compare_tweets(&tweets[i], &tweets[ij]) == -1) {
							exchangeElements(tweets, i, ij);
							printf("Swaping elements: %d -  %d\n", i, ij);
						}
					}
					
				}
			}
		}
	}
}


int main(int argc, char **argv) {
	char c;
	int numtasks, rank, dest, source, count, tag = 0;
	MPI_Status status;
	MPI_Request req;

	// init mpi and determine number of processes and current process id
	MPI_Init(NULL, NULL);
	MPI_Comm_size(MPI_COMM_WORLD, &numtasks);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);

	printf("################## PROCESS %d ##################\n", rank);

	// make sure that number of processes is power of two
	int exponent = powerOfTwo(numtasks);
	if(exponent == -1) {
		printf("Number of processes is zero or not power of 2. Aborting...");
	}

	// define length of buffers ...
	int n = 16;
	int n2 = n / numtasks;
	int n4 = n2 / numtasks;

	//FILE* inputFile = fopen( file, "rb" );
	char inputFile[64];
	snprintf(inputFile, sizeof(inputFile), "/home/s0544651/share/lehrende/forte/16777216_%d.tweets", rank);

	// read in file on current node
	printf("read file: %s\n", inputFile);
	tweet_buffer* buffers = read_tweets(1, n, n, inputFile);
	set_search_key("er");

	// do sorting on current node -> here open mp can be used
	if (rank % 2 == 0) {
		// even rank sorts list descending
		bitonicSortImperative(buffers->x, buffers->length, DESCENDING);
		printf("process %d is sorting local tweet list descending... \n", rank);
		//qsort(buffers->x, n, sizeof(tweet), compare_tweets);
	}

	else {
		// odd rank sorts list ascending
		bitonicSortImperative(buffers->x, buffers->length, ASCENDING);
		printf("process %d is sorting local tweet list ascending... \n", rank);
		//qsort(buffers->x, n, sizeof(tweet), compare_tweets);
	}

	//printf("x buffer: %d\n", buffers->x[0].tw_number);

	count = n * sizeof(tweet);

	//printf("count: %d \n", count);

	//enum Direction dir = DESCENDING;

	for (int k = 2; k <= numtasks; k = 2 * k) {
		for (int j = k >> 1; j > 0; j = j >> 1) {
			for (int i = 0; i < numtasks; i++) {
				int ij = i ^ j;
				int iak = i & k;
				int tag;
				int stepsize = k/2;
				//printf("[k = %d, j = %d, i = %d, ij = %d, iak = %d]\n", k, j, i, ij, iak);
				if (rank == i) {
					if((rank + stepsize) % 2 == 0)  {
						MPI_Send(&buffers->x[0], count/2, MPI_BYTE, ij, 0, MPI_COMM_WORLD);
						MPI_Probe(ij, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
						tag = status.MPI_TAG;
						if(status.MPI_TAG == 1) {
							MPI_Recv(&buffers->y[n/2], count/2, MPI_BYTE, ij, 1, MPI_COMM_WORLD, &status);
						} else {
							MPI_Recv(&buffers->y[0], count/2, MPI_BYTE, ij, 0, MPI_COMM_WORLD, &status);
						}
					} else {
						MPI_Send(&buffers->x[n/2], count/2, MPI_BYTE, ij, 1, MPI_COMM_WORLD);
					    MPI_Probe(ij, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
						tag = status.MPI_TAG;
						if(status.MPI_TAG == 1) {
							MPI_Recv(&buffers->y[n/2], count/2, MPI_BYTE, ij, 1, MPI_COMM_WORLD, &status);
						} else {
							MPI_Recv(&buffers->y[0], count/2, MPI_BYTE, ij, 0, MPI_COMM_WORLD, &status);
						}
					}

					MPI_Barrier(MPI_COMM_WORLD);
					int dir, start, end;

					if(iak == 0) {
						if(stepsize == 1) {
							dir = 1;
							if(tag == 1) {
								start = n/2;
								end = n;
							} else {
								start = 0;
								end = n/2;
							}
						} else {
							dir = -1;
							if(tag == 1) {
								start = 0;
								end = n/2;
							} else {
								start = n/2;
								end = n;
							}
						}
					} else {
						if(tag == 0) {
							dir = 1;
							start = 0;
							end = n/2;
						} else {
							dir = -1;
							start = n/2;
							end = n;
						}
					}

					mergeBuffers(buffers, dir, start, end);
					printf("proc: %d proc: %d is sorting list from %d to %d (iak = %d | tag = %d | step = %d)\n", rank, ij, start, end, iak, tag, stepsize);

					if((rank + stepsize) % 2 == 0)  {
						MPI_Send(&buffers->y[n/2], count/2, MPI_BYTE, ij, 1, MPI_COMM_WORLD);
						MPI_Probe(ij, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
						if(status.MPI_TAG == 0)  {
							MPI_Recv(&buffers->x[0], count/2, MPI_BYTE, ij, 0, MPI_COMM_WORLD, &status);
						} else {
							MPI_Recv(&buffers->x[n/2], count/2, MPI_BYTE, ij, 1, MPI_COMM_WORLD, &status);
						}
					} else {
						MPI_Send(&buffers->y[0], count/2, MPI_BYTE, ij, 0, MPI_COMM_WORLD);
						MPI_Probe(ij, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
						if(status.MPI_TAG == 0)  {
							MPI_Recv(&buffers->x[0], count/2, MPI_BYTE, ij, 0, MPI_COMM_WORLD, &status);
						} else {
							MPI_Recv(&buffers->x[n/2], count/2, MPI_BYTE, ij, 1, MPI_COMM_WORLD, &status);
						}
					}

					MPI_Barrier(MPI_COMM_WORLD);
				}
			}
			
		}
	}

	// write sorted local file back
	char filename[14];
	snprintf(filename, sizeof(filename), "test_%d.tweets", rank);
	write_tweets(buffers->x, n, filename);

	// finalize
	MPI_Finalize();
	
	/*tweet_buffer* buffers = read_tweets(2, n2, n, "16777216_0.tweets");
	set_search_key("er");
	printf("start sort\n");
	qsort((void*)buffers[0].x, n2, sizeof(tweet), compare_tweets);
	printf("first buffer sorted\n");
	qsort((void*)buffers[1].x, n2, sizeof(tweet), compare_tweets);
	printf("second buffer sorted\n");
	exchange(buffers, buffers + 1, n4);
	printf("exchange finished\n");
	merge(buffers[0].y, buffers[0].x, n2, n4);
	printf("first buffer merged\n");
	merge(buffers[1].y, buffers[1].x, n2, n4);
	printf("second buffer merged\n");
	write_tweets(buffers[0].x, n2, "8388608_0.stweets");
	write_tweets(buffers[1].x, n2, "8388608_1.stweets");*/

	//c = getchar();

	return 0;
}


