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

void bitonicSortImperative(tweet * tweets, int n) {
	int i, j, k;

	for (k = 2; k <= n; k = 2 * k) {
		for (j = k >> 1; j > 0; j = j >> 1) {
			for (i = 0; i < n; i++) {
				int ij = i ^ j;
				if ((ij) > i) {
					// asc
					if ((i&k) == 0 && compare_tweets(&tweets[i], &tweets[ij]) == 1) {
						exchangeElements(tweets, i, ij);
						//printf("Swaping elements: %d -  %d\n", i, ij);
					}
					// dsc
					if ((i&k) != 0 && compare_tweets(&tweets[i], &tweets[ij]) == -1) {
						exchangeElements(tweets, i, ij);
						//printf("Swaping elements: %d -  %d\n", i, ij);
					}
				}
			}
		}
	}
}

MPI_Datatype createMpiTweetDatatype() {
	MPI_Datatype mpi_tweet;

	int count = 4;
	int blocklens[] = { 1,1,1,1 };

	MPI_Aint offset[4];
	offset[0] = (MPI_Aint)offsetof(tweet, tw_file);
	offset[1] = (MPI_Aint)offsetof(tweet, tw_number);
	offset[2] = (MPI_Aint)offsetof(tweet, tw_date);
	offset[3] = (MPI_Aint)offsetof(tweet, tw_text);

	MPI_Datatype types[] = { MPI_SHORT, MPI_INT, MPI_CHAR, MPI_CHAR };

	MPI_Type_create_struct(count, blocklens, offset, types, &mpi_tweet);
	MPI_Type_commit(&mpi_tweet);

	return mpi_tweet;
}

int main(int *argc, char **argv) {

	int numtasks, rank, dest, source, count, tag = 1;
	MPI_Status status;

	MPI_Init(&argc, &argv);
	MPI_Comm_size(MPI_COMM_WORLD, &numtasks);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);

	int n = 32;
	int n2 = n / numtasks;
	int n4 = n2 / numtasks;

	char inputFile[18];
	snprintf(inputFile, sizeof(inputFile), "16777216_%d.tweets", rank);

	printf("read file: %s\n", inputFile);
	tweet_buffer* buffers = read_tweets(1, n2, n2, inputFile);

	count = buffers[0].length * sizeof(tweet);

	set_search_key("er");
	bitonicSortImperative(buffers[0].x, buffers[0].length);

	if (rank == 0) {
		MPI_Send(&buffers[0].x, count, MPI_BYTE, 1, tag, MPI_COMM_WORLD);
		//MPI_Probe(1, tag, MPI_COMM_WORLD, &status);
		//MPI_Recv(&buffers[0].y, count, MPI_BYTE, 1, tag, MPI_COMM_WORLD, &status);
	}
	else if (rank == 1) {
		MPI_Recv(&buffers[0].y, count, MPI_BYTE, 0, tag, MPI_COMM_WORLD, &status);
		//MPI_Send(&buffers[0].x, count, MPI_BYTE, 0, tag, MPI_COMM_WORLD);
		//MPI_Probe(0, tag, MPI_COMM_WORLD, &status);
	}

	char filename[14];
	snprintf(filename, sizeof(filename), "test_%d.tweets", rank);

	write_tweets(buffers[0].x, n2, filename);

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

