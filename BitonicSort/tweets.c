/*
	Parallel Systems, Sommersemester 2019
	Sorting Tweets
	(c) A. Fortenbacher
	2019-05-26
*/

#define _CRT_SECURE_NO_DEPRECATE

#include "tweets.h"
#include "mpi.h"

char* search_key;

int number_keywords(char* str) {
	int i, k = 0, key_length, n = 0;
	key_length = strlen(search_key);
	for (i = 0; i < strlen(str); i++) {
		if (str[i] == search_key[k]) {
			k += 1;
			if (k == key_length) {
				n += 1;
				k = 0;
			}
		}
	}
	return n;
}

int number_hashtags(char* str) {
	int i, n = 0;
	for (i = 0; i < strlen(str); i++) {
		if (str[i] == '#') {
			n += 1;
		}
	}
	return n;
}

/*
	compare tweet1 and tweet2
	if ( tweet1 has more keywords than tweet2 ) return -1
	else if ( tweet2 has more keywords than tweet1 ) return 1
	else if ( if tweet1 has more '#' than tweet2 ) return -1
	else if ( tweet2 has more '#' than tweet1 ) return 1
	else if ( tweet1's date is more recent than tweet2's date ) return -1
	else if ( tweet2's date is more recent than tweet1's date ) return 1
	else return 0
*/
int compare_tweets(const void* t1, const void* t2) {
	int i, n1, n2;
	tweet* tweets1 = (tweet*)t1;
	tweet* tweets2 = (tweet*)t2;
	n1 = number_keywords(tweets1->tw_text);
	n2 = number_keywords(tweets2->tw_text);
	if (n1 > n2) return -1;
	if (n1 < n2) return 1;
	n1 = number_hashtags(tweets1->tw_text);
	n2 = number_hashtags(tweets2->tw_text);
	if (n1 > n2) return -1;
	if (n1 < n2) return 1;
	char* date1 = tweets1->tw_date;
	char* date2 = tweets2->tw_date;
	for (i = 0; i < strlen(date1); i++) {
		if (date1[i] > date2[i]) return -1;
		if (date1[i] < date2[i]) return 1;
	}
	return 0;
}

/*
	read tweets from file
	n_buffers: number of buffers
	size_buffers: number of tweets each buffer (x or y) can hold
	n_tweets: number of tweets to read, will be split into number of buffers
	return array of buffers (length n_buffers) containing all tweets
*/
tweet_buffer* read_tweets(int n_buffers, int size_buffers, int n_tweets, char* filename) {
	int i, j, k;
	tweet_buffer* buffers = (tweet_buffer*)malloc(n_buffers * sizeof(tweet_buffer));
	char line_buffer[1024];
	int tweets_per_buffer = n_tweets / n_buffers;
	if (size_buffers < tweets_per_buffer) size_buffers = tweets_per_buffer;
	FILE* fp = fopen(filename, "r");
	if (fp == NULL) {
		fprintf(stderr, "fopen(%s, \"r\") failed\n", filename);
		exit(1);
	}
	tweet_buffer* bb = buffers;
	for (j = 0; j < n_buffers; j++, bb++) {
		bb->x = (tweet*)malloc(size_buffers * sizeof(tweet));
		bb->y = (tweet*)malloc(size_buffers * sizeof(tweet));
		bb->length = size_buffers;
		tweet* t = bb->x;
		for (i = 0; i < tweets_per_buffer; i++, t++) {
			char* line = fgets(line_buffer, 1024, fp);
			k = 0;
			while (line[k] != '\t') k++;
			line[k] = 0;
			t->tw_file = (short)atoi(line);
			line += k + 1;
			k = 0;
			while (line[k] != '\t') k++;
			line[k] = 0;
			t->tw_number = atoi(line);
			line += k + 1;
			k = 0;
			while (line[k] != '\t') k++;
			line[k] = 0;
			memmove((void*)t->tw_date, (void*)line, strlen(line) + 1);
			line += k + 1;
			memmove((void*)t->tw_text, (void*)line, strlen(line) + 1);
			//printf("tw_file: %d\n", t->tw_file);
			//printf("tw_number: %d\n", t->tw_number);
			//printf("tw_date: %s\n", t->tw_date);
			//printf("tw_text: %s\n", t->tw_text);
		}
	}
	fclose(fp);
	return buffers;
}

/*
	write file_number and tweet_number for each tweet
*/

void write_tweets(tweet* tweets, int n, char* filename) {
	int i;
	FILE* fp;
	fp = fopen(filename, "w");
	for (i = 0; i < n; i++, tweets++) {
		printf("%d %d\n", tweets->tw_file, tweets->tw_number);
		fprintf(fp, "%d %d\n", tweets->tw_file, tweets->tw_number);
	}
	fclose(fp);
}

void set_search_key(char* k) {
	search_key = k;
}


