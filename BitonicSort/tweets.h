#pragma once

#include <stdio.h>
#include <stdlib.h>
/*
	Parallel Systems, Sommersemester 2019
	Sorting Tweets
	(c) A. Fortenbacher
	2019-05-26
*/

#include <string.h>

typedef struct {
	short tw_file;	// file number
	int tw_number;	// number within file
	char tw_date[11];	// format: 2019-05-20
	char tw_text[141];	// maximum of 140 characters
} tweet;

typedef struct {
	tweet* x;
	tweet* y;
	long length; // number of tweets that can be stored in b1 or b2
} tweet_buffer;

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
int compare_tweets(const void* t1, const void* t2);

/*
	read tweets from file
	n_buffers: number of buffers
	size_buffers: number of tweets each buffer (b1 or b2) can hold
	n_tweets: number of tweets to read, will be split into number of buffers
	return array of buffers (length n_buffers) containing all tweets
*/
tweet_buffer* read_tweets(int n_buffers, int size_buffers, int n_tweets, char* filename);

/*
	write file_number and tweet_number for each tweet
*/
void write_tweets(tweet* tweets, int n, char* filename);

void set_search_key(char* k);


