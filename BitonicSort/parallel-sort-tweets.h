#include <stdio.h>
#include <stdlib.h>

typedef int tweet;

int compareTweets(const void * a, const void * b);

tweet * readTweets(int n, char * filename);

void writeTweets(tweet * tweets, int n, char * filename);

void * tweets2bytes(tweet * tweets, int numberTweets, int * numberBytes);

tweet * bytes2tweets(void * bytes, int numberBytes, int * numberTweets);
