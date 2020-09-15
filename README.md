# Hypernym-Detection
Hypernym detection with Map-Reduce and machine learning

In this app we were based on the research paper “Learning syntactic patterns for automatic hypernym discovery” by R. Snow, D. Jurafsky and A. Ng.
,and re-designing its algorithm for the map reduce pattern and experimenting its quality on a large-scale input.

We build a program in map-reduce pattern, that builds feature vector for each noun pair, and then ran a script that build the .arff file for WEKA,
and then we give WEKA the file and examined its result from the training.

This application was written as part of a Distributed Systems Programming course taken in BGU.

using Amazon EMR cluster to process big data of [Google Syntactic Ngrams] (http://storage.googleapis.com/books/syntactic-ngrams/index.html). We processed and trained our classifier on the set Biarcs 00 & Biarcs 01.

For further information -  The Assignment description : https://www.cs.bgu.ac.il/~dsp202/Assignments/Assignment_3 
