# README #


Master's Thesis project, in order to run, there is the need of starting dataset in json format, or a sequence File with DocumentID,DocumentText

The Class to be considered is in src/com/luca/filipponi/tweetAnalysis:

TextualClustering.java, which executes the cluster for a specified k, on the dataset provided as input
Main.java, which executes the cluster from k=2 to k=20, on a dataset provided as input

For evaluating the results the class is in src/com/luca/filipponi/tweetAnalysis/ClusterEvaluator:

ClusterEvaluator.java used for see top word, top tweet, distance from each cluster and cluster composition on different time windows, for time widows part needs a mySql db connect or won't work.
TokenEvaluator.java used for evaluate the token quality.
