Document Similarity and Clustering: In this problem, you will use the ideas on clustering
and computing similarities that you learnt in Chapter 3 in the book, including k-Means, shingling
and Jaccard similarity. This is to be implemented in Spark.
Input: Download the dataset from here. The dataset contains a list of short news articles in a
CSV format, where each row contains the columns { the short article, date of publication, title and
category.

map-reduce: Implement iterative k-Means using the map-reduce paradigm, where a single step of
map-reduce completes one iteration of the k-Means algorithm. So, to run k-Means for i iterations,
you will have to run a sequence of i map-reduce jobs.
Number of clusters: Remember that k-Means by itself does not tell you the number of clusters
actually present in the data. It performs a clustering, in an unsupervised fashion, given the number
of clusters it is expected to nd (through our input k). So in order to determine the best number
of clusters for the data, we need to examine inter-cluster distance and intra-cluster distance for a
range of k values, or equivalently, intra-cluster variance and total variance. Generally, the \knee"
or the \elbow" in the plot of total intra-cluster variance (or distance) vs the value of k indicates
the best number of clusters. Read this on how to use the elbow method to determine the optimal
2
number of clusters. The quantity SSE is the same as intra-cluster variance viz. the sum of squared
difference between each datapoint and the centroid of the cluster it belongs to.
Initialization: The manner in which the k centroids are initialized to begin with sometimes makes
a difference. You can randomly choose any k of all the vectors as initial centroids, to begin with.
There are other ways you can initialize { refer to section 7.3.2 in the book for some ideas. You
are free to use other methods of your own or from external references (please cite references; see
below).
Tasks: These are the tasks you are required to do for this problem.
 Before performing k-Means, you will need to convert the documents into vectors of equal size.
For this, use minhash signatures. You can experiment with the size of signatures. A size of
100 may be a good idea. For shingles, you may use any type of shingles { character or word.
If character, then 5-character shingle may be a good starting point. If word, then check out
section 3.2.4 in the book for some ideas for news articles. Implement minhashing using map
and reduce functions for additional credit.
 Run k-Means on the signatures to do the clustering (make k a parameter in your code i.e.
don't hardcode it). Use Euclidean distance measure. Try a sequence of k values (say, from 1
to 10) and determine the best number of clusters using the elbow method.
 Now, once the clustering is done and you have determined the best number of clusters, man-
ually check the documents in each cluster to see if the above sequence of computations have
correctly clustered the documents according to similarity.
Note: It is expected that you use map and reduce functions to implement the k-Means algorithm.
Using map and reduce functions for computing minhashes is optional, and will earn you additional
credit.
What to submit:
a) The elbow plot that was created to determine optimal number of cluster for k-Means.
b) Short write-up detailing the assumptions and 
ow of computations in your code. Also briefly
state, from your manual observation, the performance of your clustering. In particular, high-
light any bad examples (most likely as a result of using shortened minhash signatures). Also
mention the rationale behind your choices, wherever you have made them, such as (but not
limited to) the shingles, k in k-Means and size of signatures.
