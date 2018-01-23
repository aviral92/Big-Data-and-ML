Q)Network friendship recommendation algorithm. 
The key idea is that if two people have a lot of
mutual friends, then the system should recommend they connect with each other.
Input: Given Inoput.txt file
The input file contains the adjacency list and has multiple lines in the following format:
<User><TAB><Friends>
Here, <User> is a unique integer ID corresponding to a unique user and <Friends> is a comma-
separated list of unique IDs corresponding to the friends of the user with the unique ID <User>.
Note that the friendships are mutual (i.e., the edges are undirected): if A is friend with B then
B is also friend with A. The data provided is consistent with the rule that as there is an explicit
entry for each side of each edge.
Algorithm: Let us use a simple algorithm such that, for each user U, the algorithm recommends
N = 10 users who are not already friends with U, but have the largest number of mutual friends
in common with U.
Output: The output should contain one line per user in the following format:
<User><TAB><Recommendations>
where <User> is a unique ID corresponding to a user and Recommendations is a comma-separated
list of unique IDs corresponding to the algorithm's recommendation of people that <User> might
know, ordered by decreasing number of mutual friends. Even if a user has fewer than 10 second-
degree friends, output all of them in decreasing order of the number of mutual friends. If a user
has no friends, you can provide an empty list of recommendations. If there are multiple users with
the same number of mutual friends, ties are broken by ordering them in a numerically ascending
order of their user IDs.
Also, please provide a description of how you are going to use MapReduce jobs to solve this problem.
Don't write more than 3-4 sentences for this: we only want a high-level description of your strategy
to tackle this problem.
Note: It is possible to solve this question with a single MapReduce job. But if your solution
requires multiple MapReduce jobs, then that's ok too.
