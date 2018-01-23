PageRank computation: Write a program in Spark to compute the PageRanks for each webpage
in the following sample web graph { http://snap.stanford.edu/class/cs246-data/graph.txt.
Input: Each number in the le represents a node and each line represents a directed edge going
from the rst node to the second.
<Vertex 1><TAB><Vertex 2>
What to submit:
a) Short description of map and reduce functions that you used.
b) Output file for the graph containing the list of all node IDs and their corresponding PageRank
scores, in a simple format as below.
<Vertex ID><TAB><PageRank>.
Compute both simple PageRank as well as modied PageRank assuming dead ends and spider
traps. Use B = 0.8
