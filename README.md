dbpedia.pagerank
================

PageRank Computation for RDF-Graphs. 

Definition 
================
The PageRank of a graph g with n nodes is a vector with n elements, where an element is a real number from 0 to 1. The nth number denotes the propability of being at node n in time t. Therefore, the PageRank is a propability distribution over g.

Idea
================
The PageRank algorithm was invented by Brin and Page to solvethe fundamental question of any search engine: How relevant is the retrievedinformation for the questioner? PageRank, thereby, relies on the information which is encoded in the linking structure between documents. A document with many incoming links is more important than one with only a few. Further, a document which is linked by an important document is also considered important. That is,the importance of document is defined by the number of linking pages and their importance.

Computaional Model
================
PageRank employs the so-called random surfermodel: A user which traverses the (web) graph by choosing outgoing links on a random basis and sometimes restarts his/hers traversal at a random node.
Mathematically, this can be expressed in Markov chain terms. The current state of a graph has the propability distribution of p (= PageRank Vector).
Transition from one graph state to another is moving from one document to another or going to a random node. The propability of a transition from document d1 to another linked document d2 is depicted as: p(d1)/#outlinks(d1). Where #outlinks(d1) is the number of links to otherdocuments. The propabilty of a transition from a document to a random document is defined as 1/numberOfDocuments.
p t+1 = p t * M, where M is the transition propability Matrix.
M = s * A + t * E  where s is 0.85 and t = 1 - s.


