#Algorithm : Page Rank-
#1) Initialize each page’s rank to 1/number of nodes in the graph
#2) Loop : From each page p, send a rank(p)/num of Neighbors(p) to its neighbors (the pages it has links to).
#3) Add the contributions sent.


#Code : Page Rank
#---------------------------------------------------------------------
val file=sc.textFile("/user/ak6755/graph.txt")
val links = file.map{ s => val parts = s.split("\\s+") 
     (parts(0), parts(1))
   }.distinct().groupByKey().cache()
   var num=links.count
   var ranks = links.mapValues(v => 1.0/num)

   for (i <- 1 to 100) {
     val contribs = links.join(ranks).values.flatMap{ case (urls, rank) =>
     val size = urls.size
     urls.map(url => (url, rank / size))
     }
     ranks = contribs.reduceByKey(_ + _)
   }
ranks.saveAsTextFile("/user/ak6755/pr")



#Algorithm : Modified Page Rank-
#Same Algorithms as page rank, just a small modification. After comptuing the page rank, multiply by B(=0.8) and add (1-B)/NumberOfNodes #in the graph.

#Modified Page Rank
---------------------------------------
val file=sc.textFile("/user/ak6755/graph.txt")
val links = file.map{ s => val parts = s.split("\\s+") 
     (parts(0), parts(1))}.distinct().groupByKey().cache()
   var num=links.count
   var ranks = links.mapValues(v => 1.0/num)

   for (i <- 1 to 100) {
     val contribs = links.join(ranks).values.flatMap{ case (urls, rank) =>
     val size = urls.size
     urls.map(url => (url, rank / size))
     }
     ranks = contribs.reduceByKey(_ + _).mapValues(0.20 /num + 0.80 * _)
   }
ranks.saveAsTextFile("/user/ak6755/mpr.txt")


