

val input =sc.textFile("/user/ak6755/adj.txt").cache()
#find pairs of friends. Convert all friend lists to an array of friend pairs
val fp = input.map(l=>l.split("\\t")).filter(l=> (l.size == 2)).map(l=>(l(0),l(1).split(","))).flatMap(a=>a._2.flatMap(z=>Array((a._1.toInt,z.toInt))))

val myJoin = fp.join(fp)

#Get all possible pairs of friends. We will use this to get the mutual friends in the next step.
#We filtere out the values where first value is same as second value as that means that a friend i is friend with itself 
#and we do not need that tuple 
val allFriends = myJoin.map(e => e._2).filter(e => e._1 != e._2)

#Find the pairs of friends that have a mutual friend. Here we subtract the pairs of friends that computed in the first 
#step from the all possible pairs from friends in the previous step 
val mf = allFriends.subtract(fp)

#Map all mutual pairs to (pair, 1) and then reduce by key, so that we dont have duplicate pairs and add all the key values
val pairFriend = mf.map(m => (m, 1)).reduceByKey((a, b) => a + b)

#Mutual Pair - restructure the pairs of friends in the previous step.
val mp = pairFriend.map(e => (e._1._1, (e._1._2, e._2))).groupByKey()

def sortList(fList: List[(Int, Int)]) : List[Int]= { 
fList.sortBy(a =>(-a._2, a._1)).map(a => a._1)
}

#For all Mutual pairs, sort the values for each key and map each value to , separated values
val Recommend = mp.map(t => (t._1, sortList(t._2.toList))).map(t => t._1.toString + "\t" + t._2.map(x=>x.toString).toArray.mkString(","))


#8941	8943,8944,8940
#9020	9021,9016,9017,9022,317,9023
#9021	9020,9016,9017,9022,317,9023
#9993	9991,13134,13478,13877,34299,34485,34642,37941
