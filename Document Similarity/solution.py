import os
import re
import random
import time
import binascii
import csv
import numpy as np
import sys
from scipy.spatial import distance
from operator import *

list1=[]

with open("/home/ak6755/1B3/Articles.csv", "rU") as csvfile:
 spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
 for row in spamreader:
  print("---")
  print ''.join(row[0])
  list1.append(''.join(row[0]))
  print("---")


a=sc.parallelize(list1)

y=a.zipWithIndex().map(lambda (x, y): (y,x))

numDocs = y.count()

#method to create singles
def shingles(x,n):
	return [x[i:i + n] for i in range(len(x) - n + 1)]

#method to hash each shingle value
def hashS(x):
	return [binascii.crc32(str(x)) & 0xffffffff]

docsAsShingleSets = {};

for j in range(1, numDocs):
 shinglesInDoc = set()
 p=y.lookup(j)
 a=sc.parallelize(p)
 print(j)
 b=a.flatMap(lambda x: shingles(x,5))
 x=b.zipWithIndex().map(lambda (x, y): (y,x))
 numShingles = x.count()
 print(numShingles)
 h= b.flatMap(lambda x: hashS(x))
 docsAsShingleSets[j] = h.collect()
 

#Now shingles are created, lets do minhash
maxShingleID = 100000
nextPrime = 100003
#method to create random coefficients a and b
def pickRandomCoeffs(k):
  randList = []
  
  while k > 0:
    randIndex = random.randint(0, maxShingleID) 
  
    while randIndex in randList:
      randIndex = random.randint(0, maxShingleID) 
    
    # Add the random number to the list.
    randList.append(randIndex)
    k = k - 1
    
  return randList

numHashes=100;
# For each of the 'numHashes' hash functions, generate a different coefficient 'a' and 'b'.   
coeffA = pickRandomCoeffs(numHashes)
coeffB = pickRandomCoeffs(numHashes)

signatures = []
for docID in range(1, numDocs):
 signature = []
 shingleIDSet = docsAsShingleSets[docID]
 #print(docID)
 for i in range(0, numHashes):
  #print("---------------------------"+str(i))
  minHashCode = nextPrime + 1
  for shingleID in shingleIDSet:
   #print("------"+str(shingleID))
   hashCode = (coeffA[i] * shingleID + coeffB[i]) % nextPrime
   if hashCode < minHashCode:
    minHashCode = hashCode
  signature.append(minHashCode)
 signatures.append(signature)


#print the signatures generated for each document
signatures

#function to randomly assign k sentroids
def initialize_centroids(centroids, k):
    np.random.shuffle(centroids)
    return centroids[:k]

#function to find the euclidean distance
def closestPoint(p, centers):
    bestIndex = 0
    closest = float("+inf")
    for i in range(len(centers)):
        tempDist =  distance.euclidean(p, centers[i])
        #tempDist = np.sum((p - centers[i]) ** 2)
        print(tempDist)
        if tempDist < closest:
            closest = tempDist
            bestIndex = i
    return bestIndex


#Kmeans method
def kMean(signatures, k):
 kPoints = initialize_centroids(signatures, k)
 kPoints = initialize_centroids(signatures, k)
 #kPoints
 sigRDD = sc.parallelize(signatures)
 for i in range(1, 500):
  closest = sigRDD.map(lambda p: (closestPoint(p, kPoints), (p, 1))) 
  pointStats = closest.reduceByKey(lambda p1_c1, p2_c2: (map(add,p1_c1[0] , p2_c2[0]), p1_c1[1] + p2_c2[1]))
  newPoints = pointStats.map(lambda st: (st[0], [x/st[1][1] for x in st[1][0]])).collect()
  for (iK, p) in newPoints:
   kPoints[iK] = p
  return kPoints

#This piece of code will run Kmeans for k=1 to 10 and then calculate the sse and store it for each K in s
s=[]
for k in range(1,11):
 print k
 kp = kMean(signatures, k)
 sigRDD = sc.parallelize(signatures)
 p=sigRDD.zipWithIndex().map(lambda (x, y): (y,x))
 c = p.map(lambda p: (closestPoint(p[1], kp), p[1]))
 x=c.map(lambda e : [e[0],e[1]])
 #elements in each cluster
 n=x.groupByKey().map(lambda x : [x[0], list(x[1])])
 z= n.map(lambda x : [x[0], [(distance.euclidean(kp[x[0]],x[1][y]) ** 2) for y in range(0,len(x[1]))]])
 eSum = z.map(lambda a: sum(a[1]))
 s.append(sum(eSum.collect()))

#s has all the values of SSE(error) for each k. I have plotted the elbow curve using this.
s

#########Get Clusters for K=3#################################################
kp = kMean(signatures, 3)
sigRDD = sc.parallelize(signatures)
p=sigRDD.zipWithIndex().map(lambda (x, y): (y,x))
c = p.map(lambda p: (closestPoint(p[1], kp), p[0]))
x=c.map(lambda e : [e[0],e[1]])
#elements in each cluster
n=x.groupByKey().map(lambda x : [x[0], list(x[1])])
n.collect()
##############################End of required code##############################
