# -*- coding: utf-8 -*-
import math
from pyspark import SparkConf, SparkContext, SQLContext

points = [
    (1, 2),
    (2, 1),
    (3, 1),
    (5, 4),
    (5, 5),
    (6, 5),
    (10, 8),
    (7, 9),
    (11, 5),
    (14, 9),
    (14, 14),
    ]
def kmeans(k):
    conf = SparkConf().setMaster("local").setAppName("kmeans_APP")
    sc = SparkContext(conf=conf)
    #簇心点
    centerPoints = []
    for i in xrange(0,k):
        centerPoints.append((i,points[i]))
    count_inter = 0
    pointsRDD = sc.parallelize(points)
    pointsRDD.cache()
    while count_inter <5:
        clusterRDD = pointsRDD.map(lambda x:makeCluster(x,centerPoints))
        centerPoints = clusterRDD.reduceByKey(lambda x,y:((x[0]+y[0])/2,(x[1]+y[1])/2)).collect()
        count_inter = count_inter +1
    print clusterRDD.collect()
    print ''
    sc.stop()


def makeCluster(point,centerPoints):
    dis_min = distance(point,centerPoints[0][1])
    clusterID = 0
    for i in xrange(1,len(centerPoints)):
        dis = distance(point,centerPoints[i][1])
        if dis<dis_min:
            dis_min = dis
            clusterID = centerPoints[i][0]
    return (clusterID,point)

def distance(a,b):
    dis = (a[0]-b[0])*(a[0]-b[0])+(a[1]-b[1])*(a[1]-b[1])
    return math.sqrt(dis)

if __name__ == "__main__":
    kmeans(3)
