import org.apache.spark.mllib.clustering.{KMeans,KMeansModel}
import org.apache.spark.mllib.linalg.Vectors

val itemData=sc.textFile("/user/brk160030/itemusermat")
val splitData=itemData.map(line=>Vectors.dense(line.split(' ').drop(1).map(_.toDouble))).cache()

val numOfClusters=10
val numOfIterations=20
val clusterData=KMeans.train(splitData, numOfClusters, numOfIterations)

val predictData=itemData.map{line=>val part=line.split(' ')	
(part(0),clusterData.predict(Vectors.dense(part.tail.map(_.toDouble))))
}

val movieFile=sc.textFile("/user/brk160030/movies.dat")
val moviesData=movieFile.map{line=>val part=line.split("::")
(part(0),(part(1)+" , "+part(2)))
}

val finalData=predictData.join(moviesData)
val arrangedData=finalData.map(p=>(p._2._1,(p._1,p._2._2)))
val groupedData=arrangedData.groupByKey()
val answer=groupedData.map(p=>(p._1,p._2.toList))
val finalAnswer=answer.map(p=>(p._1,p._2.take(5)))
val ans=finalAnswer.sortByKey(true).map(line=>line._1+"\t"+line._2)
ans.collect.foreach(p=>println("Cluster: "+p._1+" -> "+p._2.mkString(";;")))
//finalAnswer.collect.foreach(p=>println("Cluster: "+p._1+" -> "+p._2.mkString(";;")))
ans.repartition(1).saveAsTextFile("/user/brk160030/output1")
