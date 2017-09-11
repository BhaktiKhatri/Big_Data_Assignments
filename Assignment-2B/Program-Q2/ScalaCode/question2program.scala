val reviewFile=sc.textFile("/user/brk160030/review.csv") 
val splitReviewData=reviewFile.map(line=>line.split("\\::")).map(line=>(line(2),line(3).toDouble))
val avgData=splitReviewData.map{case(k,v)=>(k,(v,1))}.reduceByKey{case((val1,cnt1),(val2,cnt2))=>(val1+val2,cnt1+cnt2)}.mapValues{case(valuei,counti)=>valuei.toDouble/counti.toDouble}
val topData=avgData.takeOrdered(10)(Ordering[Double].reverse.on(line=>line._2))
val tenData=sc.parallelize(topData)  
val businessFile=sc.textFile("/user/brk160030/business.csv")
val splitBusinessData=businessFile.map(line=>line.split("\\::")).map(line=>(line(0),line(1),line(2)))
val businessDataFrame=splitBusinessData.toDF("businessid","fulladdress","categories")
val reviewDataFrame=tenData.toDF("businessid","stars")
val output=reviewDataFrame.join(businessDataFrame,"businessid").distinct().select("businessid","fulladdress","categories","stars").map(line=>line(0)+"\t"+line(1)+"\t\t\t\t"+line(2)+"\t\t\t"+line(3))
output.collect.foreach(println)
output.repartition(1).saveAsTextFile("/user/brk160030/output2")
