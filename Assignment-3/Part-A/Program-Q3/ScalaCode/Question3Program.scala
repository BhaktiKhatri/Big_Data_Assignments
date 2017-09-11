import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.rdd

val ratingsFile = sc.textFile("/user/brk160030/ratings.dat") 
val ratings = ratingsFile.map(_.split("::") match { case Array(user,movieid, rate,timestamp) => Rating(user.toInt,movieid.toInt, rate.toDouble)})

val rank = 10
val numIterations = 10
val splitData = ratings.randomSplit(Array(0.6, 0.4))
val trainingData = splitData(0)
val testData = splitData(1)
val model = ALS.train(trainingData, rank, numIterations)

val usersProducts = testData.map { case Rating(user, product, rate) => (user, product) }	
val predictions=model.predict(usersProducts).map { case Rating(user, product, rate) => ((user, product), rate)}
val ratesAndPreds = ratings.map { case Rating(user, product, rate) => ((user, product), rate)}.join(predictions)

val MSE = ratesAndPreds.map { case ((user, product), (rt1, rt2)) =>
      val error = (rt1 - rt2)
      error * error
    }.mean()
    println("Mean Squared Error = " + MSE)
    
val RMSE = math.sqrt(MSE)
println("Root Mean Squared Error = " + RMSE)

val accuracy=RMSE
accuracy.repartition(1).saveAsTextFile("/user/brk160030/output3")
println("Accuracy of ALS -> " +accuracy)