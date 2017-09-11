import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

val glassFile = sc.textFile("/user/brk160030/glass.data")
val parsedGlassData = glassFile.map {line=>val part = line.split(',')
  	LabeledPoint(part(10).toDouble, Vectors.dense(part(0).toDouble,part(1).toDouble,part(2).toDouble,part(3).toDouble,part(4).toDouble,part(5).toDouble,part(6).toDouble,part(7).toDouble,part(8).toDouble,part(9).toDouble))
}

val splitData = parsedGlassData.randomSplit(Array(0.6, 0.4), seed = 11L)
val trainingData = splitData(0)
val testData = splitData(1)

val modelData = NaiveBayes.train(trainingData, lambda = 1.0)

val predictionAndLabel = testData.map(p => (modelData.predict(p.features), p.label))
val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / testData.count()
val acc = accuracy*100

println("Accuracy of Naive Bayes Algorithm is -> " + (accuracy*100)+"%")
acc.repartition(1).saveAsTextFile("/user/brk160030/output2NB")

