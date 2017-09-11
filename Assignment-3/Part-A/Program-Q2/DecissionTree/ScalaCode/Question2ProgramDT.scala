import org.apache.spark.SparkContext
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.tree.configuration.Algo._
import org.apache.spark.mllib.tree.impurity.Gini
import org.apache.spark.mllib.util.MLUtils

val glassFile = sc.textFile("/user/brk160030/glass.data")
val parsedGlassData = glassFile.map { line => val part = line.split(',')
LabeledPoint(part(10).toDouble, Vectors.dense(part(0).toDouble,part(1).toDouble,part(2).toDouble,part(3).toDouble,part(4).toDouble,part(5).toDouble,part(6).toDouble,part(7).toDouble,part(8).toDouble,part(9).toDouble))
}

val splitData = parsedGlassData.randomSplit(Array(0.6, 0.4), seed = 11L)
val trainingData = splitData(0)
val testData = splitData(1)


val numOfClasses = 8
val categoricalFeatures = Map[Int, Int]()
val impurity = "gini"
val mDepth = 5
val mBins = 32

val modelData = DecisionTree.trainClassifier(trainingData, numOfClasses, categoricalFeatures, impurity, mDepth, mBins)

val labelAndPreds = testData.map { point =>	
val prediction = modelData.predict(point.features)
(point.label, prediction)
}
val accuracy =  1.0 *labelAndPreds.filter(r => r._1 == r._2).count.toDouble / testData.count
val acc = accuracy*100
println("Accuracy of Decision Tree Algorithm is -> " + (accuracy*100)+"%")
acc.repartition(1).saveAsTextFile("/user/brk160030/output2DT")