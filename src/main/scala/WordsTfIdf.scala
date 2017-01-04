

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.ml.feature.IDF
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.ml.feature.Tokenizer



object WordsTfIdf {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ML Auth App").setMaster("local[1]")
    val sc = new SparkContext(conf)
    val currentDir = System.getProperty("user.dir")
    println(currentDir)
    val inputFile = "file://"+currentDir+"/sampleData.csv"
    println(inputFile)
    val trainData = sc.textFile(inputFile,2)
    trainData.foreach(line =>println(line))
    val seqdata = Seq(trainData.foreach(line=>line.split(',')))
    /*
    val sentenceData = spark.createDataFrame(Seq(
      (0, "Hi I heard about Spark"),
      (0, "I wish Java could use case classes"),
      (1, "Logistic regression models are neat")
    )).toDF("label", "sentence")

    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val wordsData = tokenizer.transform(sentenceData)
    val hashingTF = new HashingTF()
      .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)
    val featurizedData = hashingTF.transform(wordsData)
    // alternatively, CountVectorizer can also be used to get term frequency vectors

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)
    val rescaledData = idfModel.transform(featurizedData)
    rescaledData.select("features", "label").take(3).foreach(println)
    */




  }

}
