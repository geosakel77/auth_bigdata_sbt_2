/**
  * Created by george on 19/1/2017.
  */

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.feature.IDF
import org.apache.spark.mllib.feature.Normalizer
import org.apache.spark.rdd.RDD

object WordsTFIDFRDD {

  def tfidfrdd(sc: SparkContext,inputFile:String,splitRate:Array[Double],norm:Boolean=true): Array[RDD[LabeledPoint]] = {
    val hashingTF = new HashingTF()

    val trainData = sc.textFile(inputFile).map(line =>LabeledPoint.apply(line.split(",")(1).toDouble,hashingTF.transform(line.split(",")(0).replace("\"","").split(" ")))).cache()
    val hashedTrainingData = trainData.map(_.features)
    val idfModel = new IDF().fit(hashedTrainingData)
    val idf = idfModel.transform(hashedTrainingData)
    val labeledVectors = if (norm==true){
      val normalizer = new Normalizer()
      idf.zip(trainData).map(x=>LabeledPoint(x._2.label,normalizer.transform(x._1))).randomSplit(splitRate)
    }else{
      idf.zip(trainData).map(x=>LabeledPoint(x._2.label,x._1)).randomSplit(splitRate)
    }
    labeledVectors
  }

  def words2vecrdd():Unit={




  }


  def main(args: Array[String]): Unit = {
    var csvfilename: String = ""
    if (args.length != 0) {
      csvfilename = args(0)
      if (!(csvfilename == "--help")) {

        val conf = new SparkConf().setAppName("ML Auth App").setMaster("local[1]")
        val sc = new SparkContext(conf)
        val currentDir = System.getProperty("user.dir")
        println(currentDir)
        val inputFile = "file://" + currentDir + "/" + csvfilename
        println(inputFile)
        val Array(trainData,testData)=this.tfidfrdd(sc,inputFile,Array(0.6,0.4))
        trainData.foreach(f=>println(f.label+":"+f.features))

      } else {
        println("scala WordsTfIdf.scala csv_file_name")
      }

    } else {
      println("Please provide file path.")

    }
  }

}
