import java.io.File
import java.io.PrintWriter
import java.io.FileOutputStream
import scala.io.Source
object Sampling {

  def getListOfFiles(directory: String): List[File] = {
    val dir = new File(directory)
    if (dir.exists() && dir.isDirectory) {
      dir.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  def getListOfSubDir(directory: String): List[File] = {
    val dir = new File(directory)
    if (dir.isDirectory && dir.exists()) {
      dir.listFiles().filter(_.isDirectory).toList
    } else {
      List[File]()
    }
  }

  def readFiletoString(datafn: File): String = {
    Source.fromFile(datafn).getLines().mkString
  }

  def writetoCSV(file: File, csvfilename: String,flag:Boolean, stemmer: Stemmer, stopWords: List[String]): Unit = {

    val pw = new PrintWriter(new FileOutputStream(new File(csvfilename),true))
    if(flag){
      pw.println('"'+stemNstopwords(this.readFiletoString(file),stemmer,stopWords)+'"'+",1")
    }else{
      pw.println('"'+stemNstopwords(this.readFiletoString(file),stemmer,stopWords)+'"'+",0")
    }
    pw.close()
  }


  def produceCSV(samplesdir: String,csvfile: String, stemmer: Stemmer, stopWords: List[String]): Unit ={
    this.getListOfSubDir(samplesdir).foreach(d =>if(d.toString.contains("pos")) {
      println("Number of positive reviews: " +this.getListOfFiles(d.toString).size+ ", at directory: "+d.toString)
      println("Writing positive reviews to csv...")
      this.getListOfFiles(d.toString).foreach(f=>this.writetoCSV(f,csvfile,flag=true, stemmer, stopWords))
    }else {
      println("Number of negative reviews: " +this.getListOfFiles(d.toString).size+ ", at directory: "+d.toString)
      println("Writing negative reviews to csv...")
      this.getListOfFiles(d.toString).foreach(f=>this.writetoCSV(f,csvfile,flag=false, stemmer, stopWords))})
  }

  def deletePreviousCSV(csvfilename: String): Unit ={
    new File(csvfilename).delete()
  }

  def lineDistinctWords(line:String):String= {
    var docdistinctWords = scala.collection.mutable.Set.empty[String]
    line.split(" ").foreach(word => docdistinctWords.add(word))
    docdistinctWords.mkString(" ")
  }

  def produceDFFilter(samplesdir: String,minDF:Int=2,maxDF:Int=23000):Array[String]= {

    val nf=this.getListOfSubDir(samplesdir).flatMap(d=>this.getListOfFiles(d.toString)).map(f=>1).sum
    val df = this.getListOfSubDir(samplesdir).flatMap(d=>this.getListOfFiles(d.toString)).
      flatMap(file=>Source.fromFile(file).getLines()).map(line=>this.lineDistinctWords(removePunctuation(line))).
      flatMap(_.split(" ")).foldLeft(Map.empty[String,Int]){
      (count,word)=> count + (word ->(count.getOrElse(word,0) + 1))
    }
    val dfiltered=df.filter(K=>(K._2<minDF)||(K._2>maxDF))
    dfiltered.map(K=>K._1).toArray
  }

  def main(args: Array[String]): Unit = {
    val path= "/home/george/Dropbox/auth/big_data/data/train"
    val path1="samples"

    //println(this.lineDistinctWords("george george hello hello world paper paper"))
    val filterDF = this.produceDFFilter(path)
    println(filterDF.length)
    //this.wordsDFFiltered(path1)
    /*
    val current_features = Source.fromFile(new File("fulldata.csv")).getLines().map(line=>line.split(',')(0))
    val mapfeatures = current_features.flatMap(_.split(" ")).foldLeft(Map.empty[String,Int]){
      (count,word)=> count + (word ->(count.getOrElse(word,0) + 1))
    }

    println(mapfeatures.map(k=>1).sum)
    */

  }



  def stemNstopwords(line: String, stemmer: Stemmer, stopwords: List[String]): String= {
    var cleanline =  line.toLowerCase()
    cleanline = removePunctuation(cleanline)
    cleanline = cleanline.split(" ").filter(word => !stopwords.contains(word)).mkString(" ")
    return stemmer.stemLine(cleanline).filter(w => !stopwords.contains(w)).mkString(" ")
  }


  def removePunctuation(line: String): String ={
    val allowedSpecialChars :List[Char] = List('\'', ' ')
    return line.toCharArray.filter(c => {(allowedSpecialChars.contains(c) || c.isLetter)}).mkString
  }

}