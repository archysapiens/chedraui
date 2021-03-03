package org.btrust.chedrauiHerramienta

/** Contiene funciones de manipulación de archivos. */
object Files extends SparkTrait {

  import org.apache.spark.sql.{DataFrame, Row}
  import org.apache.spark.sql.types.{DataType, StructType}
  import scala.util.matching.Regex
  import scala.util.Try
  import java.net.URI 
  import org.apache.hadoop.fs.Path
  import org.apache.hadoop.fs.FileSystem 
  import org.apache.hadoop.conf.Configuration

 

  /** Listado de archivos en un path. */
  def arrayFiles(path: String, uri: String = ""): Array[String] = {
    val conf = new Configuration()
    val fs = {
      if (uri.isEmpty) FileSystem.get(conf)
      else FileSystem.get(new URI(uri),conf)
    }
    val status = fs.listStatus(new Path(path))
    status.filter(_.isFile).map(_.getPath.getName)
  }

  /** Borra archivo. */
  def deleteFile(path: String, uri: String = ""): Unit = {
    val conf = new Configuration()
    val fs = {
      if (uri.isEmpty) FileSystem.get(conf)
      else FileSystem.get(new URI(uri),conf)
    }
    val exists = fs.exists(new Path(path))
    if (exists) {
      System.out.println("[File] Archivo "+path+" existe, borrando para sobreescribir")
      fs.delete(new Path(path), true)
    } else {
      System.out.println("[File] Archivo "+path+" no existe, escribiendo")
    }
  }

  // List all unique file prefixes in a path from files of the form <prefix><suffix>.csv
  // Take these files 
  // ba201740.csv 
  // ba201739.csv
  // ba201738.csv 
  // walmart201740.csv
  // wamlart201738.csv 
  // For suffix=40 => Array(ba, walmart)
  // For suffix=39 => Array(ba)
  /** Lista todos los sufijos de archivos en un directorio para archivos de la forma <prefix><suffix>.csv. */
  def arrayPrefixes(suffix: String, path: String, uri: String = ""): Array[String] = {
    def isolatePrefix(s: String): String = {
      (("(.+?)"+suffix+".csv").r findFirstMatchIn s) match {
        case Some(x) => x.group(1)
        case None => ""
      }
    }
    arrayFiles(path, uri).map(isolatePrefix).filter(!_.isEmpty).distinct
  }

  // List all unique file suffixes in a path from files of the form <prefix><suffix>.csv
  // Take these files 
  // ba201740.csv 
  // ba201739.csv
  // ba201738.csv 
  // walmart201740.csv
  // wamlart201738.csv 
  // For prefix=ba => Array(201738, 201739, 201740)
  // For prefix=walmart => Array(201738, 201740)
  /** Lista todos los prefijos de archivos en un directorio para archivos de la forma <prefix><suffix>.csv. */
  def arraySuffixes(prefix: String, path: String, uri: String = ""): Array[String] = {
    def isolateSuffix(s: String): String = {
      ((prefix+"(.+?)"+".csv").r findFirstMatchIn s) match {
        case Some(x) => x.group(1)
        case None => ""
      }
    }
    arrayFiles(path, uri).map(isolateSuffix).filter(!_.isEmpty).distinct
  }

  /** Lee las cabeceras Spark en formato JSON y regresa un StructType. */
  def loadSchema(s: String): Option[StructType] = {
    Try(DataType.fromJson(s)).toOption.flatMap {
      case s: StructType => Some(s)
      case _ => None 
    }
  }

  /** Guarda DataFrame en tres archivos file. 
    * El archivo "file" contiene el DataFrame sin cabeceras, 
    * el archivo "file.sparkheader" contiene las cabeceras de Spark y
    * el archivo "file.plainheader" conetiene las cabeceras en texto plano. 
    */
  def saveFileWithHeader(df: DataFrame, file: String, sep: String = "|"): Unit = {
    import spark.implicits._
    val uri = "^.+?://[^//]+?/".r.findFirstMatchIn(file).mkString
    val isHDFS = uri.startsWith("hdfs://")
    val df_schema = Seq(df.schema.json).toDF("schema")
    def concat(a: String, b: String): String = if (a.isEmpty) b else a+sep+b
    val df_plain = Seq(df.schema.map(_.name).foldLeft("")(concat)).toDF("schema")
    if (!isHDFS) {
      deleteFile(file, "")
      deleteFile(file+".sparkheader", "")
      deleteFile(file+".plainheader", "")
    }
    df.write.format("com.databricks.spark.csv")
      .option("header", "false").option("delimiter",sep)
      .save(file)
    df_schema.write.format("com.databricks.spark.csv")
      .option("header","true").option("delimiter",sep)
      .save(file+".sparkheader")
    df_plain.write.format("com.databricks.spark.csv")
      .option("header","false").option("delimiter",",")
      .save(file+".plainheader")
  }

  /** Lee archivo junto con su cabecera sparkheader. */
  def loadFileWithHeader(file: String, sep: String = "|"): DataFrame = {
    val uri = "^.+?://[^//]+?/".r.findFirstMatchIn(file).mkString
    val isHDFS = uri.startsWith("hdfs://")
    val schemaDF = spark.read.format("com.databricks.spark.csv")
      .option("header","true").option("inferSchema","true")
      .option("delimiter",sep).option("quote","\"").option("charset","UTF8")
      .load(file+".sparkheader")
    val schema = loadSchema(schemaDF.head.apply(0).asInstanceOf[String]) match {
      case Some(s) => s
      case None => throw new Error("Bad schema")
    }
    spark.read.format("com.databricks.spark.csv")
        .option("header","false").schema(schema)
        .option("delimiter",sep).option("quote","\"").option("charset","UTF8")
        .load(file)
  }

  /** Genera archivo que contiene las cabeceras de un DataFrame de Spark. */
  def generateHeader(file: String, sep: String = "|"): Unit = {
    import spark.implicits._
    val df = loadFileWithHeader(file, sep)
    def concat(a: String, b: String): String = if (a.isEmpty) b else a+sep+b
    val df_plain = Seq(df.schema.map(_.name).foldLeft("")(concat)).toDF("schema") 
    df_plain.write.format("com.databricks.spark.csv")
      .option("header","false").option("delimiter",",")
      .save(file+".plainheader") 
  }



}
