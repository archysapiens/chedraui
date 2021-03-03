package org.btrust.chedrauiHerramienta

/** Funciones referentes a precios de regulación central. */
object Central extends SparkTrait {

  import org.apache.spark.sql.DataFrame
  import org.apache.spark.sql.functions.{date_add, from_unixtime, lit, max, to_date, unix_timestamp, when, first, udf}
  import spark.implicits._
  import org.btrust.chedrauiHerramienta.Files.{arrayFiles}
  import org.btrust.chedrauiHerramienta.Aux.{getMidWeekDate}



  /** Convierte el mes s a inglés. */
  private def english(s: String): String = {
    val mapping = List(("Ene","Jan"),("Abr","Apr"),("Ago","Aug"),("Dic","Dec"))
    def sub(list: List[(String,String)], acc: String): String = {
      if (list.isEmpty) acc
      else {
        list.head match {
          case (pspa,peng) => sub(list.tail, (pspa.r replaceAllIn(acc,peng)).toString)
        }
      }
    }
    sub(mapping,s)
  }
  /** Aisla el mes de un string tipo "...ddMMMyy.csv" para inventarios. */
  private def isolateDate(s: String): String = {
    ("^.+?_(\\d\\d...\\d\\d).csv".r findFirstMatchIn s) match {
      case Some(x) => english(x.group(1))
       case None => ""
     }
  }



  /** Obtiene archivo de inventario más nuevo sin pasarse de una fecha dada. 
    *  
    * @param myDate Fecha ddMMMyy a obtener.
    * @param rootDir Directorio raíz completo de los datos de precios Chedraui.
    * @param uri Para HDFS, URI del namenode del HDFS a utilizar. Por ejemplo, hdfs://T801:54310. 
    * @return Par (fecha,archivo).
    */
  def getInvenFile(myDate: String, rootDir: String, uri: String = ""): (String,String) = {
    import scala.util.matching.Regex
    val ct = arrayFiles(rootDir+"/inventarios/", uri).toSeq
      .map(x => (isolateDate(x),x)).toDF("date","file")
      .withColumn("date", to_date(from_unixtime(unix_timestamp($"date","ddMMMyy"))))
    val cf = ct.filter($"date".leq(lit(myDate)))
    val cm = cf.agg(max("date") as "date").select("date")
    val re = cf.join(cm,Seq("date")).select("date","file").head
    (re.apply(0).toString,re.apply(1).toString)
  }



  /** Lee inventario de un archivo sobre rootDir/inven.
    *  
    * @param rootDir Directorio raíz completo de los datos de precios Chedraui.
    * @param file Nombre del archivo de inventario a leer. 
    * @return DataFrame del imventario. 
    */
  def getInventario(rootDir: String, file: String): DataFrame = {
    spark.read.format("com.databricks.spark.csv")
      .option("header","true").option("inferSchema","true").option("nullValue","NA")
      .option("delimiter","|").option("quote","\"").option("charset","UTF8").option("comment","#")
      .option("timestampFormat", "EEE MMM dd HH:mm:ss zz yyyy")
      .load(rootDir+"/inventarios/"+file)
      .withColumn("FechaInven", to_date($"FechaInven"))
  }



  /** Lee inventario y obtiene precios de regulación central
    * según la fecha dada. Lee los inventarios más nuevos posibles. 
    *  
    * @param myYear Año a regular.
    * @param myWeek Semana a regular.
    * @param rootDir Directorio raíz completo de los datos de precios Chedraui.
    * @param uri Para HDFS, URI del namenode del HDFS a utilizar. Por ejemplo, hdfs://T801:54310.
    * @return DataFrame con columnas 
    * "SKU","Tienda","PrecioCentral","InvFinUni","InvFinVta","InvFinCto"
    */
  def getPRCfromInven(myYear: Int, myWeek: Int, rootDir: String, uri: String): DataFrame = {
    // Fetch from file
    val midWeek  = getMidWeekDate(myYear*100+myWeek)
    val (invDate, invFile) = getInvenFile(midWeek, rootDir, uri)
    System.out.println("[ched] Utilizando "+invFile)
    val inventry = getInventario(rootDir, invFile)
      .withColumn("FechaInven",when($"FechaInven".isNull, invDate).otherwise($"FechaInven"))
    // Filter and return DF
    val invFiltr = inventry.filter($"FechaInven".leq(lit(midWeek)))
    val invGroup = invFiltr.groupBy("SKU","Tienda").agg(max("FechaInven") as "FechaInven")
    val invFinal = invFiltr.join(invGroup, Seq("SKU","Tienda","FechaInven"), "inner")
    // Return value
    invFinal.withColumn("PrecioCentral",$"InvFinVta"/$"InvFinUni")
      .select("SKU","Tienda","PrecioCentral","InvFinUni","InvFinVta","InvFinCto")
  }



  /** Lee inventario y obtiene precios de regulación central
    * sobre un archivo en rootDir/inven.
    *  
    * @param rootDir Directorio raíz completo de los datos de precios Chedraui.
    * @param invFile Nombre del archivo de inventario a leer. 
    * @return DataFrame con columnas 
    * "SKU","Tienda","PrecioCentral","InvFinUni","InvFinVta","InvFinCto".
    */
  def getPRCfromInvenFile(rootDir: String, invFile: String): DataFrame = {
    // Fetch from file
    System.out.println("[ched] Utilizando "+invFile)
    val myDate = udf(() => isolateDate(invFile))
    val inventry = getInventario(rootDir, invFile)
      .withColumn("FechaInven", myDate())
      .withColumn("FechaInven", to_date(from_unixtime(unix_timestamp($"FechaInven","ddMMMyy"))))
      
    // Filter and return DF
    val invFinal = inventry.groupBy("SKU","Tienda").agg(
        first("FechaInven") as "FechaInven", 
        first("InvFinUni") as "InvFinUni",
        first("InvFinVta") as "InvFinVta",
        first("InvFinCto") as "InvFinCto"
      )
    // Return value
    invFinal.withColumn("PrecioCentral",$"InvFinVta"/$"InvFinUni").select("SKU","Tienda","PrecioCentral","InvFinUni","InvFinVta","InvFinCto")
  }




  /** Lee inventario y obtiene precios de regulación central.
    * según la fecha dada. Lee los inventarios más nuevos posibles. 
    * Interfaz de [[getPRCfromInven]].
    *  
    * @param myYear Año a regular.
    * @param myWeek Semana a regular.
    * @param rootDir Directorio raíz completo de los datos de precios Chedraui.
    * @param uri Para HDFS, URI del namenode del HDFS a utilizar. Por ejemplo, hdfs://T801:54310.
    * @return DataFrame con columnas 
    * "SKU","Tienda","PrecioCentral","InvFinUni","InvFinVta","InvFinCto"
    */
  def getPrecioRegulacionCentral(myYear: Int, myWeek: Int, rootDir: String, uri: String =""): DataFrame = {
    getPRCfromInven(myYear, myWeek, rootDir, uri)
  }

}
