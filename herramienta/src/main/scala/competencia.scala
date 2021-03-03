package org.btrust.chedrauiHerramienta

/** Contiene las funiones referentes a lectura de precios de competidores. */
object Competition extends SparkTrait {



  import org.apache.spark.sql.DataFrame
  import org.apache.spark.sql.types.{StructType,StructField,IntegerType, DoubleType, StringType}
  import org.apache.spark.sql.functions.udf
  import org.btrust.chedrauiHerramienta.Files.{arrayPrefixes}



  /** Lee los precios de la competencia para una semana.
    *
    * @param weekSuffix Año+Semana en formato YYYYWW a extraer de precios de competencia.
    * @param rootDir Directorio raíz completo de los datos de precios Chedraui.
    * @param uri Para HDFS, URI del namenode del HDFS a utilizar. Por ejemplo, hdfs://T801:54310.
    * @param columnSuffix Sufijo a añadir a las columnas de Precio, Tienda y Fuente 
    * del DataFrame resultante.
    * @return DataFrame con columnas 
    * "Semana","UPC","PrecioCompetencia","TiendaCompetencia","FuenteCompetencia". 
    * Las columnas de Precio, Tienda y Fuente pueden tener un sufijo 
    * especificado por columnSuffix. 
    */
  def readCompPrices(weekSuffix: String, rootDir: String, uri: String = "", columnSuffix: String = ""): DataFrame = {
    import spark.implicits._
    import org.apache.spark.sql.types.{DoubleType,LongType,IntegerType}
    // Column names
    val Precio = "PrecioCompetencia"+columnSuffix
    val Tienda = "TiendaCompetencia"+columnSuffix
    val Fuente = "FuenteCompetencia"+columnSuffix
    // Read dataframe
    def readDF(source: String): DataFrame = {
      // UDFs
      val udfSource = udf(() => source)
      val udfWeek = udf(() => weekSuffix.toInt)
      // Read from file
      System.out.println("[comp] Leyendo "+source+weekSuffix+".csv")
      spark.read.format("com.databricks.spark.csv")
        .option("header","true").option("inferSchema","true").option("nullValue","NA")
        .option("delimiter","|").option("quote","\"").option("charset","UTF8").option("comment","#")
        .load(rootDir+"/preciosCompetencia/"+source+weekSuffix+".csv")
        .withColumnRenamed("shop","Tienda").withColumn("Tienda", $"Tienda".cast(DoubleType).cast(IntegerType))
        .withColumnRenamed("barcode","UPC").withColumn("UPC", $"UPC".cast(DoubleType).cast(LongType))
        .withColumnRenamed("pr","Precio").withColumn("Precio", $"Precio".cast(DoubleType))          .withColumn("Fuente",udfSource())
        .withColumn("Semana",udfWeek())
        .withColumnRenamed("Precio",Precio)
        .withColumnRenamed("Tienda",Tienda)
        .withColumnRenamed("Fuente",Fuente)
        .select("Semana","UPC",Tienda,Precio,Fuente)
    }
    // Read the dataframes
    val empty = Seq.empty[(Int, Long, Long, Double, String)]
      .toDF("Semana","UPC",Tienda,Precio,Fuente)
    val names = arrayPrefixes(weekSuffix, rootDir+"/preciosCompetencia", uri)
    def mergeDFs(list: List[DataFrame]) = {
      def iter(ls: List[DataFrame], acc: DataFrame): DataFrame = {
        if (ls.isEmpty) acc
        else iter(ls.tail, acc.union(ls.head))
      }
      iter(list, empty) 
    }
    mergeDFs(names.map(readDF).toList)
  }



  /** Lee los precios de la competencia para varias semanas.
    *
    * @param weeks Lista de Años+Semanas en formato YYYYWW a extraer de precios de competencia.
    * @param rootDir Directorio raíz completo de los datos de precios Chedraui.
    * @param uri Para HDFS, URI del namenode del HDFS a utilizar. Por ejemplo, hdfs://T801:54310.
    * @param columnSuffix Sufijo a añadir a las columnas de Precio, Tienda y Fuente 
    * del DataFrame resultante.
    * @return DataFrame con columnas 
    * "Semana","UPC","PrecioCompetencia","TiendaCompetencia","FuenteCompetencia". 
    * Las columnas de Precio, Tienda y Fuente pueden tener un sufijo 
    * especificado por columnSuffix. 
    */
  def readCompPricesFromList(weeks: List[String], rootDir: String, uri: String = "", columnSuffix: String = ""): DataFrame = {
    val dfList = weeks.map(w => readCompPrices(w,rootDir,uri,columnSuffix))
    dfList.reduceLeft(_ union _)
  }



}
