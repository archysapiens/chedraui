package org.btrust.chedrauiHerramienta

object Catalogues extends SparkTrait {

  import org.apache.spark.sql.DataFrame
  import org.apache.spark.sql.functions.{udf, first, max, min, col, when}
  import org.btrust.chedrauiHerramienta.Files.arrayFiles

  //val rootDir = "hdfs://This:54310/ChedCatsDec3/"

  def leeCatalogos(rootDir: String, matriz: String = "matriz_competencia.csv"): Map[String,DataFrame] = {
    import spark.implicits._
    // UDFs
    val udfToInt = udf[Int, Double]( _.toInt)
    import scala.util.matching.Regex
    val toDecil: String => Int = (s: String) => {
      ("D(\\d+?)$".r findFirstMatchIn s) match {
        case Some(x) => x.group(1).toInt
        case None => throw new Error("Unexpected format")
      }
    }
    val udfToDecil = udf(toDecil)
    // Reads
    System.out.println("[Catalogos] Tiendas")
    val cat_tiendas = spark.read.format("com.databricks.spark.csv")
      .option("header","true").option("inferSchema","true").option("nullValue","NA")
      .option("delimiter","|").option("quote","\"").option("charset","UTF8").option("comment","#")
      .load(rootDir+"/cat_tiendas.csv") 
      .groupBy("Tienda").agg(
        first("Nombre_tda") as "Nombre_tda", 
        first("Formato") as "Formato", 
        first("Region") as "Region", 
        first("Zona") as "Zona", 
        first("Tipo_Tda") as "Tipo_Tda"
      )
    System.out.println("[Catalogos] SKUs")
    val cat_skus = spark.read.format("com.databricks.spark.csv")
      .option("header","true").option("inferSchema","true").option("nullValue","NA")
      .option("delimiter","|").option("quote","\"").option("charset","UTF8").option("comment","#")
      .load(rootDir+"/cat_skus.csv") 
      .withColumnRenamed("Articulo","SKU")
      .withColumnRenamed("EAN_UPC","UPC")
      .groupBy("SKU").agg(
        first("UPC") as "UPC",
        first("Descripcion") as "Descripcion", 
        first("GrupoArticulos") as "GrupoArticulos",
        first("Estatus") as "Estatus", 
        first("Depto") as "Depto",
        first("DescripDepto") as "DescripDepto",
        first("SubDepto") as "SubDepto",
        first("DescripSubdepto") as "DescripSubdepto",
        first("Clase") as "Clase",
        first("DescripClase") as "DescripClase",
        first("SubClase") as "SubClase",
        first("DescripSubCl") as "DescripSubCl",
        first("Num_Dpt") as "Num_Dpt"
      )
    System.out.println("[Catalogos] Deciles")
    val deciles = spark.read.format("com.databricks.spark.csv")
      .option("header","true").option("inferSchema","true").option("nullValue","NA")
      .option("delimiter","|").option("quote","\"").option("charset","UTF8").option("comment","#")
      .load(rootDir+"/cat_deciles.csv") 
    val decilesRecientes = deciles.groupBy("SKU","Tienda").agg(max("InicioVigencia") as "InicioVigencia")
    val cat_deciles = deciles.join(decilesRecientes, Seq("SKU","Tienda","InicioVigencia"))
      .drop("InicioVigencia").drop("FinVigencia").withColumnRenamed("Decil","DecilTienda")
    System.out.println("[Catalogos] Deciles Nacionales")
    val cat_deciles_nacional = spark.read.format("com.databricks.spark.csv")
      .option("header","true").option("inferSchema","true").option("nullValue","NA")
      .option("delimiter","|").option("quote","\"").option("charset","UTF8").option("comment","#")
      .load(rootDir+"/cat_deciles_nacional.csv") 
      .select("SKU","UPC","Decil")
      .withColumn("DecilNacional",udfToDecil(col("Decil")))
      .select("SKU","UPC","DecilNacional")
      .groupBy("SKU","UPC").agg(min("DecilNacional") as "DecilNacional")
    System.out.println("[Catalogos] Impuestos")
    val cat_impuestos = spark.read.format("com.databricks.spark.csv")
      .option("header","true").option("inferSchema","true").option("nullValue","NA")
      .option("delimiter","|").option("quote","\"").option("charset","UTF8").option("comment","#")
      .load(rootDir+"/cat_impuestos.csv") 
      .withColumnRenamed("Articulo","SKU")
      .groupBy("SKU").agg(first("IVA") as "IVA", first("IEPS") as "IEPS")
      .withColumn("IVA", when($"IVA".isNull,0.0).otherwise($"IVA"))
      .withColumn("IEPS", when($"IEPS".isNull,0.0).otherwise($"IEPS"))

    System.out.println("[Catalogos] Matriz")
    val cat_matriz = spark.read.format("com.databricks.spark.csv")
      .option("header","true").option("inferSchema","true").option("nullValue","NA")
      .option("delimiter","|").option("quote","\"").option("charset","UTF8").option("comment","#")
      .load(rootDir+"/"+matriz)
      .withColumnRenamed("Centro","Tienda")
      .filter($"Tienda".isNotNull && $"TiendaCompetencia".isNotNull)
      .groupBy("Tienda","Prioridad","Depto","SubDepto").agg(
        first("TiendaCompetencia") as "TiendaCompetencia",
        first("FuenteCompetencia") as "FuenteCompetencia",
        first("CadenaCompetencia") as "CadenaCompetencia"
      )
      .orderBy("Tienda","Depto","SubDepto","Prioridad")

    System.out.println("[Catalogos] Cotas de Promociones")
    val cat_promos = spark.read.format("com.databricks.spark.csv")
      .option("header","true").option("inferSchema","true")
      .option("delimiter","|").option("quote","\"").option("charset","UTF8")
      .load(rootDir+"/cat_promos.csv")
      .groupBy("Depto","SubDepto").agg(first("CambioPromocion") as "CambioPromocion")

    System.out.println("[Catalogos] Margenes (catalogo dummy)")
    val cat_margenes = Seq.empty[(Int, Int, Int, Double)]
      .toDF("Depto","SubDepto","Clase","MargenObjetivo")

    System.out.println("[Catalogos] Grupos")
    val cat_grupos = spark.read.format("com.databricks.spark.csv")
      .option("header","true").option("inferSchema","true").option("nullValue","")
      .option("delimiter","|").option("quote","\"").option("charset","UTF8").option("comment","#")
      .load(rootDir+"/cat_grupos.csv")
      .withColumnRenamed("Articulo","SKU")
      .groupBy("SKU").agg(first("GrupoArticulo") as "GrupoArticulo")

    System.out.println("[Catalogos] Excepciones")
    val cat_excepciones = spark.read.format("com.databricks.spark.csv")
      .option("header","true").option("inferSchema","true").option("nullValue","")
      .option("delimiter","|").option("quote","\"").option("charset","UTF8").option("comment","#")
      .load(rootDir+"/cat_excepciones.csv")
      .withColumnRenamed("Articulo","SKU")
      .groupBy("SKU").agg(first("Excepcion") as "Excepcion")

    Map(
      "tiendas"->cat_tiendas,
      "skus"->cat_skus,
      "deciles"->cat_deciles,
      "deciles_nacional"->cat_deciles_nacional,
      "impuestos"->cat_impuestos,
      "matriz"->cat_matriz,
      "promos"->cat_promos,
      "margenes"->cat_margenes,
      "grupos"->cat_grupos,
      "excepciones"->cat_excepciones
    )

  }



  /** Obtiene las tienas únicas de la competencia.
    *
    * @param prefix Nombre de la fuente de precios, 
    * es el prefijo de preciosCompetencia/prefixYYYYWW.csv
    * @param rootDir Directorio raíz completo de los datos de precios Chedraui.
    * @param uri Para HDFS, URI del namenode del HDFS a utilizar. Por ejemplo, hdfs://T801:54310.
    * @return Escribe los archivos de la forma prefixNombresNielsen.csv
    */
  def buildCompStoreCatalogue(prefix: String, rootDir: String, uri: String = ""): Unit = {
    val files = arrayFiles(rootDir+"/preciosCompetencia/",uri).filter(_.startsWith(prefix)).zipWithIndex
    def uniqueStores(pair: (String, Int)): DataFrame = pair match {
      case (file, index) => {
        System.out.println("[ched] File "+(index+1).toString+"/"+files.size)
        spark.read.format("com.databricks.spark.csv")
          .option("header","true").option("inferSchema","true").option("nullValue","NA")
          .option("delimiter","|").option("quote","\"").option("charset","UTF8").option("comment","#")
          .load(rootDir+"/preciosCompetencia/"+file)
          .withColumnRenamed("Articulo","SKU")
          .withColumnRenamed("Tienda","compId")
          .withColumnRenamed("Precio","PrecioCompetencia")
          .select("compId")
      }
    }
  }

}
