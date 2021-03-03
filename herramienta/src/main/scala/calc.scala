package org.btrust.chedrauiHerramienta

/** Contiene las funciones principales para la regulación de precios de Chedraui. */
object Calc extends SparkTrait {



  import org.apache.spark.sql.{DataFrame, Row}
  import org.apache.spark.sql.types.{DataType, StructType}
  import org.apache.spark.sql.functions.{udf,year,weekofyear,month,dayofmonth,unix_timestamp,col,avg,stddev,when,first,min,max,desc,sum,count}
  import java.util.Calendar
  import java.text.SimpleDateFormat
  import org.btrust.chedrauiHerramienta.Sales.readSales
  import org.btrust.chedrauiHerramienta.Competition.{readCompPrices,readCompPricesFromList}
  import org.btrust.chedrauiHerramienta.Central.{getInvenFile, getInventario, getPrecioRegulacionCentral, getPRCfromInvenFile}
  import org.btrust.chedrauiHerramienta.Files.{saveFileWithHeader,loadFileWithHeader}
  import org.btrust.chedrauiHerramienta.Aux.{subtractWeek,udfChedRound,udfBanderaWalmart}





  /** Se encarga de preparar los datos referentes a los precios de la competencia.
    * Calcula modas y medianas, además sustituye medianas por precios de alguna tienda existente.  
    *
    * @param myYear Año a regular.
    * @param myWeek Semana a regular.
    * @param cats Mapa de catálogos resultantes de leeCatalogos.
    * @param rootDir Directorio raíz completo de los datos de precios Chedraui.
    * @param uri Para HDFS, URI del namenode del HDFS a utilizar. Por ejemplo, hdfs://T801:54310.
    * @param semanaDesde Desde que semana hacia atrás buscar precios.
    * @param semanaHasta Hasta que semana hacia atrás buscar precios.
    * @return Escribe los archivos de la forma compX_YYYYWW.csv
    */
  def calcCompetencia(
    myYear: Int, 
    myWeek: Int, 
    cats: Map[String,DataFrame],
    rootDir: String, 
    uri: String = "",
    semanaDesde: Int = 0,
    semanaHasta: Int = 6
  ): Unit = {

    import spark.implicits._
    val myYearWeek = myYear*100+myWeek

    // Decidiendo orden de semanas
    val (sem1,sem2) = if (semanaDesde<semanaHasta) (semanaDesde,semanaHasta) else (semanaHasta,semanaDesde)

    // Lectura de los precios de la competencia
    System.out.println("[ched] Construccion de precios de la competencia para la semana "+myYearWeek)
    val stores = cats("matriz").select("TiendaCompetencia","FuenteCompetencia").distinct 
    val weeks  = (sem1 to sem2).toList.map(subtractWeek(myYearWeek.toInt,_).toString)
    val preciosCompetenciaFull = readCompPricesFromList(weeks,rootDir,uri)
      .filter($"PrecioCompetencia".isNotNull)
      .join(stores, Seq("TiendaCompetencia","FuenteCompetencia"), "inner")
      .select("Semana","UPC","TiendaCompetencia","PrecioCompetencia","FuenteCompetencia")
    preciosCompetenciaFull.persist()
    val myYearWeek2 = subtractWeek(myYearWeek.toInt,2)
    val preciosCompetencia = preciosCompetenciaFull.filter($"Semana"===myYearWeek2)
    val preciosCompetenciaActual = preciosCompetenciaFull.filter($"Semana"===myYearWeek)
      .withColumnRenamed("PrecioCompetencia", "PrecioCompetenciaActual")

    // Calcula medianas, modas y promedios nacionales para la semana -2
    System.out.println("[ched] Procesando precios de semanas 0 y -2")
    preciosCompetencia.createOrReplaceTempView("Competencia")
    preciosCompetenciaActual.createOrReplaceTempView("CompetenciaActual")
    val preciosMercado_1 = spark.sql("select UPC, "+
      "avg(PrecioCompetencia) as PrecioMercadoProm, "+
      "stddev(PrecioCompetencia) as PrecioMercadoDesv, "+
      "percentile_approx(PrecioCompetencia,0.5) as PrecioMercadoMedn "+
      "from Competencia "+
      "where PrecioCompetencia is not null "+
      "group by UPC")
    val preciosMercado_2 = preciosCompetencia.join(preciosMercado_1.select("UPC","PrecioMercadoMedn"), Seq("UPC"), "inner")
      .filter($"PrecioCompetencia"<=$"PrecioMercadoMedn")
      .groupBy("UPC")
      .agg(max("PrecioCompetencia") as "PrecioMercadoMednTmp")
    val preciosMercado_A = preciosMercado_1.join(preciosMercado_2, Seq("UPC"), "inner")
      .withColumn("PrecioMercadoMedn", $"PrecioMercadoMednTmp")
      .drop("PrecioMercadoMednTmp")
    val preciosMercado_3 = preciosCompetencia.withColumn("PrecioCompetencia",udfChedRound($"PrecioCompetencia"))
      .groupBy("UPC","PrecioCompetencia")
      .agg(count("PrecioCompetencia") as "FrecCompetencia")
    val preciosMercado_4 = preciosMercado_3
      .groupBy("UPC")
      .agg(max("FrecCompetencia") as "FrecCompetencia")
    val preciosMercado_B = preciosMercado_3.join(preciosMercado_4, Seq("UPC","FrecCompetencia"), "inner")
      .groupBy("UPC")
      .agg(first("FrecCompetencia") as "PrecioMercadoFrec", min("PrecioCompetencia") as "PrecioMercadoModa")
    val preciosMercado = preciosMercado_A.join(preciosMercado_B, Seq("UPC"), "inner")
    val preciosMercadoActual_1 = spark.sql("select UPC, "+
      "avg(PrecioCompetenciaActual) as PrecioMercadoActualProm, "+
      "stddev(PrecioCompetenciaActual) as PrecioMercadoActualDesv, "+
      "percentile_approx(PrecioCompetenciaActual,0.5) as PrecioMercadoActualMedn "+
      "from CompetenciaActual "+
      "where PrecioCompetenciaActual is not null "+
      "group by UPC")
    val preciosMercadoActual_2 = preciosCompetenciaActual.join(preciosMercadoActual_1.select("UPC","PrecioMercadoActualMedn"), Seq("UPC"), "inner")
      .filter($"PrecioCompetenciaActual"<=$"PrecioMercadoActualMedn")
      .groupBy("UPC")
      .agg(max("PrecioCompetenciaActual") as "PrecioMercadoActualMednTmp")
    val preciosMercadoActual_A = preciosMercadoActual_1.join(preciosMercadoActual_2, Seq("UPC"), "inner")
      .withColumn("PrecioMercadoActualMedn", $"PrecioMercadoActualMednTmp")
      .drop("PrecioMercadoActualMednTmp")
    val preciosMercadoActual_3 = preciosCompetenciaActual.withColumn("PrecioCompetenciaActual",udfChedRound($"PrecioCompetenciaActual"))
      .groupBy("UPC","PrecioCompetenciaActual")
      .agg(count("PrecioCompetenciaActual") as "FrecCompetenciaActual")
    val preciosMercadoActual_4 = preciosMercadoActual_3
      .groupBy("UPC")
      .agg(max("FrecCompetenciaActual") as "FrecCompetenciaActual")
    val preciosMercadoActual_B = preciosMercadoActual_3.join(preciosMercadoActual_4, Seq("UPC","FrecCompetenciaActual"), "inner")
      .groupBy("UPC")
      .agg(first("FrecCompetenciaActual") as "PrecioMercadoActualFrec", min("PrecioCompetenciaActual") as "PrecioMercadoActualModa")
    val preciosMercadoActual = preciosMercadoActual_A.join(preciosMercadoActual_B, Seq("UPC"), "inner")
    val preciosMercadoM0M2 = preciosMercado.join(preciosMercadoActual, Seq("UPC"), "left")

    // Calcula medidas estadisticas de precios por UPC y Tienda
    System.out.println("[ched] Calculando medidas estadisticas por Tienda y UPC (mediana)")
    preciosCompetenciaFull.createOrReplaceTempView("Full")
    val preciosCompetenciaHist_1 = spark.sql("select UPC, TiendaCompetencia, FuenteCompetencia, "+
      "avg(PrecioCompetencia) as PrecioCompetenciaHistProm, "+
      "stddev(PrecioCompetencia) as PrecioCompetenciaHistDesv, "+
      "min(PrecioCompetencia) as PrecioCompetenciaHistMin, "+
      "max(PrecioCompetencia) as PrecioCompetenciaHistMax, "+
      "percentile_approx(PrecioCompetencia,0.5) as PrecioCompetenciaHistMedn, "+
      "percentile_approx(PrecioCompetencia,0.1) as PrecioCompetenciaHistPr10, "+
      "percentile_approx(PrecioCompetencia,0.2) as PrecioCompetenciaHistPr20, "+
      "percentile_approx(PrecioCompetencia,0.8) as PrecioCompetenciaHistPr80, "+
      "percentile_approx(PrecioCompetencia,0.9) as PrecioCompetenciaHistPr90 "+
      "from Full "+
      "where PrecioCompetencia is not null "+
      "group by UPC, TiendaCompetencia, FuenteCompetencia ")
    val preciosCompetenciaHist_2 = preciosCompetenciaFull
      .join(
        preciosCompetenciaHist_1.select("UPC","TiendaCompetencia","FuenteCompetencia","PrecioCompetenciaHistMedn"), 
        Seq("UPC","TiendaCompetencia","FuenteCompetencia"), 
        "inner"
      ).filter($"PrecioCompetencia"<=$"PrecioCompetenciaHistMedn")
      .groupBy("UPC","TiendaCompetencia","FuenteCompetencia")
      .agg(max("PrecioCompetencia") as "PrecioCompetenciaHistMednTienda")
    val preciosCompetenciaHist = preciosCompetenciaHist_1
      .join(preciosCompetenciaHist_2, Seq("UPC","TiendaCompetencia","FuenteCompetencia"), "inner")
      .withColumn("PrecioCompetenciaHistMedn", $"PrecioCompetenciaHistMednTienda")
      .drop("PrecioMercadoMednTienda","PrecioCompetenciaHistMednTienda")

    // Encuentra precios nacionales mas recientes
    System.out.println("[ched] Encontrando precios nacionales recientes por UPC")
    preciosCompetenciaFull.createOrReplaceTempView("Full")
    val preciosMercadoRcnt_1_Meds = spark.sql("select Semana, UPC, "+
      "avg(PrecioCompetencia) as PrecioMercadoRecienteProm, "+
      "percentile_approx(PrecioCompetencia,0.5) as PrecioMercadoRecienteMedn "+
      "from Full "+
      "where PrecioCompetencia is not null "+
      "group by Semana, UPC")
    val preciosMercadoRcnt_1_Mods_1 = preciosCompetenciaFull.withColumn("PrecioCompetencia",udfChedRound($"PrecioCompetencia"))
      .groupBy("Semana","UPC","PrecioCompetencia")
      .agg(count("PrecioCompetencia") as "FrecCompetencia")
    val preciosMercadoRcnt_1_Mods_2 = preciosMercadoRcnt_1_Mods_1
      .groupBy("Semana","UPC")
      .agg(max("FrecCompetencia") as "FrecCompetencia")
    val preciosMercadoRcnt_1_Mods = preciosMercadoRcnt_1_Mods_1.join(preciosMercadoRcnt_1_Mods_2, Seq("Semana","UPC","FrecCompetencia"), "inner")
      .groupBy("Semana","UPC")
      .agg(first("FrecCompetencia") as "PrecioMercadoRecienteFrec", min("PrecioCompetencia") as "PrecioMercadoRecienteModa")
    val preciosMercadoRcnt_1 = preciosMercadoRcnt_1_Meds.join(preciosMercadoRcnt_1_Mods, Seq("Semana","UPC"), "left")
    val preciosMercadoRcnt_2 = preciosMercadoRcnt_1
      .groupBy("UPC")
      .agg(max($"Semana") as "Semana")
    val preciosMercadoRcnt_3 = preciosMercadoRcnt_1.join(preciosMercadoRcnt_2, Seq("Semana","UPC"), "inner")
    val preciosMercadoRcnt_4 = preciosCompetenciaFull
      .join(
        preciosMercadoRcnt_3.select("Semana","UPC","PrecioMercadoRecienteMedn"), 
        Seq("Semana","UPC"),
        "inner"
      ).filter($"PrecioCompetencia"<=$"PrecioMercadoRecienteMedn")
      .groupBy("Semana","UPC")
      .agg(max("PrecioCompetencia") as "PrecioMercadoRecienteMednTmp")
    val preciosMercadoRcnt = preciosMercadoRcnt_3.join(preciosMercadoRcnt_4, Seq("Semana","UPC"), "inner")
      .withColumn("PrecioMercadoRecienteMedn", $"PrecioMercadoRecienteMednTmp")
      .drop("PrecioMercadoRecienteMednTmp")
      .withColumnRenamed("Semana","SemanaMercadoReciente")

    // Encuentra precios mas recientes
    System.out.println("[ched] Encontrando precios recientes por UPC y Tienda")
    preciosCompetenciaFull.createOrReplaceTempView("Full")
    val preciosCompetenciaRcnt_1 = preciosCompetenciaFull
      .filter($"PrecioCompetencia".isNotNull)
      .select("Semana","UPC","TiendaCompetencia","FuenteCompetencia","PrecioCompetencia")
      .withColumnRenamed("PrecioCompetencia","PrecioCompetenciaReciente")
    val preciosCompetenciaRcnt_2 = preciosCompetenciaRcnt_1
      .groupBy("UPC","TiendaCompetencia","FuenteCompetencia")
      .agg(max("Semana") as "Semana")
    val preciosCompetenciaRcnt = preciosCompetenciaRcnt_1.join(preciosCompetenciaRcnt_2, Seq("UPC","TiendaCompetencia","FuenteCompetencia","Semana"))
      .withColumnRenamed("Semana","SemanaReciente")

    // Guarda resultados
    System.out.println("[ched] Guardando resultados, es posible encontrar Warnings por Memory Leaks no manejados")
    saveFileWithHeader(preciosMercadoM0M2, rootDir+"/resultados/compNacional_"+myYearWeek+".csv")
    saveFileWithHeader(preciosCompetenciaHist, rootDir+"/resultados/compHist_"+myYearWeek+".csv")
    saveFileWithHeader(preciosMercadoRcnt, rootDir+"/resultados/compRcntNacional_"+myYearWeek+".csv")
    saveFileWithHeader(preciosCompetenciaRcnt, rootDir+"/resultados/compRcnt_"+myYearWeek+".csv")

    // Adios
    System.out.println("[ched] Proceso finalizado")
    preciosCompetenciaFull.unpersist()

  }



  /** Se encarga de encontrar los precios de la competencia para SKU y Tiendas.
    *
    * @param myYear Año a regular.
    * @param myWeek Semana a regular.
    * @param cats Mapa de catálogos resultantes de leeCatalogos.
    * @param rootDir Directorio raíz completo de los datos de precios Chedraui.
    * @param uri Para HDFS, URI del namenode del HDFS a utilizar. Por ejemplo, hdfs://T801:54310.
    * @param inven Inventario en rootDir/inven a utilizar, busca el inventario más nuevo por defecto.
    * @return Escribe los archivos de la forma datos_YYYYWW.csv
    */
  def calcDatos(
    myYear: Int, 
    myWeek: Int, 
    cats: Map[String,DataFrame],
    rootDir: String, 
    uri: String = "",
    inven: String = ""
  ): Unit = {

    import spark.implicits._

    // Options and imports
    def fnWeekStr(w: Int): String = if (w>=10) w.toString else "0"+w.toString
    val myYearWeek = myYear*100+myWeek
    System.out.println("[ched] Calculando cantidades para "+myYearWeek.toString)



    // Ventas
    System.out.println("[ched] Leyendo datos de ventas")
    val ventas = if (inven=="") {
      readSales(myYearWeek.toString, rootDir)
      .drop("UPC","Depto","SubDepto","Clase","SubClase")
      .join(cats("skus").select("SKU","UPC","Depto","SubDepto","Clase","SubClase"), Seq("SKU"), "inner")
      .withColumn("PrecioPromedio", when($"VentaUni"===0,null).otherwise($"VentaPesos"/$"VentaUni"))
      .filter($"UPC".isNotNull)
    } else {
      getPRCfromInvenFile(rootDir, inven)
      .select("Tienda","SKU")
      .join(readSales(myYearWeek.toString, rootDir), Seq("Tienda","SKU"), "left")
      .drop("UPC","Depto","SubDepto","Clase","SubClase")
      .join(cats("skus").select("SKU","UPC","Depto","SubDepto","Clase","SubClase"), Seq("SKU"), "inner")
      .filter($"UPC".isNotNull) 
      .withColumn("PrecioPromedio", when($"VentaUni"===0,null).otherwise($"VentaPesos"/$"VentaUni"))
    }
    ventas.persist()

    val ventasEx = ventas.join(cats("tiendas"), Seq("Tienda")).select("SKU","UPC","Formato","PrecioPromedio")
    ventasEx.createOrReplaceTempView("ventasEx")
    val ventasStat = spark.sql("select sku,upc,formato, "+
      "avg(PrecioPromedio) as PrecioPromedioProm,"+
      "stddev(PrecioPromedio) as PrecioPromedioDesv,"+
      "percentile_approx(PrecioPromedio,0.5)  as PrecioPromedioMedn,"+
      "min(PrecioPromedio)  as PrecioPromedioMin,"+
      "percentile_approx(PrecioPromedio,0.05) as PrecioPromedioPr05,"+
      "percentile_approx(PrecioPromedio,0.15) as PrecioPromedioPr10,"+
      "percentile_approx(PrecioPromedio,0.90) as PrecioPromedioPr90,"+
      "percentile_approx(PrecioPromedio,0.95) as PrecioPromedioPr95, "+
      "max(PrecioPromedio)  as PrecioPromedioMax "+
      "from ventasEx "+
      "where PrecioPromedio!=0 and PrecioPromedio is not null "+
      "group by sku,upc,formato ")



    // Precios centrales
    System.out.println("[ched] Calculando precios de regulacion central")
    val preciosCentrales = getPrecioRegulacionCentral(myYear, myWeek, rootDir, uri)



    // Lectura de los precios de la competencia
    System.out.println("[ched] Leyendo precios de la competencia")
    val stores = cats("matriz").select("TiendaCompetencia","FuenteCompetencia").distinct 
    val myYearWeek2 = subtractWeek(myYearWeek.toInt,2)
    val preciosCompetencia = readCompPrices(myYearWeek2.toString, rootDir, uri)
    val preciosCompetenciaActual = readCompPrices(myYearWeek.toString, rootDir, uri)
      .withColumnRenamed("PrecioCompetencia", "PrecioCompetenciaActual")
    // Medidas estadisticas de precios agrupados por UPC y Tienda
    val preciosCompetenciaHist = loadFileWithHeader(rootDir+"/resultados/compHist_"+myYearWeek+".csv")
    // Medianas nacionales por UPC para semanas -0 y -2
    val preciosMercadoM0M2 = loadFileWithHeader(rootDir+"/resultados/compNacional_"+myYearWeek+".csv")
    // Cruces y calculos de precios de la competencia
    val preciosMercadoRcnt = loadFileWithHeader(rootDir+"/resultados/compRcntNacional_"+myYearWeek+".csv")
    val preciosCompetenciaRcnt = loadFileWithHeader(rootDir+"/resultados/compRcnt_"+myYearWeek+".csv")



    // Joining
    System.out.println("[ched] Cruzando tablas")
    val df_matriz = ventas.join(cats("matriz"), Seq("Tienda","Depto","SubDepto"))
    val df_preCen = df_matriz.join(preciosCentrales, Seq("SKU","Tienda"), "left")
    val addP = (p: Int) => udf( (x: Int) => x + p ) 
    val df_preCom = df_preCen.join(preciosCompetencia.drop("Semana") , Seq("UPC","TiendaCompetencia","FuenteCompetencia"), "left")
        .join(preciosCompetenciaActual.drop("Semana"), Seq("UPC","TiendaCompetencia","FuenteCompetencia"), "left")
        .join(preciosCompetenciaRcnt, Seq("UPC","TiendaCompetencia","FuenteCompetencia"), "left")
        .join(preciosMercadoRcnt, Seq("UPC"), "left")
        .withColumn("Prioridad", when($"PrecioCompetencia".isNull        , addP(10000)($"Prioridad")).otherwise($"Prioridad"))
        .withColumn("Prioridad", when($"PrecioCompetenciaActual".isNull  ,  addP(1000)($"Prioridad")).otherwise($"Prioridad"))
        .withColumn("Prioridad", when($"PrecioCompetenciaReciente".isNull,   addP(100)($"Prioridad")).otherwise($"Prioridad"))
    val df_preCom_min = df_preCom.groupBy("Tienda","UPC").agg(min("Prioridad") as "Prioridad")
    val df_priority = df_preCom.join(df_preCom_min, Seq("Tienda","UPC","Prioridad"), "inner")
    val df = df_priority.join(cats("impuestos"), Seq("SKU"), "inner")
      .join(cats("margenes"), Seq("Depto","SubDepto","Clase"), "left")
      .join(cats("promos"), Seq("Depto","SubDepto"), "left")
      .join(preciosCompetenciaHist, Seq("UPC","TiendaCompetencia", "FuenteCompetencia"), "left")
      .join(preciosMercadoM0M2, Seq("UPC"), "left")



    // Desired columns 
    val colNames = Array("SKU","UPC","Tienda","Semana",
      "InvFinUni","InvFinVta","InvFinCto",
      "VentaUni","VentaCosto",
      "VentaPesos","VentaPesosTotal",
      "VentaCentral","VentaCentralTotal",
      "VentaCompetencia","VentaCompetenciaActual",
      "MargenPesosTotal","MargenCentralTotal",
      "MargenCompetencia","MargenCompetenciaActual",
      "PrecioPromedio","PrecioPromedioTotal",
      "PrecioCentral","PrecioCentralTotal",
      "TiendaCompetencia","CadenaCompetencia","FuenteCompetenciaOr","FuenteCompetencia","Prioridad",
      "PrecioCompetencia","PrecioCompetenciaActual",
      "PrecioCompetenciaReciente","SemanaReciente",
      "PrecioCompetenciaHistProm","PrecioCompetenciaHistDesv",
      "PrecioCompetenciaHistMin","PrecioCompetenciaHistMax",
      "PrecioCompetenciaHistMedn","PrecioCompetenciaHistPr10","PrecioCompetenciaHistPr20",
      "PrecioCompetenciaHistPr80","PrecioCompetenciaHistPr90",
      "PrecioMercadoProm","PrecioMercadoDesv","PrecioMercadoMedn","PrecioMercadoModa",
      "PrecioMercadoActualProm","PrecioMercadoActualDesv","PrecioMercadoActualMedn","PrecioMercadoActualModa",
      "SemanaMercadoReciente","PrecioMercadoRecienteProm","PrecioMercadoRecienteMedn","PrecioMercadoRecienteModa",
      "Remate","RemateCompetencia","RemateCompetenciaActual",
      "Promo","PromoCompetencia","PromoCompetenciaActual","PromoCompetenciaReciente",
      "CUP","CUPC","ImpactoChequeoSimulado","ImpactoChequeoReal","CUIC","CUT",
      "DiffVentasCompetenciaActualMPesosTotal","DiffPrecioCompetenciaConMedia",
      "MargenObjetivo")




    // Calculations
    System.out.println("[ched] Calculando dataframe final")
    val udfSemana = udf((x:Int) => (myYear*100+x%100).toInt)
    val rawPromo = (p: Double, r:Double, l: Double) => if (r-p>0 && 100*scala.math.abs(r-p)/r>=l) 1 else 0
    val rawRemate = (p: Double, r:Double) => if (r-p>0 && 100*scala.math.abs(r-p)/r>=60) 1 else 0
    val udfPromo = udf(rawPromo)
    val udfRemate = udf(rawRemate)
    val df_calc = df
      .withColumn("MargenObjetivo", when($"MargenObjetivo".isNull, 0.17).otherwise($"MargenObjetivo"))
      .withColumn("FuenteCompetenciaActual", $"FuenteCompetencia")
      .withColumn("FuenteCompetenciaOr", $"FuenteCompetencia")
      .withColumn("Semana", udfSemana($"Semana"))
      .withColumn("PrecioPromedioTotal", $"PrecioPromedio"+$"PrecioPromedio"*($"IVA"+$"IEPS")/1000.0)
      .withColumn("PrecioCentralTotal", $"PrecioCentral"+$"PrecioCentral"*($"IVA"+$"IEPS")/1000.0)
      .withColumn("FuenteCompetencia", when($"PrecioCompetencia".isNull,"Moda").otherwise($"FuenteCompetencia"))
      .withColumn("PrecioCompetencia", when($"PrecioCompetencia".isNull,"PrecioMercadoModa").otherwise($"PrecioCompetencia"))
      .withColumn("FuenteCompetenciaActual", when($"PrecioCompetenciaActual".isNull,"Moda").otherwise($"FuenteCompetenciaActual"))
      .withColumn("PrecioCompetenciaActual", when($"PrecioCompetenciaActual".isNull,"PrecioMercadoActualModa").otherwise($"PrecioCompetenciaActual")) 
      .withColumn("VentaPesosTotal", $"VentaPesos"+$"VentaPesos"*($"IVA"+$"IEPS")/1000.0)
      .withColumn("VentaCentral", $"PrecioCentral"*$"VentaUni")
      .withColumn("VentaCentralTotal", $"VentaCentral"+$"VentaCentral"*($"IVA"+$"IEPS")/1000.0)
      .withColumn("VentaCompetencia" , $"PrecioCompetencia"*$"VentaUni")
      .withColumn("VentaCompetenciaActual", $"PrecioCompetenciaActual"*$"VentaUni")
      .withColumn("MargenPesos" , $"VentaPesos"-$"VentaCosto")
      .withColumn("MargenPesosTotal" , $"VentaPesosTotal"-$"VentaCosto") 
      .withColumn("MargenCompetencia" , $"VentaCompetencia"-$"VentaCosto")
      .withColumn("MargenCompetenciaActual", $"VentaCompetenciaActual"-$"VentaCosto")
      .withColumn("MargenCentral", $"VentaCentral"-$"VentaCosto")
      .withColumn("MargenCentralTotal" , $"VentaCentralTotal"-$"VentaCosto")
      .withColumn("CUP" ,  $"VentaUni"*($"PrecioCompetencia"-$"PrecioPromedioTotal"))
      .withColumn("CUPC",  $"VentaUni"*($"PrecioCompetencia"-$"PrecioCentralTotal"))
      .withColumn("ImpactoChequeoSimulado", $"PrecioCompetenciaActual"-$"PrecioCompetencia")
      .withColumn("ImpactoChequeoReal", $"PrecioPromedioTotal"-$"PrecioCentralTotal")
      .withColumn("CUIC", $"VentaUni"*($"ImpactoChequeoSimulado"-$"ImpactoChequeoReal"))
      .withColumn("CUT", $"CUPC"+$"CUIC")
      .withColumn("DiffVentasCompetenciaActualMPesosTotal", $"VentaCompetenciaActual"-$"VentaPesosTotal")
      .withColumn("DiffPrecioCompetenciaConMedia", $"PrecioCompetencia"-$"PrecioCompetenciaHistMedn")
      .withColumn("CambioPromocion", when($"CambioPromocion".isNull,20).otherwise($"CambioPromocion"))
      .withColumn("Remate", udfRemate($"PrecioPromedioTotal",$"PrecioCompetenciaHistMedn"))
      .withColumn("RemateCompetencia", udfRemate($"PrecioCompetencia",$"PrecioCompetenciaHistMedn"))
      .withColumn("RemateCompetenciaActual", udfRemate($"PrecioCompetenciaActual",$"PrecioCompetenciaHistMedn"))
      .withColumn("Promo", udfPromo($"PrecioPromedioTotal",$"PrecioCompetenciaHistMedn",$"CambioPromocion"))
      .withColumn("PromoCompetencia", udfPromo($"PrecioCompetencia",$"PrecioCompetenciaHistMedn",$"CambioPromocion"))
      .withColumn("PromoCompetenciaReciente", udfPromo($"PrecioCompetenciaReciente",$"PrecioCompetenciaHistMedn",$"CambioPromocion"))
      .withColumn("PromoCompetenciaReciente", when($"PromoCompetenciaReciente"===0, udfRemate($"PrecioCompetenciaReciente",$"PrecioMercadoRecienteModa")).otherwise($"PromoCompetenciaReciente"))
      .withColumn("PromoCompetenciaActual", udfPromo($"PrecioCompetenciaActual",$"PrecioCompetenciaHistMedn",$"CambioPromocion"))
      .select(colNames.head, colNames.tail:_*)
      


    // Joins
    val df_final = df_calc.join(cats("tiendas"), "Tienda")
      .join(cats("skus").drop("UPC"), Seq("SKU"), "left")
      .join(cats("deciles"), Seq("SKU","Tienda"),"left")
      .join(cats("deciles_nacional").drop("UPC"), Seq("SKU"),"left")
      .join(ventasStat, Seq("SKU","UPC","Formato"),"left")



    // Write
    System.out.println("[ched] Guardando resultados, esto puede tomar unas horas")
    saveFileWithHeader(df_final, rootDir+"/resultados/datos_"+myYearWeek.toString+".csv") 
    ventas.unpersist()



    // Clean up
    System.out.println("[ched] Terminado, se recomienda reiniciar la consola para purgar memoria")

  }



  /** Se encarga de sugerir precios. Implementa la lógica de grupos y excepciones.
    *
    * @param myYear Año a regular.
    * @param myWeek Semana a regular.
    * @param cats Mapa de catálogos resultantes de leeCatalogos.
    * @param rootDir Directorio raíz completo de los datos de precios Chedraui.
    * @param uri Para HDFS, URI del namenode del HDFS a utilizar. Por ejemplo, hdfs://T801:54310.
    * @param remateWalmart Si true, elimina precios marcados como promocionales del grupo Walmart 
    * (ciertas terminaciones de centavos). Es false por defecto.
    * @param ignoraModaNacional Si true, no sugiere precios a raíz de modas nacionales.
    * @param ignoraPrioridades Si true, solamente toma en cuenta la prioridad más alta de precios en la lógica de precios.
    * @param ignoraDeteccionPromos Si true, no deteca precios bajos en grupos de precios.
    * @return Escribe los archivos de la forma preciosX_YYYYWW.csv
    */
  def calcPrecios(
    myYear: Int, 
    myWeek: Int, 
    cats: Map[String,DataFrame], 
    rootDir: String, 
    uri: String = "", 
    remateWalmart: Boolean = false, 
    ignoraModaNacional: Boolean = true, 
    ignoraPrioridades: Boolean = true,
    ignoraDeteccionPromos: Boolean = false
  ): Unit = {

    import spark.implicits._

    // Exceptions
    val grpExpn = cats("excepciones").join(cats("grupos"), Seq("SKU"), "inner")
      .groupBy("GrupoArticulo")
      .agg(
        first("SKU") as "ExcepcionGrupoSKU",
        first("Excepcion") as "ExcepcionGrupo"
      )



    // Calculations
    val myYearWeek = myYear*100+myWeek
    val udfEmpty = udf(() => "")
    val udfMod = udf((x:Int) => (x%100).toInt)
    val wmGroup = Array("WALMART CORPORATION","BODEGA AURRERA")
    System.out.println("[ched] Calculando precios sugeridos para la semana "+myYearWeek)
    val df = loadFileWithHeader(rootDir+"/resultados/datos_"+myYearWeek.toString+".csv")

    // Marcar precios con banderaWalmart como promocionales
    val df2 = df.withColumn("FuenteSugerido", udfEmpty())
      .withColumn("PromoCompetenciaReciente", when(udfBanderaWalmart($"PrecioCompetenciaReciente")===true && $"PrecioCompetenciaReciente">$"PrecioMercadoRecienteModa" && remateWalmart==true && $"CadenaCompetencia".isin(wmGroup:_*), 1).otherwise($"PromoCompetenciaReciente"))
    // Ignorar modas si ignoraModaNacional es true
    val df3 = if (ignoraModaNacional) {
      df2.withColumn("FuenteSugerido", 
        when($"PrecioCompetenciaReciente".isNull, null).otherwise(
          when($"PromoCompetenciaReciente"===1, null).otherwise("PrecioPuntual")
        ))
      .withColumn("PrecioSugerido", 
        when($"PrecioCompetenciaReciente".isNull, null).otherwise(
          when($"PromoCompetenciaReciente"===1, null).otherwise($"PrecioCompetenciaReciente")
        ))
    } else {
      df2.withColumn("FuenteSugerido", 
        when($"PrecioCompetenciaReciente".isNull, "ModaNacional").otherwise(
          when($"PromoCompetenciaReciente"===1, "ModaNacional").otherwise("PrecioPuntual")
        ))
      .withColumn("PrecioSugerido", 
        when($"PrecioCompetenciaReciente".isNull, $"PrecioMercadoRecienteModa").otherwise(
          when($"PromoCompetenciaReciente"===1, $"PrecioMercadoRecienteModa").otherwise($"PrecioCompetenciaReciente")
        ))
    }

    // Marcar precios con Moda como prioridad baja, renombrar variables
    val df4 = df3.withColumn("PrioridadSugerido", when($"FuenteSugerido"==="PrecioPuntual",udfMod($"Prioridad")).otherwise(10))
      .withColumnRenamed("PrecioMercadoRecienteMedn","MedianaNacional")
      .withColumnRenamed("PrecioMercadoRecienteModa","ModaNacional")
      .withColumnRenamed("PrecioCompetenciaHistMedn","MedianaTienda")
      .withColumnRenamed("PrecioCompetenciaReciente","PrecioPuntual")
      .withColumn("FuenteSugerido", when($"PrecioSugerido".isNull,"").otherwise($"FuenteSugerido"))
      .select("SKU","UPC","Depto","SubDepto","Descripcion","Tienda","Nombre_tda",
        "PrecioSugerido","FuenteSugerido","PrioridadSugerido",
        "PrecioPuntual","TiendaCompetencia","CadenaCompetencia","FuenteCompetenciaOr",
        "MedianaTienda","MedianaNacional","ModaNacional")
      .withColumnRenamed("PrioridadSugerido", "Prioridad")
      .withColumnRenamed("FuenteCompetenciaOr", "FuenteCompetencia")

    // Guarda auxiliares
    saveFileWithHeader(df4, rootDir+"/resultados/preciosAux_"+myYearWeek.toString+".csv") 



    // Logica de grupos, cruce de catalogos
    val rawPromo = (p: Double, r:Double, l: Double) => if (r-p>0 && 100*scala.math.abs(r-p)/r>=l) 1 else 0
    val udfPromo = udf(rawPromo)
    val dfg = loadFileWithHeader(rootDir+"/resultados/preciosAux_"+myYearWeek.toString+".csv")
      .drop("GrupoArticulo")
      .join(cats("grupos"), Seq("SKU"), "left")
      .join(cats("promos"), Seq("Depto","SubDepto"), "left")
      .withColumn("CambioPromocion", when($"CambioPromocion".isNull,20).otherwise($"CambioPromocion"))
    dfg.createOrReplaceTempView("dfg")



    // Precios Nacionales de Grupo 
    val grpNacn = dfg.groupBy("GrupoArticulo").agg(min("MedianaNacional") as "MedianaGrupo", min("ModaNacional") as "ModaGrupo")
 

    // Calculo de mediana por Grupo y Tienda, aqui se pueden quitar precios no frontales
    val pri = spark.sql("select Tienda, GrupoArticulo, "+
      "min(Prioridad) as Prioridad "+
      "from dfg "+
      "where (PrecioSugerido is not null and GrupoArticulo is not null) "+
      "group by Tienda, GrupoArticulo ")
    // Quitar prioridades bajas de la tabla de precios
    val dfgPri = {
      if (ignoraPrioridades) dfg.join(pri, Seq("Tienda","GrupoArticulo","Prioridad"), "inner")
      else dfg
    }
    dfgPri.createOrReplaceTempView("dfg")
    // Calculo de Mediana del Grupo
    val mdg = spark.sql("select Tienda, GrupoArticulo, "+
      "percentile_approx(PrecioSugerido, 0.5) as MedianaTiendaGrupo "+
      "from dfg "+
      "where (PrecioSugerido is not null and GrupoArticulo is not null) "+
      "group by Tienda, GrupoArticulo ")
    // Deteccion de precios bajos, calculo de minimo
    val grpPrcs = if (ignoraDeteccionPromos) {
        dfg.join(mdg, Seq("Tienda","GrupoArticulo"), "inner")
          .withColumn("Bajo", udfPromo($"PrecioSugerido",$"MedianaTiendaGrupo",$"CambioPromocion"))
          .groupBy("Tienda","GrupoArticulo").agg(min("PrecioSugerido") as "PrecioGrupo")
      } else {
        dfg.join(mdg, Seq("Tienda","GrupoArticulo"), "inner")
          .withColumn("Bajo", udfPromo($"PrecioSugerido",$"MedianaTiendaGrupo",$"CambioPromocion"))
          .filter($"Bajo"=!=1)
          .groupBy("Tienda","GrupoArticulo").agg(min("PrecioSugerido") as "PrecioGrupo")
      }

    // Rastreo de fuentes de precios de grupo
    val grpData = dfg.join(grpPrcs.withColumnRenamed("PrecioGrupo","PrecioSugerido"), Seq("Tienda","GrupoArticulo","PrecioSugerido"), "inner")
      .withColumnRenamed("Prioridad","PrioridadGrupo")
      .withColumnRenamed("TiendaCompetencia","TiendaGrupo")
      .withColumnRenamed("CadenaCompetencia","CadenaGrupo")
      .withColumnRenamed("FuenteCompetencia","FuenteGrupo")
      .groupBy("Tienda","GrupoArticulo")
      .agg(
        first("SKU") as "SKUGrupo",
        first("PrecioSugerido") as "PrecioGrupo",
        first("PrioridadGrupo") as "PrioridadGrupo",
        first("TiendaGrupo") as "TiendaGrupo",
        first("CadenaGrupo") as "CadenaGrupo",
        first("FuenteGrupo") as "FuenteGrupo"
      )

    // Sugiere precios de Grupo
    val dft = dfg.join(grpPrcs, Seq("Tienda","GrupoArticulo"), "left")
      .join(grpNacn, Seq("GrupoArticulo"), "left")
      .withColumn("FuenteSugerido", 
        when($"GrupoArticulo".isNotNull && ($"PrecioSugerido">$"PrecioGrupo" || $"PrecioSugerido"<$"PrecioGrupo" || $"PrecioSugerido".isNull), "PrecioGrupo").otherwise($"FuenteSugerido")
      )
      .withColumn("PrecioSugerido", 
        when($"GrupoArticulo".isNotNull && ($"PrecioSugerido">$"PrecioGrupo" || $"PrecioSugerido"<$"PrecioGrupo" || $"PrecioSugerido".isNull), $"PrecioGrupo").otherwise($"PrecioSugerido")
      )
    // Ignora precios nacionales si ignoraModaNacional es true
    val dft2 = if (ignoraModaNacional) dft else {
      dft.withColumn("FuenteSugerido", 
        when($"GrupoArticulo".isNotNull && $"PrecioSugerido".isNull, "ModaGrupo").otherwise($"FuenteSugerido")
      )
      .withColumn("PrecioSugerido", 
        when($"GrupoArticulo".isNotNull && $"PrecioSugerido".isNull, $"ModaGrupo").otherwise($"PrecioSugerido")
      )
    }



    // Preparacion de precios finales a guardar
    val dft3 = dft2.withColumn("PrecioSugerido", udfChedRound($"PrecioSugerido"))
      .withColumn("FuenteSugerido", when($"PrecioSugerido".isNull,"").otherwise($"FuenteSugerido"))
      .join(grpData.drop("PrecioGrupo"), Seq("Tienda","GrupoArticulo"), "left")
      .select("SKU","UPC","GrupoArticulo","Tienda",
        "PrecioSugerido","FuenteSugerido","Prioridad",
        "PrecioPuntual","TiendaCompetencia","CadenaCompetencia","FuenteCompetencia",
        "MedianaTienda","MedianaNacional","ModaNacional","PrecioGrupo","MedianaGrupo","ModaGrupo",
        "SKUGrupo","PrioridadGrupo","TiendaGrupo","CadenaGrupo","FuenteGrupo")
      .join(cats("excepciones"), Seq("SKU"), "left")
      .join(grpExpn, Seq("GrupoArticulo"), "left")

    // Persist
    dft3.persist

    // Show results to implement the persist
    System.out.println("[ched] Registros: "+dft3.count)
    dft3.select("Tienda","SKU","PrecioSugerido","FuenteSugerido").filter($"PrecioSugerido".isNotNull).show

    // Save by Exceptions
    System.out.println("[ched] Guardando resultados")
    saveFileWithHeader(dft3.filter($"Excepcion".isNull && $"ExcepcionGrupo".isNull), rootDir+"/resultados/precios_"+myYearWeek.toString+".csv") 
    saveFileWithHeader(dft3.filter($"Excepcion".isNotNull || $"ExcepcionGrupo".isNotNull), rootDir+"/resultados/preciosExcepciones_"+myYearWeek.toString+".csv") 

    // Done
    dft3.unpersist

  }





  /** Se encarga de calcular los impactos de la regulación de precios.
    *
    * @param myYear Año a regular.
    * @param myWeek Semana a regular.
    * @param cats Mapa de catálogos resultantes de leeCatalogos.
    * @param inven Inventario de rootDir/inven a utilizar.
    * @param rootDir Directorio raíz completo de los datos de precios Chedraui.
    * @param uri Para HDFS, URI del namenode del HDFS a utilizar. Por ejemplo, hdfs://T801:54310.
    * @param ventas Archivo de ventas a utilizar para calcular impactos. 
    * Puede tomar un resultado de sumaVentas también.
    * Por defecto busca el archivo de ventas de la semana pasada. 
    * @param ignoraModaNacional Si true, no sugiere precios a raíz de modas nacionales.
    * @return Escribe los archivos de la forma impactoX_YYYYWW.csv
    */
  def calcImpactoPrecios(
    myYear: Int, 
    myWeek: Int, 
    cats: Map[String,DataFrame], 
    inven: String,
    rootDir: String, 
    uri: String = "", 
    ventas: String = "",
    ignoraModaNacional: Boolean = true
  ): Unit = {

    import spark.implicits._

    val myYearWeek = myYear*100+myWeek
    val myYearWeek2 = subtractWeek(myYearWeek,1)

    System.out.println("[ched] Calculando impactos de cambio de precio para la semana "+myYearWeek)

    // Inventory
    val df_inv = getPRCfromInvenFile(rootDir, inven)
      .join(cats("impuestos"),Seq("SKU"),"left")
      .withColumn("PrecioCentral",udfChedRound($"PrecioCentral"))
      .withColumn("PrecioCentralTotal",udfChedRound($"PrecioCentral"+$"PrecioCentral"*($"IVA"+$"IEPS")/1000.0))
      .select("SKU","Tienda","InvFinUni","InvFinVta","PrecioCentralTotal")
      .filter(($"Tienda"<300 || $"Tienda">499) && $"Tienda"=!=686 && $"Tienda"=!=687)
    val df_exc = df_inv.select("SKU").distinct
      .join(cats("grupos"), Seq("SKU"), "inner")
      .join(cats("excepciones"), Seq("SKU"), "inner")
      .filter($"Excepcion".isNotNull)
      .groupBy("GrupoArticulo")
      .agg(
        first($"Excepcion") as "ExcepcionGrupo",
        first($"SKU") as "ExcepcionGrupoSKU"
      )

    // Price database
    val df_prc = loadFileWithHeader(rootDir+"/resultados/precios_"+myYearWeek+".csv")
    .union(loadFileWithHeader(rootDir+"/resultados/preciosExcepciones_"+myYearWeek+".csv"))
    .select("SKU", "UPC", "GrupoArticulo", "Tienda", 
      "PrecioSugerido", "FuenteSugerido", "Prioridad", 
      "PrecioPuntual","TiendaCompetencia","CadenaCompetencia","FuenteCompetencia",
      "MedianaTienda", "MedianaNacional", "ModaNacional", "PrecioGrupo", "MedianaGrupo", "ModaGrupo",
      "SKUGrupo","PrioridadGrupo","TiendaGrupo","CadenaGrupo","FuenteGrupo")
    val df_med = df_prc.filter($"MedianaNacional".isNotNull)
      .groupBy("SKU")
      .agg(
        first($"MedianaNacional") as "ContingenciaMedianaNacional", 
        first($"ModaNacional") as "ContingenciaModaNacional"
      )
    val df_grp = df_prc.filter($"PrecioGrupo".isNotNull)
      .groupBy("GrupoArticulo","Tienda")
      .agg(
        first($"PrecioGrupo") as "ContingenciaPrecioGrupo",
        first($"SKUGrupo") as "ContingenciaSKUGrupo",
        first($"PrioridadGrupo") as "ContingenciaPrioridadGrupo",
        first($"TiendaGrupo") as "ContingenciaTiendaGrupo",
        first($"CadenaGrupo") as "ContingenciaCadenaGrupo",
        first($"FuenteGrupo") as "ContingenciaFuenteGrupo"
      )
    val df_con = df_prc.filter($"MedianaGrupo".isNotNull && $"GrupoArticulo".isNotNull)
      .groupBy("GrupoArticulo")
      .agg(
        first($"MedianaGrupo") as "ContingenciaMedianaGrupo",
        first($"ModaGrupo") as "ContingenciaModaGrupo"
      )


    // Sales table
    val df_vnt = ({
      if (ventas.startsWith("ventas")) {
        System.out.println("[ched] Utilizando ventas"+"\\D".r.replaceAllIn(ventas,"")+".csv")
        readSales("\\D".r.replaceAllIn(ventas,""), rootDir)
      } else if (ventas.startsWith("sumaVentas")) {
        System.out.println("[ched] Utilizando "+ventas)
        loadFileWithHeader(rootDir+"/resultados/"+ventas)
          .withColumnRenamed("TotalVta","VentaPesos")
          .withColumnRenamed("TotalUni","VentaUni")
      } else readSales(myYearWeek2.toString, rootDir)
    }).join(cats("impuestos"),Seq("SKU"),"left")
      .withColumn("PrecioPromedio", when($"VentaUni"<=0 || $"VentaPesos"<=0,null).otherwise($"VentaPesos"/$"VentaUni"))
      .withColumn("PrecioPromedioTotal", udfChedRound($"PrecioPromedio"+$"PrecioPromedio"*($"IVA"+$"IEPS")/1000.0))
      .select("SKU","Tienda","PrecioPromedioTotal")


    // Put it all together
    val joinType = if (ventas.isEmpty) "left" else "full"
    val df = df_inv
      .join(df_vnt, Seq("SKU","Tienda"), joinType)
      .join(cats("grupos"), Seq("SKU"), "left")
      .join(df_prc.drop("GrupoArticulo"), Seq("SKU","Tienda"), "left")
      .join(df_med, Seq("SKU"), "left")
      .join(df_grp, Seq("GrupoArticulo","Tienda"), "left")
      .join(df_con, Seq("GrupoArticulo"), "left")
      .withColumn("MedianaNacional", when($"MedianaNacional".isNull,$"ContingenciaMedianaNacional").otherwise($"MedianaNacional"))
      .withColumn("ModaNacional", when($"ModaNacional".isNull,$"ContingenciaModaNacional").otherwise($"ModaNacional"))
      .withColumn("PrecioGrupo", when($"PrecioGrupo".isNull,$"ContingenciaPrecioGrupo").otherwise($"PrecioGrupo"))
      .withColumn("MedianaGrupo", when($"MedianaGrupo".isNull,$"ContingenciaMedianaGrupo").otherwise($"MedianaGrupo"))
      .withColumn("ModaGrupo", when($"ModaGrupo".isNull,$"ContingenciaModaGrupo").otherwise($"ModaGrupo"))
      .withColumn("SKUGrupo", when($"SKUGrupo".isNull,$"ContingenciaSKUGrupo").otherwise($"SKUGrupo"))
      .withColumn("PrioridadGrupo", when($"PrioridadGrupo".isNull,$"ContingenciaPrioridadGrupo").otherwise($"PrioridadGrupo"))
      .withColumn("TiendaGrupo", when($"TiendaGrupo".isNull,$"ContingenciaTiendaGrupo").otherwise($"TiendaGrupo"))
      .withColumn("CadenaGrupo", when($"CadenaGrupo".isNull,$"ContingenciaCadenaGrupo").otherwise($"CadenaGrupo"))
      .withColumn("FuenteGrupo", when($"FuenteGrupo".isNull,$"ContingenciaFuenteGrupo").otherwise($"FuenteGrupo"))
      .withColumn("FuenteSugerido", when($"PrecioSugerido".isNull, "PrecioGrupo").otherwise($"FuenteSugerido"))
      .withColumn("PrecioSugerido", when($"PrecioSugerido".isNull,$"PrecioGrupo").otherwise($"PrecioSugerido"))
    val df2 = if (ignoraModaNacional) df else {
      df.withColumn("FuenteSugerido", when($"PrecioSugerido".isNull, "ModaGrupo").otherwise($"FuenteSugerido"))
        .withColumn("PrecioSugerido", when($"PrecioSugerido".isNull,$"ModaGrupo").otherwise($"PrecioSugerido"))
        .withColumn("FuenteSugerido", when($"PrecioSugerido".isNull, "ModaNacional").otherwise($"FuenteSugerido"))
        .withColumn("PrecioSugerido", when($"PrecioSugerido".isNull,$"ModaNacional").otherwise($"PrecioSugerido"))
    }
    val df3 = df2.withColumn("FuenteSugerido", when($"PrecioSugerido".isNull,null).otherwise($"FuenteSugerido"))
      .select("SKU","Tienda","InvFinUni","InvFinVta","PrecioCentralTotal","PrecioPromedioTotal","UPC","GrupoArticulo","PrecioSugerido","FuenteSugerido","Prioridad","PrecioPuntual","TiendaCompetencia","CadenaCompetencia","FuenteCompetencia","MedianaTienda","MedianaNacional","ModaNacional","PrecioGrupo","MedianaGrupo","ModaGrupo","SKUGrupo","PrioridadGrupo","TiendaGrupo","CadenaGrupo","FuenteGrupo")
      .withColumn("PrecioSugerido", udfChedRound($"PrecioSugerido"))
      .withColumn("PrecioCentralTotal", udfChedRound($"PrecioCentralTotal"))
      .withColumn("PrecioPromedioTotal", udfChedRound($"PrecioPromedioTotal"))
      .withColumn("ImpactoConCentral", $"InvFinUni"*($"PrecioSugerido"-$"PrecioCentralTotal"))
      .withColumn("ImpactoConPromedio", $"InvFinUni"*($"PrecioSugerido"-$"PrecioPromedioTotal"))

    val df_tot = df3
      .join(df_exc, Seq("GrupoArticulo"), "left")
      .join(cats("excepciones"), Seq("SKU"), "left")
      .join(cats("skus").drop("UPC"), Seq("SKU"), "left")
      .join(cats("deciles_nacional").drop("UPC"), Seq("SKU"),"left")
      .join(cats("tiendas"), Seq("Tienda"), "left")
      .filter($"Excepcion".isNull && $"ExcepcionGrupo".isNull)
      .filter($"Estatus"=!="P" && $"Estatus"=!="AT")

    System.out.println("[ched] Guardando resultado")
    saveFileWithHeader(df_tot, rootDir+"/resultados/impacto_"+myYearWeek.toString+".csv")

    val df_tot2 = loadFileWithHeader(rootDir+"/resultados/impacto_"+myYearWeek.toString+".csv")

    // Skus sin sugerencias de precios
    val nullskus = df_tot2.filter($"PrecioSugerido".isNull).select("SKU").distinct
    val onceskus = df_tot2.join(nullskus, Seq("SKU"), "inner")
      .filter($"PrecioSugerido".isNotNull).select("SKU").distinct
      .withColumn("TodosNulos", when($"SKU".isNotNull,0).otherwise(1))
    val repsskus = nullskus.join(onceskus, Seq("SKU"), "left")
      .withColumn("TodosNulos", when($"TodosNulos".isNull,1).otherwise(0))
      .join(cats("grupos"), Seq("SKU"), "left")
      .join(df_exc, Seq("GrupoArticulo"), "left")
      .join(cats("skus"), Seq("SKU"), "left")

    System.out.println("[ched] Guardando SKUs sin sugerencias de precio")
    saveFileWithHeader(repsskus, rootDir+"/resultados/impactoNulos_"+myYearWeek.toString+".csv")

    // Agregaciones
    val agg_sku = df_tot2.groupBy("SKU","UPC").agg(
        sum($"ImpactoConCentral") as "ImpactoConCentral", 
        sum($"ImpactoConPromedio") as "ImpactoConPromedio"
      ).join(cats("skus").drop("UPC"), Seq("SKU"), "left")
      .join(cats("deciles_nacional").drop("UPC"), Seq("SKU"),"left")
    val agg_sto = df_tot2.groupBy("Tienda").agg(
        sum($"ImpactoConCentral") as "ImpactoConCentral", 
        sum($"ImpactoConPromedio") as "ImpactoConPromedio"
      ).join(cats("tiendas"), Seq("Tienda"), "left")

    System.out.println("[ched] Guardando impactos agregados por SKU y por Tienda")
    saveFileWithHeader(agg_sku, rootDir+"/resultados/impactoSKU_"+myYearWeek.toString+".csv")
    saveFileWithHeader(agg_sto, rootDir+"/resultados/impactoTienda_"+myYearWeek.toString+".csv")


    // Fraccion de skus regulados
    val df_nulls = df_tot2.filter($"PrecioSugerido".isNull)
      .groupBy("Tienda","Depto","SubDepto")
      .agg(count("SKU") as "Nulos")
    val df_found = df_tot2.filter($"PrecioSugerido".isNotNull)
      .groupBy("Tienda","Depto","SubDepto")
      .agg(
        count("SKU") as "NoNulos", 
        sum("ImpactoConCentral") as "ImpactoConCentral",
        sum("ImpactoConPromedio") as "ImpactoConPromedio"
      )
    val udfRound = udf((x:Double) => scala.math.round(100.0*x))
    val df_res = df_found.join(df_nulls, Seq("Tienda","Depto","SubDepto"))
      .withColumn("PorcentajeRegulado", udfRound($"NoNulos"/($"Nulos"+$"NoNulos")))
      .select("Tienda","Depto","SubDepto","NoNulos","Nulos",
        "PorcentajeRegulado","ImpactoConCentral","ImpactoConPromedio")
      .orderBy("Tienda","Depto","SubDepto")

    System.out.println("[ched] Calculando fracciones de SKUs regulados por SubDepto")
    saveFileWithHeader(df_res, rootDir+"/resultados/impactoSubDepto_"+myYearWeek.toString+".csv")

    // Reportes adicionales
    val df_conteos = df_tot2.withColumn("Prioridad", when($"Prioridad".isNull,0).otherwise($"Prioridad"))
      .withColumn("Prioridad", when($"FuenteSugerido"==="PrecioPuntual",$"Prioridad").otherwise(0))
      .groupBy("Tienda","Prioridad","FuenteSugerido")
      .agg(count("SKU") as "Conteo")
      .orderBy("Tienda","Prioridad","FuenteSugerido")
    val df_promos = df_tot2.filter(($"FuenteSugerido"==="ModaNacional" || $"FuenteSugerido"==="MedianaNacional") && $"PrecioPuntual".isNotNull)

    System.out.println("[ched] Guardando reportes adicionales")
    saveFileWithHeader(df_conteos, rootDir+"/resultados/impactoConteos_"+myYearWeek.toString+".csv")
    saveFileWithHeader(df_promos, rootDir+"/resultados/impactoPromos_"+myYearWeek.toString+".csv")

  }





  /** Suma ventas de varias semanas a partir de la semana de regulación. 
    *
    * @param myYear Año a regular.
    * @param myWeek Semana a regular.
    * @param weeks Cuantas semanas hacia atrás tomar en cuenta al sumar ventas.
    * @param cats Mapa de catálogos resultantes de leeCatalogos.
    * @param rootDir Directorio raíz completo de los datos de precios Chedraui.
    * @param uri Para HDFS, URI del namenode del HDFS a utilizar. Por ejemplo, hdfs://T801:54310.
    * @return Escribe los archivos de la forma sumaVentas_YYYYWWaYYYYWW.csv
    */
  def sumaVentas(
    myYear: Int, 
    myWeek: Int, 
    weeks: Int, 
    cats: Map[String,DataFrame],
    rootDir: String, 
    uri: String = ""
  ): Unit = {
    import spark.implicits._

    val myYearWeek = myYear*100+myWeek
    val arrWeeks = (myYearWeek :: (1 to weeks-1).toList.map(x => subtractWeek(myYearWeek.toInt,x))).toArray
    System.out.println("[ched] Sumando ventas de "+arrWeeks(weeks-1)+" a "+arrWeeks(0)+", "+weeks+" semanas en total")
    val ventas = arrWeeks.map(w => readSales(w.toString, rootDir)).reduce(_ union _)
    val agg = ventas.groupBy("SKU","Tienda")
      .agg(sum("VentaPesos") as "TotalVta",sum("VentaUni") as "TotalUni")
      .join(cats("skus").select("SKU","Depto","SubDepto","Clase","SubClase"), Seq("SKU"), "left")

    System.out.println("[ched] Guardando resultados")
    saveFileWithHeader(agg, rootDir+"/resultados/sumaVentas_"+arrWeeks(weeks-1).toString+"a"+myYearWeek.toString+".csv")

  }



  /** Lista los UPCs de la competencia.
    *
    * @param myYear Año a regular.
    * @param myWeek Semana a regular.
    * @param weeks Cuantas semanas hacia atrás tomar en cuenta el listado de UPCs.
    * @param cats Mapa de catálogos resultantes de leeCatalogos.
    * @param rootDir Directorio raíz completo de los datos de precios Chedraui.
    * @param uri Para HDFS, URI del namenode del HDFS a utilizar. Por ejemplo, hdfs://T801:54310.
    * @return Escribe los archivos de la forma listaUPCs_YYYYWW.csv
    */
  def listaUPCsCompetencia(
    myYear: Int, 
    myWeek: Int, 
    weeks: Int, 
    cats: Map[String,DataFrame],
    rootDir: String, 
    uri: String = ""
  ): Unit = {
    import spark.implicits._
    val myYearWeek = myYear*100+myWeek
    val arrWeeks = (myYearWeek :: (1 to weeks-1).toList.map(x => subtractWeek(myYearWeek.toInt,x))).toList
    System.out.println("[ched] Sumando ventas de "+arrWeeks(weeks-1)+" a "+arrWeeks(0)+", "+weeks+" semanas en total")
    val prices = readCompPricesFromList(arrWeeks.map(_.toString), rootDir, uri)
    val agg = prices.select("UPC").distinct
    System.out.println("[ched] Guardando resultados")
    saveFileWithHeader(agg, rootDir+"/resultados/listaUPCs_"+arrWeeks(weeks-1).toString+"a"+myYearWeek.toString+".csv")
  }



}
