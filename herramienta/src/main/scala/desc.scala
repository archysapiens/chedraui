package org.btrust.chedrauiHerramienta

/** Contiene funciones auxiliares para análisis descriptivo. */
object Desc extends SparkTrait {



  import org.apache.spark.sql.{DataFrame, Row}
  import org.apache.spark.sql.types.{DataType, StructType}
  import org.apache.spark.sql.functions.{udf,year,weekofyear,month,dayofmonth,unix_timestamp,col,avg,stddev,count,countDistinct,sum,when,min,max,collect_list}
  import java.util.Calendar
  import java.text.SimpleDateFormat
  import org.btrust.chedrauiHerramienta.Sales.readSales
  import org.btrust.chedrauiHerramienta.Competition.{readCompPrices,readCompPricesFromList}
  import org.btrust.chedrauiHerramienta.Central.{getInvenFile, getInventario, getPrecioRegulacionCentral}
  import org.btrust.chedrauiHerramienta.Files.{saveFileWithHeader,loadFileWithHeader}
  import org.btrust.chedrauiHerramienta.Aux.subtractWeek



  /** Descripción pendiente. */
  def calcDescriptivo(myYear: Int, myWeek: Int, cats: Map[String,DataFrame], rootDir: String): Unit = {
    import spark.implicits._

    // Week select
    def fnWeekStr(w: Int): String = if (w>=10) w.toString else "0"+w.toString
    val myYearWeek = myYear*100+myWeek
    System.out.println("[ched] Performing calculations for "+myYearWeek.toString)

    // Ventas, articles with VentaUni = 0.0 bring in noise, delete later?
    System.out.println("[ched] Reading sales data")
    val cat_impuestos = cats("cat_impuestos")
    val cat_tiendas = cats("cat_tiendas")
    val cat_skus = cats("cat_skus")
    val ventas = readSales(myYearWeek.toString, rootDir).filter($"UPC".isNotNull)
    val ventasEx = ventas
      .join(cat_impuestos, Seq("SKU"))
      .withColumn("Precio", when($"VentaUni"===0,null).otherwise($"VentaPesos"/$"VentaUni"))

      .withColumn("VentaPesosTotal", $"VentaPesos"+$"VentaPesos"*(($"IVA"+$"IEPS")/1000.0))
      .withColumn("PrecioTotal", $"Precio"+$"Precio"*(($"IVA"+$"IEPS")/1000.0))

    // Count
    val dfCounts = ventas.groupBy("Semana")
      .agg(countDistinct($"Tienda") as "NoTiendas", countDistinct($"UPC") as "NoArticulos")

    // Store grouping
    val dfStore = ventasEx.groupBy("Tienda","Semana")
      .agg(
        sum($"VentaPesos") as "VentaPesos",
        sum($"VentaPesosTotal") as "VentaPesosTotal", 
        count($"UPC") as "NoArticulos"
      ).join(cat_tiendas, Seq("Tienda"), "left")

    // SKUs 
    val udfS = udf((a: Seq[String]) => a.mkString(":"))
    val dfItemEx = ventasEx.groupBy("SKU","UPC","Semana")
      .agg(
        sum($"VentaPesos") as "VentaPesos",
        sum($"VentaPesosTotal") as "VentaPesosTotal",
        count($"Tienda") as "NoTiendas",
        avg($"PrecioTotal") as "PrecioPromedio",
        min($"PrecioTotal") as "PrecioMinimo",
        max($"PrecioTotal") as "PrecioMaximo",
        stddev($"PrecioTotal") as "PrecioDesvStd"
      ).withColumn("PrecioDesvStd", when($"PrecioDesvStd".isNaN,0.0).otherwise($"PrecioDesvStd"))
    val dfItemMin = ventasEx.join(dfItemEx, Seq("SKU","UPC","Semana"))
      .filter($"PrecioMinimo"===$"PrecioTotal")
      .select("SKU","UPC","Semana","PrecioTotal","Tienda")
      .groupBy("SKU","UPC","Semana")
      .agg(collect_list($"Tienda") as "TiendasMin")
      .withColumn("TiendasMin", udfS($"TiendasMin"))
    val dfItemMax = ventasEx.join(dfItemEx, Seq("SKU","UPC","Semana"))
      .filter($"PrecioMaximo"===$"PrecioTotal")
      .select("SKU","UPC","Semana","PrecioTotal","Tienda")
      .groupBy("SKU","UPC","Semana")
      .agg(collect_list($"Tienda") as "TiendasMax")
      .withColumn("TiendasMax", udfS($"TiendasMax"))
    val dfItem = dfItemEx
      .join(dfItemMin, Seq("SKU","UPC","Semana"), "left")
      .join(dfItemMax, Seq("SKU","UPC","Semana"), "left")
      .join(cat_skus, Seq("SKU","UPC"), "left")

    // Save results
    System.out.println("[ched] Reading sales data")
    saveFileWithHeader(dfCounts, rootDir+"/resultados/descConteos_"+myYearWeek.toString+".csv")
    saveFileWithHeader(dfStore, rootDir+"/resultados/descTiendas_"+myYearWeek.toString+".csv")
    saveFileWithHeader(dfItem, rootDir+"/resultados/descArticulos_"+myYearWeek.toString+".csv")

  }

}
