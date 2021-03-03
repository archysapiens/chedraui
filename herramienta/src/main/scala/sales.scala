package org.btrust.chedrauiHerramienta

/** Contiene funciones referentes a lecturas de ventas de Chedraui. */
object Sales extends SparkTrait {

  import org.apache.spark.sql.{DataFrame,AnalysisException}
  import spark.implicits._

// SKU|EAN_UPC|Descripcion|Prov|RazonSocial|TipoArticulo|GrupoArticulos|Depto|SubDepto|Clase|SubClase|CategoriaArticulo|Estatus|GrupoCompras|Semana|Semana_nombre|Tienda|VentaUni|VentaPesos|VentaCosto|Invfinvta|FP|CAN_AP|DiasInvHist

  /** Lee las ventas del archivo rootDir/ventas/ventasYYYYWW.csv 
    *
    * @param myYearWeek Año+Semana del archivo de ventas a leer. 
    * @param rootDir Directorio raíz de los datos de precios Chedraui.
    * @param raw Si true selecciona todos los campos del DataFrame de ventas, si es false 
    * selecciona solamente los campos utilizados por la herramienta. Es false por defecto. 
    * @return DataFrame con columnas "SKU","UPC","Depto","SubDepto","Clase","SubClase",
    * "Semana","Tienda","VentaUni","VentaPesos","VentaCosto" si raw es false, las columnas de ventasYYYYWW.csv si raw es true.
    */
  def readSales(myYearWeek: String, rootDir: String, raw: Boolean = false): DataFrame = {
    import org.apache.spark.sql.functions.udf
    val year = (((myYearWeek.toInt) - (myYearWeek.toInt%100))/100).toInt
    val udfWeek = udf((s: Int) => (year*100+s%100).toInt)
    val df = (try { 
        spark.read.format("com.databricks.spark.csv")
          .option("header","true").option("inferSchema","true").option("nullValue","NA")
          .option("delimiter","|").option("quote","\"").option("charset","UTF8").option("comment","#")
          .load(rootDir+"/ventas/ventas"+myYearWeek+".csv")
          .withColumn("Semana", udfWeek($"Semana"))
          .withColumnRenamed("EAN_UPC","UPC")
      } catch {
        case e: AnalysisException => {
          Seq.empty[(Int, Int, Long, Int, Int, Int, Int, Int, Double, Double, Double, Double)]
            .toDF("Semana","SKU","UPC","Depto","SubDepto","Clase","SubClase","Tienda","VentaUni","VentaPesos","VentaCosto","Invfinvta")
        }
      }).select("Semana","SKU","UPC","Depto","SubDepto","Clase","SubClase","Tienda","VentaUni","VentaPesos","VentaCosto","Invfinvta")
    if (raw) df else {
      df.withColumnRenamed("EAN_UPC","UPC")
        .select("SKU","UPC","Depto","SubDepto","Clase","SubClase","Semana","Tienda","VentaUni","VentaPesos","VentaCosto")
    }
  }

}
