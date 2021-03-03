// Imports
import org.tamalytics.chedrauiHerramienta.Catalogues.readCatalogues
import org.tamalytics.chedrauiHerramienta.Competition.readCompPrices
import org.tamalytics.chedrauiHerramienta.Calc.{calcIndicadores, calcResumen}
import org.tamalytics.chedrauiHerramienta.Files.{loadFileWithHeader, saveFileWithHeader}
import org.tamalytics.chedrauiHerramienta.Sales.readSales

// Defaults
val rootDir = "hdfs://T801:54310/Tamalytics/chedraui2"
val uri = "hdfs://T801:54310/"
val myYear = 2017
val myWeek = 0
val myYearWeek = myYear*100 + myWeek
val fl = (40 to 47).toList.map(w => rootDir+"/resultados/datos_"+(myYear*100+w).toString+".csv")

// Read file
val df = fl.map(f => loadFileWithHeader(f)).reduce(_ union _)
  
/*  fl.map(f => spark.read.format("com.databricks.spark.csv")
      .option("header","true").option("inferSchema","true")
      .option("delimiter","|").option("quote","\"").option("charset","UTF8")
      .load(f)).reduce(_ union _)
*/

// Deteccion


// Aggregate
df.createOrReplaceTempView("df")
val agg = df.groupBy("Semana", "Region", "Depto", "SubDepto", "DecilNacional", "Remate", "RemateCompetenciaActual", "Promo","PromoCompetenciaActual")
  .agg( 
    first($"DescripDepto") as "DescripDepto",
    first($"DescripSubDepto") as "DescripSubDepto",
    sum($"VentaPesos") as "sumVentaPesos",
    sum($"VentaPesosTotal") as "sumVentaPesosTotal",
    sum($"VentaCompetencia") as "sumVentaCompetencia",
    sum($"VentaCompetenciaActual") as "sumVentaCompetenciaActual",
    sum($"DiffVentasCompetenciaActualMPesosTotal") as "sumVentasCompetenciaActualMPesosTotal",
    sum($"VentaUni") as "sumVentaUni",
    countDistinct($"UPC") as "NoArticulos",
    countDistinct($"Tienda") as "NoTiendas"
  ).orderBy("Semana","Region","Depto","SubDepto","DecilNacional","Remate","RemateCompetenciaActual","Promo","PromoCompetenciaActual")

// Save 
saveFileWithHeader(agg, rootDir+"/resEx/agregacionDic.csv")


agg.groupBy("Remate","RemateCompetenciaActual","Promo","PromoCompetenciaActual")
  .agg(sum("sumVentasCompetenciaActualMPesosTotal") as "sum")
  .orderBy("Remate","RemateCompetenciaActual","Promo","PromoCompetenciaActual")
  .show

agg.groupBy("Promo","PromoCompetenciaActual")
  .agg(sum("sumVentasCompetenciaActualMPesosTotal")/1e6 as "sum")
  .orderBy("Promo","PromoCompetenciaActual")
  .show
