// Imports
import org.tamalytics.chedrauiHerramienta.Catalogues.readCatalogues
import org.tamalytics.chedrauiHerramienta.Competition.readCompPrices
import org.tamalytics.chedrauiHerramienta.Calc.{calcIndicadores, calcResumen}
import org.tamalytics.chedrauiHerramienta.Files.{loadFileWithHeader, saveFileWithHeader}
import org.tamalytics.chedrauiHerramienta.Sales.readSales

def roundAt(n: Int)(x: Double): Double = {
  val p = scala.math.pow(10,n)
  scala.math.round(x*p) / p.toDouble
}
def lowRule(x: Double): Double = {
  val x2  = roundAt(2)(x)
  val ent = scala.math.floor(10*x2)
  val frc = scala.math.round(10*(10*x2-ent))
  println("low "+x+" "+ent+" "+frc)
  if (frc < 3) roundAt(1)(ent/10.toDouble)
  else if (frc >= 8) roundAt(1)((ent/10.toDouble)+0.1)
  else roundAt(2)((ent/10.toDouble)+0.05)
}
def highRule(x: Double): Double = {
  val x2  = roundAt(2)(x)
  val ent = scala.math.round(x2)
  val frc = scala.math.round(10*(x2-ent))
  if (frc < 5) ent 
  else ent+1
}
val chedRound = (x: Double) => if (x<100) lowRule(x) else highRule(x)

val udfChedRound = udf(chedRound)


val df = loadFileWithHeader(rootDir+"/resultados/datos_201747.csv")
val df2 = df.withColumn("PrecioSugerido", when($"PrecioCompetenciareciente".isNull, $"PrecioMercadoMedn").otherwise($"PrecioCompetenciaReciente"))
val dfg = df2.drop("GrupoArticulo").join(cats("grupos"), Seq("SKU"), "left")
val agg = dfg.groupBy("Tienda","GrupoArticulo").agg(min("PrecioSugerido") as "PrecioGrupo")
val dft = dfg.join(agg, Seq("Tienda","GrupoArticulo"), "left")
  .withColumn("PrecioSugerido", when($"PrecioSugerido">$"PrecioGrupo" || $"PrecioSugerido".isNull, $"PrecioGrupo").otherwise($"PrecioSugerido"))
  .select("SKU","GrupoArticulo","Tienda","PrecioSugerido")
  .withColumn("PrecioSugerido", udfChedRound($"PrecioSugerido"))

// Defaults
val rootDir = "hdfs://T801:54310/Tamalytics/chedraui2"
val uri = "hdfs://T801:54310/"
val myYear = 2017
val myWeek = 47
val myYearWeek = myYear*100 + myWeek
val fl = (0 to 4).toList.map(w => rootDir+"/resEx/datos_"+w.toString+".csv")

// Read file
val df = fl.map(f => spark.read.format("com.databricks.spark.csv")
      .option("header","true").option("inferSchema","true")
      .option("delimiter","|").option("quote","\"").option("charset","UTF8")
      .load(f)).reduce(_ union _)

// Aggregate
val arr = Array(0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0)
val dft = df.withColumn("PrecioCompetenciaActual", $"VentaCompetenciaActual"/$"VentaUni")
  .withColumn("Q",($"PrecioCompetenciaActual"-$"PrecioCompetenciaMedn")/$"PrecioCompetenciaMedn")
dft.createOrReplaceTempView("dft")

val agg = spark.sql(
  "SELECT Semana, Depto, first(DescripDepto) as DescripDepto, "+
  "SubDepto, first(DescripSubdepto) as DescripSubdepto, "+
  "AVG(Q) as CambioProm, MIN(Q) as CambioMin, MAX(Q) as CambioMax, "+
  "PERCENTILE_APPROX(Q, array(0.0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0)) as percentiles "+
  "FROM dft GROUP BY Semana, Depto, SubDepto "+
  "ORDER BY Semana, Depto, SubDepto")

val desv = agg.withColumn("P00",$"Percentiles".apply(0))
  .withColumn("P10",$"Percentiles".apply(1))
  .withColumn("P20",$"Percentiles".apply(2))
  .withColumn("P80",$"Percentiles".apply(8))
  .withColumn("P90",$"Percentiles".apply(9))
  .drop("Percentiles")

saveFileWithHeader(desv, rootDir+"/resEx/promoTodasSemanas.csv")


dft.groupBy("Depto")
  .agg(avg($"Q") as "Qave", min($"Q") as "Qmin", max($"Q") as "Qmax", Percentile($"Q",arr))
