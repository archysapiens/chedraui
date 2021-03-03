// Agarra los datos de la semana 42, y te dice los 2 skus que mas vendieron por decil

import org.tamalytics.chedrauiHerramienta.Aux.loadSchema
val rootDir = "hdfs://T801:54310/Tamalytics/chedraui2/"

val datos = spark.read.format("com.databricks.spark.csv")
  .option("header",true).option("inferSchema",true)
  .option("delimiter","|").option("quote","\"").option("charset","UTF8")
  .load(rootDir+"/merge30Nov2017/datos.csv")

val df = datos.groupBy("Tienda")
  .agg(first($"Competencia") as "Competencia",
    sum($"VentaPesosTotal") as "VentaTotal")
  .orderBy("Competencia")
df.persist

df.show


datos_ejemplo.write.format("com.databricks.spark.csv")
  .option("header",true)
  .option("delimiter","|").option("quote","\"").option("charset","UTF8")
  .save(rootDir+"/merge30Nov2017/datosRestringidos.csv")
