// Agarra los datos de la semana 42, y te dice los 2 skus que mas vendieron por decil

import org.tamalytics.chedrauiHerramienta.Aux.loadSchema
val rootDir = "hdfs://T801:54310/Tamalytics/chedraui2/"

val datos = spark.read.format("com.databricks.spark.csv")
  .option("header",true).option("inferSchema",true)
  .option("delimiter","|").option("quote","\"").option("charset","UTF8")
  .load(rootDir+"/merge30Nov2017/datos.csv")
 
val sku = 3019421
val tienda = 174
val datos_ejemplo = datos.filter($"SKU"===sku && $"Tienda"===tienda)


datos_ejemplo.write.format("com.databricks.spark.csv")
  .option("header",true)
  .option("delimiter","|").option("quote","\"").option("charset","UTF8")
  .save(rootDir+"/merge30Nov2017/datosRestringidos.csv")
