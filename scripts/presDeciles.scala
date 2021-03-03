// Agarra los datos de la semana 42, y te dice los 2 skus que mas vendieron por decil

import org.tamalytics.chedrauiHerramienta.Aux.loadSchema
val rootDir = "hdfs://T801:54310/Tamalytics/chedraui2/"

val schemaDF = spark.read.format("com.databricks.spark.csv")
  .option("header","true").option("inferSchema","true").option("nullValue","NA")
  .option("delimiter","|").option("quote","\"").option("charset","UTF8").option("comment","#")
  .load(rootDir+"/resultados/semanal/sparkheader_201740.csv")
val schema = loadSchema(schemaDF.head.apply(0).asInstanceOf[String]) match {
  case Some(s) => s
  case None => throw new Error("Bad schema")
} 
val datos = spark.read.format("com.databricks.spark.csv")
  .option("header",false).schema(schema)
  .option("delimiter","|").option("quote","\"").option("charset","UTF8")
  .load(rootDir+"/resultados/semanal/datos_201740.csv")
 
val df = datos.select("Tienda","SKU","VentaPesosTotal","decilNacional")
  .filter($"decilNacional".isNotNull)

val skus = df.groupBy("SKU")
  .agg(sum($"VentaPesosTotal") as "Venta", first($"decilNacional") as "Decil")
  .orderBy("Decil","Venta")

val lista = (1 to 5).map(decil => {
    skus.filter($"Decil"===decil)
      .orderBy(desc("Venta"))
      .select("SKU")
      .take(2)
  })

lista.foreach(s => println(s(0)+" "+s(1)))
