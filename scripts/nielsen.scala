// Imports 
import org.tamalytics.chedrauiHerramienta.Catalogues.{readCatalogues,buildCompStoreCatalogue}
import org.tamalytics.chedrauiHerramienta.Competition.readCompPrices
import org.tamalytics.chedrauiHerramienta.Calc.{calcIndicadores, generateHeader, calcResumen}
import org.tamalytics.chedrauiHerramienta.Aux.loadSchema

// Some useful defaults
val rootDir = "hdfs://T801:54310/Tamalytics/chedrauiMargenes"
val uri = "hdfs://T801:54310/"

// Setup variables
val cats  = readCatalogues(matriz="matriz_competencia21112017.csv")

// Build catalogue for ba_
buildCompStoreCatalogue("ba_",rootDir,uri)

// Build catalogue for walmart_
buildCompStoreCatalogue("walmart_",rootDir,uri)

// Read the frames
val df_ba = spark.read.format("com.databricks.spark.csv")
      .option("header","true").option("inferSchema","true")
      .option("delimiter","|").option("quote","\"")
      .load(rootDir+"/resultados/ba_NombresNielsen.csv")
val df_walmart = spark.read.format("com.databricks.spark.csv")
      .option("header","true").option("inferSchema","true")
      .option("delimiter","|").option("quote","\"")
      .load(rootDir+"/resultados/walmart_NombresNielsen.csv")

// Union
val df_nielsen = (df_ba union df_walmart).withColumn("competencia_nielsen2",$"competencia_nielsen")

// Join
val joined = cats("cat_matriz").join(df_nielsen, Seq("competencia_nielsen"), "left")
  .withColumnRenamed("competencia_nielsen" ,"competencia_nielsen_matriz")
  .withColumnRenamed("competencia_nielsen2","competencia_nielsen_precios")
  .select("Tienda","competencia_nielsen_matriz","competencia_nielsen_precios")

// Save it
joined.write.format("com.databricks.spark.csv")
  .option("header","true").option("delimiter","|")
  .save(rootDir+"/resultados/cruceMatrizNielsen.csv")

