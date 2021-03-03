package org.btrust.chedrauiHerramienta
/** Trait necesario para Spark en las librerías. */
trait SparkTrait {
  import org.apache.log4j.Logger
  import org.apache.log4j.Level
  import org.apache.spark.sql.SparkSession
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  /** Spark. */
  implicit val spark = SparkSession.builder().getOrCreate()
}
