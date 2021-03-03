package org.btrust.chedrauiHerramienta

/** Funciones auxiliares de la herramienta de regulación de precios. */
object Aux extends SparkTrait {

  import org.apache.spark.sql.DataFrame
  import org.apache.spark.sql.types.{DataType, StructType}
  import org.apache.spark.sql.functions.{unix_timestamp, from_unixtime, to_date, date_add, year, weekofyear, udf}

  /** Fecha de mitad de la semana. */
  def getMidWeekDate(yearWeek: Int): String = {
    import spark.implicits._
    val offset = if (yearWeek%100==1) 1 else 0
    Seq(((yearWeek+offset)*10+1).toString).toDF("init")
      .withColumn("ts", unix_timestamp($"init","yyyywwu"))
      .withColumn("date", date_add(to_date(from_unixtime($"ts")),7))
      .withColumn("midWeekDate", date_add($"date",-3-7*offset))
      .filter($"ts".isNotNull)
      .select("midWeekDate").head.apply(0).toString
  }

  /** Substrae weekSub semanas de yearWeek. */
  def subtractWeek(yearWeek: Int, weekSub: Int): Int = {
    import spark.implicits._
    if (weekSub==0) yearWeek 
    else {
      val offset = if (yearWeek%100==1) 1 else 0
      val seq = (1 to 7).flatMap(ii => Seq(((yearWeek+offset)*10+ii).toString))
      val df = seq.toDF("init")
        .withColumn("ts", unix_timestamp($"init","yyyywwu"))
        .withColumn("date", to_date(from_unixtime($"ts")))
        .withColumn("week", weekofyear($"date"))
        .filter($"week"===yearWeek%100+offset)
        .withColumn("subDate", date_add($"date",-7*(weekSub+offset)))
        .withColumn("subYear", org.apache.spark.sql.functions.year($"subDate"))
        .withColumn("subWeek", weekofyear($"subDate"))
        .withColumn("res",$"subYear"*100+$"subWeek")
      val rs = df.filter($"ts".isNotNull)
        .filter($"res".isNotNull)
        .select($"res")
        .collect
        .map(x => x.apply(0).asInstanceOf[Int])
        .groupBy(x => x).mapValues(_.size)
        .maxBy(_._2)._1
      rs
    }
  }

  /** Redondea x a n decimales. */
  private def roundAt(n: Int)(x: Double): Double = {
    val p = scala.math.pow(10,n)
    scala.math.round(x*p) / p.toDouble
  }
  /** Regla de redondeo para x<100. */
  private def lowRule(x: Double): Double = {
    val x2  = roundAt(2)(x)
    val ent = scala.math.floor(10*x2)
    val frc = scala.math.round(10*(10*x2-ent))
    if (frc < 3) roundAt(1)(ent/10.toDouble)
    else if (frc >= 8) roundAt(1)((ent/10.toDouble)+0.1)
    else roundAt(2)((ent/10.toDouble)+0.05)
  }
  /** Regla de redondeo para x>100 */
  private def highRule(x: Double): Double = {
    val x2  = roundAt(2)(x)
    val ent = scala.math.round(x2)
    val frc = scala.math.round(10*(x2-ent))
    if (frc < 5) ent else ent+1
  }
  /** Fórmula de redondeo de Chedraui. */
  val chedRound = (x: Double) => if (x<100) lowRule(x) else highRule(x)
  /** UDF para [[chedRound]]. */
  val udfChedRound = udf(chedRound)

  /** Bandera de remate Walmart, x es remate si acaba en 0.01, 0.02 o 0.03. */
  val banderaWalmart = (x: Double) => x.toString.endsWith(".01") || x.toString.endsWith(".02") || x.toString.endsWith(".03")
  /** UDF para [[banderaWalmart]]. */
  val udfBanderaWalmart = udf(banderaWalmart)

}
