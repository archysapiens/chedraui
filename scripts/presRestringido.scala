import org.tamalytics.chedrauiHerramienta.Aux.loadSchema
val rootDir = "hdfs://T801:54310/Tamalytics/chedraui2/"

val datos = spark.read.format("com.databricks.spark.csv")
  .option("header",true).option("inferSchema",true)
  .option("delimiter","|").option("quote","\"").option("charset","UTF8")
  .load(rootDir+"/merge30Nov2017/datos.csv")
 
val skus = Array(3284209,3132396,
  3074417,3019267,
  3350075,3404882,
  3074554,3465137,
  3467127,3345732)

val pares = Array((232,4.3506738442525E8), (234,2.6377094591814488E8), (235,2.568408412261214E8), (239,2.2834907325594315E8), (4,2.184165716600564E8), (59,2.0763344534229982E8), (19,2.0447575664150026E8), (16,1.654279944474852E8), (237,1.653465777421566E8), (241,1.651268695530707E8), (61,1.638613764284137E8), (253,1.623997436578069E8), (20,1.5423999476179546E8), (90,1.5340520991674373E8), (48,1.4961129029452515E8), (47,1.489492007114657E8), (6,1.4894918881030813E8), (24,1.48881425721987E8), (31,1.4377593899254325E8), (231,1.4321530216593432E8))

val venta = pares.map(_._2).foldLeft[Double](0.0)((a,b) => a+b)
val tiendas = pares.map(_._1)

val df_reducido = datos.filter($"SKU".isin(skus:_*))
  .filter($"Tienda".isin(tiendas:_*))

df_reducido.write.format("com.databricks.spark.csv")
  .option("header",true)
  .option("delimiter","|").option("quote","\"").option("charset","UTF8")
  .save(rootDir+"/merge30Nov2017/datosRestringidosB.csv")
