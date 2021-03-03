import org.btrust.chedrauiHerramienta.Catalogues.leeCatalogos
import org.btrust.chedrauiHerramienta.Sales.readSales
import org.btrust.chedrauiHerramienta.Files.{saveFileWithHeader,loadFileWithHeader}
import org.btrust.chedrauiHerramienta.Aux.subtractWeek

val rootDir = "hdfs://T801:54310/Tamalytics/chedraui2"
val uri = "hdfs://T801:54310/"
val myYearWeek = 201751
val weeks = 12

val cats  = leeCatalogos(rootDir,matriz="matriz_11Dic2017.csv")



// Part 1
val arrWeeks = (myYearWeek :: (1 to weeks-1).toList.map(x => subtractWeek(myYearWeek.toInt,x))).toArray

val ventas = arrWeeks.map(w => readSales(w.toString, rootDir)).reduce(_ union _)
val agg = ventas.groupBy("SKU","Tienda")
  .agg(
    sum("VentaPesos") as "TotalVta",
    sum("VentaUni") as "TotalUni"
  ).join(
    cats("skus").select("SKU","Depto","SubDepto","Clase","SubClase"),
    Seq("SKU"),
    "left"
  )
saveFileWithHeader(agg, rootDir+"/resultados/sumaVentas_"+myYearWeek.toString+".csv")



// Part 2
val agg2 = loadFileWithHeader(rootDir+"/resultados/decilAux_"+myYearWeek.toString+".csv")
  .filter($"TotalVta">0.0)
agg2.createOrReplaceTempView("agg2")

def queryByLvl(lvl: Int): String = {
  val master = Seq("Tienda","Depto","SubDepto","Clase","SubClase")
  val groups = master.take(1+lvl)
  val prGrp = groups.last
  val stGrp = groups.mkString(",")
  val vta = (1 to 9).map(x => f"percentile_approx(TotalVta,0.${x}) as DecVta${prGrp}${x}")
  val uni = (1 to 9).map(x => f"percentile_approx(TotalUni,0.${x}) as DecUni${prGrp}${x}")
  f"select ${stGrp},"+
  vta.mkString(",")+","+
  uni.mkString(",")+" "+
  f"from agg2 group by ${stGrp}"
}

val df_depto    = spark.sql(queryByLvl(1))
val df_subdepto = spark.sql(queryByLvl(2))
val df_clase    = spark.sql(queryByLvl(3))
val df_subclase = spark.sql(queryByLvl(4))

val decideDecil = 
  (vc: Double, v1: Double, v2: Double, v3: Double, v4: Double, 
   v5: Double, v6: Double, v7: Double, v8: Double, v9: Double) => {
     if (vc >= v9) 1
     else if (vc < v9 && vc >= v8) 2
     else if (vc < v8 && vc >= v7) 3
     else if (vc < v7 && vc >= v6) 4
     else if (vc < v6 && vc >= v5) 5
     else if (vc < v5 && vc >= v4) 6
     else if (vc < v4 && vc >= v3) 7
     else if (vc < v3 && vc >= v2) 8
     else if (vc < v2 && vc >= v1) 9
     else 10
  }
val udfDecideDecil = udf(decideDecil)

val df = agg2
  .join(df_depto   , Seq("Tienda","Depto"))
  .join(df_subdepto, Seq("Tienda","Depto","SubDepto"))
  .join(df_clase   , Seq("Tienda","Depto","SubDepto","Clase"))
  .join(df_subclase, Seq("Tienda","Depto","SubDepto","Clase","SubClase"))
  .withColumn("DecilVtaTiendaDepto",udfDecideDecil($"TotalVta",$"DecVtaDepto1",$"DecVtaDepto2",$"DecVtaDepto3",$"DecVtaDepto4",$"DecVtaDepto5",$"DecVtaDepto6",$"DecVtaDepto7",$"DecVtaDepto8",$"DecVtaDepto9"))
  .withColumn("DecilVtaTiendaSubDepto",udfDecideDecil($"TotalVta",$"DecVtaSubDepto1",$"DecVtaSubDepto2",$"DecVtaSubDepto3",$"DecVtaSubDepto4",$"DecVtaSubDepto5",$"DecVtaSubDepto6",$"DecVtaSubDepto7",$"DecVtaSubDepto8",$"DecVtaSubDepto9"))
  .withColumn("DecilVtaTiendaClase",udfDecideDecil($"TotalVta",$"DecVtaClase1",$"DecVtaClase2",$"DecVtaClase3",$"DecVtaClase4",$"DecVtaClase5",$"DecVtaClase6",$"DecVtaClase7",$"DecVtaClase8",$"DecVtaClase9"))
  .withColumn("DecilVtaTiendaSubClase",udfDecideDecil($"TotalVta",$"DecVtaSubClase1",$"DecVtaSubClase2",$"DecVtaSubClase3",$"DecVtaSubClase4",$"DecVtaSubClase5",$"DecVtaSubClase6",$"DecVtaSubClase7",$"DecVtaSubClase8",$"DecVtaSubClase9"))
  .withColumn("DecilUniTiendaDepto",udfDecideDecil($"TotalUni",$"DecUniDepto1",$"DecUniDepto2",$"DecUniDepto3",$"DecUniDepto4",$"DecUniDepto5",$"DecUniDepto6",$"DecUniDepto7",$"DecUniDepto8",$"DecUniDepto9"))
  .withColumn("DecilUniTiendaSubDepto",udfDecideDecil($"TotalUni",$"DecUniSubDepto1",$"DecUniSubDepto2",$"DecUniSubDepto3",$"DecUniSubDepto4",$"DecUniSubDepto5",$"DecUniSubDepto6",$"DecUniSubDepto7",$"DecUniSubDepto8",$"DecUniSubDepto9"))
  .withColumn("DecilUniTiendaClase",udfDecideDecil($"TotalUni",$"DecUniClase1",$"DecUniClase2",$"DecUniClase3",$"DecUniClase4",$"DecUniClase5",$"DecUniClase6",$"DecUniClase7",$"DecUniClase8",$"DecUniClase9"))
  .withColumn("DecilUniTiendaSubClase",udfDecideDecil($"TotalUni",$"DecUniSubClase1",$"DecUniSubClase2",$"DecUniSubClase3",$"DecUniSubClase4",$"DecUniSubClase5",$"DecUniSubClase6",$"DecUniSubClase7",$"DecUniSubClase8",$"DecUniSubClase9"))
  .select("Tienda","SKU","TotalVta","TotalUni",
    "DecilVtaTiendaDepto","DecilVtaTiendaSubDepto","DecilVtaTiendaClase","DecilVtaTiendaSubClase",
    "DecilUniTiendaDepto","DecilUniTiendaSubDepto","DecilUniTiendaClase","DecilUniTiendaSubClase")
  .orderBy($"Tienda",$"Depto",$"SubDepto",desc("TotalUni"))
  .join(cats("skus"), Seq("SKU"), "left")

saveFileWithHeader(df, rootDir+"/resultados/decilTienda_"+myYearWeek.toString+".csv")


val df_dec = loadFileWithHeader(rootDir+"/resultados/decilTienda_"+myYearWeek.toString+".csv")


}
