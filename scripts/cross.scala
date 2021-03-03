// Imports 
import org.tamalytics.chedrauiHerramienta.Catalogues.readCatalogues
import org.tamalytics.chedrauiHerramienta.Competition.readCompPrices
import org.tamalytics.chedrauiHerramienta.Calc.{calcIndicadores, generateHeader, calcResumen}
import org.tamalytics.chedrauiHerramienta.Aux.loadSchema
import org.tamalytics.chedrauiHerramienta.Sales.readSales

// Some useful defaults
val rootDir = "hdfs://T801:54310/Tamalytics/chedraui2"
val uri = "hdfs://T801:54310/"
val myYear = 2017
val myWeek = 42
val myYear2 = 2017
val myWeek2 = 40
val myYearWeek = myYear*100+myWeek
val myYearWeek2 = myYear2*100+myWeek2

// Def 
def isAllDigits(x: String) = x forall Character.isDigit
def asIntArray(df: Array[Any]): Array[Int] = {
  try {
    df.map(_.asInstanceOf[Int])
  } catch {
    case _: Throwable => df.map(_.asInstanceOf[String]).filter(isAllDigits).map(_.toInt)
  }
}

// Read data
val cats = readCatalogues(rootDir,matriz="matriz_30Nov2017.csv")
val ventas = readSales(myYearWeek.toString,rootDir)
ventas.persist
val cat_matriz = cats("cat_matriz")



// Selecting stores
val t_ventas_df = ventas.select("Tienda").distinct
val t_matriz_df = cat_matriz.select("Tienda").distinct
val t_ventas: Array[Int] = asIntArray(t_ventas_df.collect.map(_.apply(0)))
val t_matriz: Array[Int] = asIntArray(t_matriz_df.collect.map(_.apply(0)))
val (t_ventasInMatriz, t_ventasNotMatriz) = t_ventas.partition(t_matriz contains _)
val (t_matrizInVentas, t_matrizNotVentas) = t_matriz.partition(t_ventas contains _)



// Join result
val ventasMatr = ventas.join(cat_matriz,"Tienda")

// Count stores
val t_ventasMatr_df = ventasMatr.select("Tienda").distinct
val t_ventasMatr = asIntArray(t_ventasMatr_df.collect.map(_.apply(0)))



// Read competition data
val competencia = readCompPrices(myYearWeek.toString,rootDir,uri)

// Join
val ventasComp = ventasMatr.join(competencia, Seq("SKU","compId"), "inner")

// How many stores are left?
val t_ventasComp_df = ventasComp.select("Tienda").distinct
val t_ventasComp: Array[Int] = asIntArray(t_ventasComp_df.collect.map(_.apply(0)))



// Read competition data
val competencia2 = readCompPrices(myYearWeek2.toString,rootDir,uri)

// Join
val ventasComp2 = ventasComp.join(competencia2, Seq("SKU","compId"), "inner")

// How many stores are left?
val t_ventasComp2_df = ventasComp2.select("Tienda").distinct
val t_ventasComp2: Array[Int] = asIntArray(t_ventasComp_df.collect.map(_.apply(0)))




// Setup variables

// Perform calculations
weeks.foreach(w => calcIndicadores(w._1,w._2,cats))

// Aggregate
weeks.foreach(w => calcResumen(w._1,w._2,rootDir,uri))

// Generate a header
generateHeader(2017, 42)
