// Imports 
import org.btrust.chedrauiHerramienta.Catalogues.leeCatalogos
import org.btrust.chedrauiHerramienta.Calc.{calcCompetencia,calcDatos,calcPrecios,calcImpactoPrecios,sumaVentas}
import org.btrust.chedrauiHerramienta.Desc.{calcDescriptivo}
import org.btrust.chedrauiHerramienta.Sales.readSales
import org.btrust.chedrauiHerramienta.Central.{getInventario,getPRCfromInvenFile}
import org.btrust.chedrauiHerramienta.Competition.readCompPrices
import org.btrust.chedrauiHerramienta.Files.{saveFileWithHeader,loadFileWithHeader}
import org.btrust.chedrauiHerramienta.Aux.{udfChedRound}
spark.conf.set("spark.sql.autoBroadcastJoinThreshold",-1)

// Test
val df = loadFileWithHeader(rootDir+"/resultados/impacto_201802.csv")

val dftest = df.filter($"GrupoArticulo".isNotNull)
  .select("SKU","Tienda","GrupoArticulo","PrecioSugerido","PrecioPuntual",
    "PrecioGrupo","FuenteSugerido","SKUGrupo","TiendaGrupo","CadenaGrupo","FuenteGrupo")

dftest.filter($"Tienda"===93 && $"GrupoArticulo"===4837).drop("PrecioGrupo").show


// Some useful defaults
val rootDir = "hdfs://This:54310/Tamalytics/chedraui2"
val uri = "hdfs://This:54310/"

// Options
val myYear = 2018
val myWeek = 2
val inven  = "Inven_15Ene18.csv"

// Catalogues
val cats  = leeCatalogos(rootDir,matriz="matriz_subdepto.csv")

// Suma ventas
sumaVentas(2017,48,5,cats,rootDir,uri)

// Auxiliary stuff
calcCompetencia(myYear,myWeek,cats,rootDir,uri,semanaDesde=6,semanaHasta=0)

// Perform calculations
calcDatos(myYear,myWeek,cats,rootDir,uri=uri,inven=inven)

// Prescribe prices
calcPrecios(myYear,myWeek,cats,rootDir,uri)

// Calculate impact of pricing 
calcImpactoPrecios(myYear,myWeek,cats,inven,rootDir,uri,ventas="sumaVentas_201744a201748.csv")








// Filtrado de datos
val files = Seq("datos_201751.csv","impacto_201751.csv","impactoNulos_201751.csv")

// Aseo del hogar
files.foreach(fi => {
  val datos = loadFileWithHeader(rootDir+"/resultados/"+fi).filter($"Depto"===1 && $"SubDepto"===7)
  saveFileWithHeader(datos, rootDir+"/resultados/aseo_"+fi)
})


// Solo depto 1
val files = Seq("datos_201751.csv","impacto_201751.csv","impactoNulos_201751.csv","impactoPromos_201751.csv")

val files = Seq("impacto_201751.csv","impactoNulos_201751.csv","impactoPromos_201751.csv")
val descs = List("PGC_NoComestible","PGC_Comestible")
files.foreach(fi => {
  descs.foreach(de => {
    val name = de+"_"+fi
    println(name)
    val datos = loadFileWithHeader(rootDir+"/resultados/"+fi).filter($"Depto"===1 && $"DescripDepto"===de)
    saveFileWithHeader(datos, rootDir+"/resultados/"+name)
  })
})

// Filter data

val files = Seq("impacto_201802.csv","impactoNulos_201802.csv","impactoPromos_201802.csv")
files.foreach(fi => {
  println("Filtering "+fi)
  val datos = loadFileWithHeader(rootDir+"/resultados/"+fi)
  val deptos = datos.select("Depto").filter($"Depto".isNotNull).distinct.collect.map(_(0).asInstanceOf[Int]).sorted
  // Depto 1,4 and 7 are big, subdivide in subdeptos
  deptos.foreach(depto => {
    println("Depto "+depto)
    val datosdepto = datos.filter($"Depto"===depto)
    if (7==7) {
      val subdeptos = datosdepto.select("SubDepto").filter($"SubDepto".isNotNull).distinct.collect.map(_(0).asInstanceOf[Int]).sorted
      subdeptos.foreach(subdepto => {
        println("SubDepto "+subdepto)
        val datossubdepto = datosdepto.filter($"SubDepto"===subdepto)
        saveFileWithHeader(datossubdepto, rootDir+"/resultados/depto"+depto+"subdepto"+subdepto+"_"+fi)
      })
    } else {
      saveFileWithHeader(datosdepto, rootDir+"/resultados/depto"+depto+"_"+fi)
    }
  })
})

// Filtrando impactos por tienda

val files = Seq("impacto_201751.csv","impactoPromos_201751.csv")
val tiendas = Seq(245,139,121)
files.foreach(fi => {
  val datos = loadFileWithHeader(rootDir+"/resultados/"+fi)
  tiendas.foreach(ti => {
    saveFileWithHeader(datos.filter($"Tienda"===ti), 
      rootDir+"/resultados/tienda"+ti+"_"+fi)
  })
})
