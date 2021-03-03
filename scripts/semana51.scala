// Imports 
import org.btrust.chedrauiHerramienta.Catalogues.leeCatalogos
import org.btrust.chedrauiHerramienta.Calc.{calcCompetencia,calcDatos,calcResumen,calcPrecios,calcImpactoPrecios,sumaVentas}
import org.btrust.chedrauiHerramienta.Desc.{calcDescriptivo}
import org.btrust.chedrauiHerramienta.Sales.readSales
import org.btrust.chedrauiHerramienta.Central.{getInventario,getPRCfromInvenFile}
import org.btrust.chedrauiHerramienta.Competition.readCompPrices
import org.btrust.chedrauiHerramienta.Files.{saveFileWithHeader,loadFileWithHeader}
import org.btrust.chedrauiHerramienta.Aux.{udfChedRound}

// Some useful defaults
val rootDir = "hdfs://T801:54310/Tamalytics/chedraui2"
val uri = "hdfs://T801:54310/"

// Options
val myYear = 2017
val myWeek = 51
val inven  = "Inven_09Ene18.csv"

// Catalogues
val cats  = leeCatalogos(rootDir,matriz="matriz_09Ene2018.csv")

// Suma ventas
sumaVentas(myYear,48,5,cats,rootDir,uri)

// Auxiliary stuff
calcCompetencia(myYear,myWeek,cats,rootDir,uri)

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

val files = Seq("impacto_201751.csv","impactoNulos_201751.csv","impactoPromos_201751.csv")
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
