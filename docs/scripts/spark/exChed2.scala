/* Este no es un script completo, sino una coleccion de 
 * pequenios mini-scripts que muestran como manipular la 
 * informacion con Spark y filtros */

// Importaciones y variables comunes a todos 
import org.btrust.chedrauiHerramienta.Catalogues.leeCatalogos
import org.btrust.chedrauiHerramienta.Calc.{calcCompetencia,calcDatos,calcPrecios,calcImpactoPrecios,sumaVentas}
import org.btrust.chedrauiHerramienta.Files.{saveFileWithHeader,loadFileWithHeader}
spark.conf.set("spark.sql.autoBroadcastJoinThreshold",-1)
val rootDir = "/media/black/test/herramienta/data"





/***************************************
 * FILTRADO DE DATOS, UN SOLO SUBDEPTO *
 * ************************************/

// Filtrado de datos, queremos estos archivos especificamente
val files = Seq("datos_201751.csv","impacto_201751.csv","impactoNulos_201751.csv")

// Aseo del hogar, lee archivo luego filtra y guarda
files.foreach(fi => {
  val datos = loadFileWithHeader(rootDir+"/resultados/"+fi).filter($"Depto"===1 && $"SubDepto"===7)
  saveFileWithHeader(datos, rootDir+"/resultados/aseo_"+fi)
})





/****************************************************
 * FILTRADO DE DATOS, TODOS LOS DEPTOS POR SUBDEPTO *
 * **************************************************/

// Archivos a filtrar en resultados
val files = Seq("impacto_201802.csv","impactoNulos_201802.csv","impactoPromos_201802.csv")

// Filtracion
files.foreach(fi => {
  println("Filtering "+fi)
  // Lectura de datos
  val datos = loadFileWithHeader(rootDir+"/resultados/"+fi)
  // Listado de departamentos no nulos y en orden
  val deptos = datos.select("Depto").filter($"Depto".isNotNull).distinct.collect.map(_(0).asInstanceOf[Int]).sorted
  deptos.foreach(depto => {
    println("Depto "+depto)
    // Filtrando un depto
    val datosdepto = datos.filter($"Depto"===depto)
    // Listado de subdeptos no nulos y en orden
    val subdeptos = datosdepto.select("SubDepto").filter($"SubDepto".isNotNull).distinct.collect.map(_(0).asInstanceOf[Int]).sorted
    subdeptos.foreach(subdepto => {
      println("SubDepto "+subdepto)
      // Filtrado un subdepto
      val datossubdepto = datosdepto.filter($"SubDepto"===subdepto)
      // Guardando datos
      saveFileWithHeader(datossubdepto, rootDir+"/resultados/depto"+depto+"subdepto"+subdepto+"_"+fi)
    })
  })
})





/********************************
 * FILTRADO DE DATOS POR TIENDA *
 * ******************************/

// Filtrando impactos por tienda
val files = Seq("impacto_201751.csv","impactoPromos_201751.csv")

// Tiendas deseadas
val tiendas = Seq(245,139,121)
files.foreach(fi => {
  val datos = loadFileWithHeader(rootDir+"/resultados/"+fi)
  tiendas.foreach(ti => {
    saveFileWithHeader(datos.filter($"Tienda"===ti), rootDir+"/resultados/tienda"+ti+"_"+fi)
  })
})
