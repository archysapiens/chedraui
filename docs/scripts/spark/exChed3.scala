/* Este no es un script completo, sino una coleccion de 
 * pequenios mini-scripts que muestran como manipular la 
 * informacion con Spark y agregaciones */

// Importaciones y variables comunes a todos 
import org.btrust.chedrauiHerramienta.Catalogues.leeCatalogos
import org.btrust.chedrauiHerramienta.Calc.{calcCompetencia,calcDatos,calcPrecios,calcImpactoPrecios,sumaVentas}
import org.btrust.chedrauiHerramienta.Files.{saveFileWithHeader,loadFileWithHeader}
spark.conf.set("spark.sql.autoBroadcastJoinThreshold",-1)
val rootDir = "/media/black/test/herramienta/data"





/******************************************
 * SUMA DE IMPACTOS POR TIENDA Y SUBDEPTO *
 ******************************************/

// Archivo a leer
val fi = "impacto_201751.csv"

// Aseo del hogar, lee archivo luego filtra y guarda
val datos = loadFileWithHeader(rootDir+"/resultados/"+fi).filter($"Depto"===1 && $"SubDepto"===7)
// Sumando impactos
val agg = datos.groupBy("Tienda","Depto","SubDepto")
  .agg(sum("ImpactoConCental") as "SumaImpactoConCentral", sum("ImpactoConPromedio") as "SumaImpactoConPromedio")

// Guardando resultados
saveFileWithHeader(agg, rootDir+"/resultados/aggTiendaSubdepto_"+fi)





/********************************************
 * CONTEO DE RESULTADOS CON PRECIO SUGERIDO *
 ********************************************/

// Archivo a leer
val fi = "impacto_201751.csv"

// Sumando articulos regulados
val agg1 = datos.groupBy("Tienda","Depto","SubDepto")
  .filter($"PrecioSugerido".isNotNull)
  .agg(count("SKU") as "ArticulosRegulados")
// Sumando todos los articulos
val agg2 = datos.groupBy("Tienda","Depto","SubDepto")
  .agg(count("SKU") as "ArticulosTotales")
// En una sola tabla
val agg = agg1.join(agg2, Seq("Tienda","Depto","SubDepto"), "inner")

// Guardando resultados
saveFileWithHeader(agg, rootDir+"/resultados/regulados_"+fi)
