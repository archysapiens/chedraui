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

// Some useful defaults
val rootDir = "hdfs://This:54310/Tamalytics/chedraui2"
val uri = "hdfs://This:54310/"

// Test
val df = loadFileWithHeader(rootDir+"/resultados/impacto_201802.csv")
val dftest = df.filter($"GrupoArticulo".isNotNull)
  .select("SKU","Tienda","GrupoArticulo","PrecioSugerido","PrecioPuntual",
    "Prioridad","PrioridadGrupo",
    "PrecioGrupo","FuenteSugerido","SKUGrupo","TiendaGrupo","CadenaGrupo","FuenteGrupo")
dftest.filter($"Tienda"===93).orderBy("GrupoArticulo").drop("Prioridad","PrioridadGrupo","PrecioGrupo").show
dftest.filter($"Tienda"===93 && $"GrupoArticulo"===4837).drop("Prioridad","PrioridadGrupo","PrecioGrupo").show
