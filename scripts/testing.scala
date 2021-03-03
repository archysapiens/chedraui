// Imports 
import org.tamalytics.chedrauiHerramienta.Catalogues.readCatalogues
import org.tamalytics.chedrauiHerramienta.Competition.readCompPrices
import org.tamalytics.chedrauiHerramienta.Inventory.{getInvenFile, getInventario, getPrecioRegulacionCentral}

// Some variables for testing
val rootDit = "hdfs://T801:54310/Tamalytics/chedrauiMargenes/"
val uri = "hdfs://T801:54310/"

// Setup variables
val cats  = readCatalogues()

// Weeks
val weeks = (4 to 42).filter(x => x!=24 && x!=26).map(x => (2017,x)).toList.reverse

// Do it
weeks.foreach(w => chedrauiMargenes(w._1,w._2,cats))
