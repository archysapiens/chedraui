require(dplyr)
matchIt <- function(comp, numb) {
	if (numb<10) {
		suff <- paste0("0",numb,"")
	} else {
		suff <- paste0("",numb,"")
	}
	suff2 <- paste0("2017",suff)
	file  <- paste0("Tablas_competencia/" ,comp,suff ,".csv","")
	file2 <- paste0("Tablas_competencia2/",comp,suff2,".csv","")
	data <- read.csv(file=file, header=TRUE, sep=",") %>%
		select(Barcode=Barcode, Articulo=Articulo, GrupoArticulos=GrupoArticulos, compDesc=Tienda, Precio=Precio)
	cata <- read.csv(file="cat_tiendas_nielsen.csv", header=TRUE, sep="|", na.strings="") %>%
		mutate(compFuente="nielsen")
	data2 <- data %>% 
		left_join(cata, by="compDesc") %>%
		select(Barcode=Barcode, Articulo=Articulo, GrupoArticulos=GrupoArticulos, compFuente=compFuente, compId=compId, compDesc=compDesc, Precio=Precio)
	write.table(data2, file=file2, sep="|", row.names=FALSE)
	return(1)
}

compArr <- list("ba_","walmart_")
weekArr <- 2:42

for (c in compArr) {
	for (w in weekArr) {
		print(paste0(c," ",w))
		matchIt(c,as.numeric(w))
	}
}
