require(dplyr)

df <- read.csv("sumaVentas_201751.csv",sep="|") %>% filter(TotalVta>0.0)

groupsTienda <- c("Tienda")
groupsSubDepto <- c("Tienda","Depto","SubDepto")
groupsClase <- c("Tienda","Depto","SubDepto","Clase")
groupsSubClase <- c("Tienda","Depto","SubDepto","Clase","SubClase")

# Deciles por Venta
decilesVta <- function(group_vector) {
	to <- df %>% 
		group_by_at(group_vector) %>%
		summarise(SumaVta=sum(TotalVta)) %>%
		ungroup()
	re <- df %>% 
		left_join(to, group_vector) %>%
		group_by_at(group_vector) %>% 
		arrange(desc(TotalVta)) %>%
		mutate(CumVta=cumsum(TotalVta),FracVta=CumVta/SumaVta)
	return(re)
}

# Deciles por Unidades
decilesUni <- function(group_vector) {
	to <- df %>% 
		group_by_at(group_vector) %>%
		summarise(SumaUni=sum(TotalUni)) %>%
		ungroup()
	re <- df %>% 
		left_join(to, group_vector) %>%
		group_by_at(group_vector) %>% 
		arrange(desc(TotalUni)) %>%
		mutate(CumUni=cumsum(TotalUni),FracUni=CumUni/SumaUni)
	return(re)
}



# Por Tiendas
write.table(decilesVta(groupsTienda), file="decilesVtaTienda_201751.csv", sep="|", row.names=FALSE)
write.table(decilesUni(groupsTienda), file="decilesUniTienda_201751.csv", sep="|", row.names=FALSE)

# Por Vta
write.table(decilesVta(groupsClase), file="decilesVtaClase_201751.csv", sep="|", row.names=FALSE)
write.table(decilesVta(groupsSubDepto), file="decilesVtaSubDepto_201751.csv", sep="|", row.names=FALSE)

# Por Uni
write.table(decilesUni(groupsClase), file="decilesUniClase_201751.csv", sep="|", row.names=FALSE)
write.table(decilesUni(groupsSubDepto), file="decilesUniSubDepto_201751.csv", sep="|", row.names=FALSE)

# Heads
head(decilesUnidades(groupsClase) %>% 
     select(SKU,Tienda,TotalUni,Depto,SubDepto,Clase,SubClase,SumaUni,FracUni) %>%
     filter(Tienda==232,Depto==2,SubDepto==4,Clase==2)
     )
head(decilesUnidades(groupsSubDepto) %>% 
     select(SKU,Tienda,TotalUni,Depto,SubDepto,Clase,SubClase,SumaUni,FracUni) %>%
     filter(Tienda==232,Depto==2,SubDepto==4)
     )
