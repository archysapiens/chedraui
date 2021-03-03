require(dplyr)

catalogo <- read.csv("matriz_01Dic2017.csv",sep="|",header = TRUE)

names(catalogo)[1] <- "Tienda"
catalogoV2 <- catalogo[,c("Tienda","Descripcion")]

#################################################################################
##### FUNCION  -------
conteosTienda <- function(datos){
  
    conteos <- datos %>% 
    group_by(Tienda,Semana)%>%
    summarise(freq=n())
  
  semana <-unique(conteos$Semana)
  
  diferencias <- left_join(catalogoV2,conteos, by="Tienda")
  
  
  for(i in 1:nrow(diferencias)){
    if(is.na(diferencias[i,"Semana"])){
      diferencias[i,"Semana"]= semana
    }
  }
  
  return(diferencias)
}



#######################################################################################
##### CICLO  -------
carpeta <- "csvCooked/"
files <-list.files(path = carpeta,pattern = "*.csv")
lista <- list()

for(i in files){
  archivo <- paste0(carpeta, i, "")
  print(archivo)
  df <- read.csv(archivo,sep="|",header = TRUE,stringsAsFactors = TRUE)
  print("Lectura exitosa")
  lista[[i]] <- conteosTienda(df)
}
final <- do.call(rbind,lista)
write.table(final, "storeMatch.csv",row.names=FALSE,sep="|")

##########################################################################################
