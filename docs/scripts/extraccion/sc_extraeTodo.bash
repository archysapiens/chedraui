#!/bin/bash

for tbl in $(cat listado.csv) 
do 
    echo "Extraccion de $tbl"
    bash sc_extraeCSV.bash $tbl
done
