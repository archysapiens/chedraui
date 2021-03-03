#!/bin/bash
ls -d ../resultados/*$1*.csv | xargs -n 1 basename > listado.csv
