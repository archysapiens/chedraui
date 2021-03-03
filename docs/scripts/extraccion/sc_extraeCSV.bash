#!/bin/bash
name=$1
cat ../resultados/$name.plainheader/part-* > $name
cat ../resultados/$name/part-* >> $name
