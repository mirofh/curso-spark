#!/bin/bash

url="http://dadosabertos.c3sl.ufpr.br/curitiba/Sigesguarda/"
output="data/sigesguarda"

wget --progress=bar --no-verbose --cut-dirs 2 --no-parent --recursive --no-directories --accept csv --directory-prefix=${output} $url
