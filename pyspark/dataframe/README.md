# Operações com Datasets e DataFrames

Mostra todas as colunas do DataFrame:

```python
cities.columns
```

Calcula estatísticas básicas sobre o DataFrame:

```python
cities.describe().show()
```

Renomeia uma coluna de um DataFrame:

```python
cities.printSchema()
cities = cities.withColumnRenamed("Nome_Municipio", "nome").withColumnRenamed("Codigo Municipio Completo", "codigo")
cities.printSchema()
```

Seleciona determinadas colunas de um DataFrame:

```python
cities.select("nome", "codigo").show()
```

Conta quantas linhas existe:

```python
cities.count()
```

Selectiona somente os nomes distintos:


```python
cities.distinct("nome")
```

Seleciona 3 nomes:

```python
cities.select("nomes").limit(3).show()
```

```python
cities.orderBy("Nome_Mesorregiao").show()
```

```python
cities.sort("Nome_UF").show()
```

```python
cities.where( cities['Municipio'] < 100).show()
```

```python
cities.sample(False, 0.145).show()
```



Calcula a quantidade de cidades por Estado:

```python
cities.groupBy(cities["Nome_UF"]).count().show()
```

Seleciona todas as informações de todas as cidades que começam com a letra 'X':

```python
cities.filter(cities['nome'].startswith('X')).show()
```

Seleciona o nome e o código de todas as cidades que começam com a letra 'X':

```python
cities.filter(cities['nome'].startswith('X')).select(cities['nome'], cities['codigo']).show()
```





Seleciona o total dos repasses do governo federal feitos aos municípios, por mes.

```python
transf = transf.withColumnRenamed("2 decendio","decendio2")
transf = transf.withColumnRenamed("3 decendio","decendio3")

# importa uma serie de funcoes
import pyspark.sql.functions as F

transf = transf.withColumn('total', transf.decendio + transf.decendio2 + transf.decendio3).select("municipio", "uf", "ano",  "mes", F.round("total",2).alias("total"))
transf.show()
```

Seleciona o total dos repasses feitos do governo federal ao municipio de Curitiba, por ano.

```python
transf.filter(transf.municipio == "Curitiba").groupBy(transf.municipio, "ano").agg(F.round(F.sum("total"),2).alias("total")).orderBy("municipio", "ano").show()
```

Seleciona o total dos repasses feitos do governo federal aos municipios, por ano.

```python
transf = transf.groupBy(transf.municipio, "ano").agg(F.round(F.sum("total"),2).alias("total")).orderBy("municipio", "ano")
transf.show()
```


Realiza junção dos DataFrames *cities* e *geo* pelo código do IBGE.

```python
j = cities.join(geo, cities['codigo'] == geo['codigo_ibge']).show()
j.show(10)
```

Seleciona o nome, o código do IBGE, a latitude e longitude de todas as cidades.

```python
df1 = cities.join(geo, cities['codigo'] == geo['codigo_ibge']).select("nome", "codigo_ibge", "latitude", "longitude")
df1.show(10)
```

```python
df2 = df1.join(transf, df1['nome'] == transf['municipio']).select(df1["nome"], df1["codigo_ibge"], df1["latitude"], df1["longitude"], transf["ano"], transf["mes"], transf["decendio"], transf["2 decendio"], transf["3 decendio"])
df2.show()
```

```python
df4 = df3.join(df2, df3['municipio_macro'] == df2['codigo_ibge'])
df4.show()
```

