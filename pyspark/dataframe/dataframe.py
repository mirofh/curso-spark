# Operações DataFrames

# link para a documentação oficial
# https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame

#############################################
# Acessando informações sobre os dados 

# Mostra o tipo da variavel cities:
cities

# Retorna uma lista com o nomes das colunas do DataFrame:
cities.columns

# Retorna o Schema do DataFrame
cities.printSchema()

# Mostra as n primeiras linhas no console:
cities.show(10)

# Retorna as primeiras n linhas de uma DataFrame
# retorna dados para o Driver
cities.take(3)

# Retorna todos os records como uma lista 
# retorna dados para o Driver
cities.collect()

#############################################
# Funções

# Calcula estatísticas básicas sobre o DataFrame:
cities.describe().show()

# Renomeia uma coluna de um DataFrame:
cities.printSchema()
cities.withColumnRenamed("Nome_Municipio", "nome").withColumnRenamed("Codigo Municipio Completo", "codigo").printSchema()

# Renomeia uma coluna de um DataFrame:
# Retorna um novo DataFrame com novos nomes
mcmv.toDF("houses", "ibge_code", "year").show(3)

#######
## Ex1: mudar o nome das colunas da tabela cities.
cities = cities.toDF("uf_code", "uf_name", "meso_code", "meso_name", "micro_code", "micro_region", "city_code", "ibge_code", "city_name")
cities.show()

# Select: projeta uma série de expressões e retorna um novo DataFrame
# Seleciona determinadas colunas de um DataFrame:
cities.select("city_name", "ibge_code").show()

# Seleciona 3 nomes:
cities.select("city_name").limit(3).show()

# Selectiona todas as colunas de cities
cities.select("*").show();

# selectExpr: é uma variação do select que aceita expressões SQL
cities.selectExpr("municipio", "uf", "ano").show()

cities.selectExpr("city_code + 20").show()

#######
## Ex2: selecione o conteúdo da tabela transf e 
##      calcule o total dos repasses do governo federal.
#transftotal = transf.selectExpr("municipio", "uf", "ano", "decendio + decendio2 + decendio3")
#transftotal.show(10)

# Conta quantas linhas existe:
cities.count()

# Retorna um novo DataFrame contendo as linhas unicas
cities.distinct()
cities.select("city_name").distinct()

# Ordena por uma coluna
cities.sort("city_name").show(3)
cities.sort(cities.city_name.desc()).show(3)
cities.sort("city_name", ascending=False).show(3)

# Orderna por uma coluna
cities.orderBy(cities.city_name.desc()).show(10)
cities.orderBy(["uf_code", "city_name"], ascending=[0, 1]).show(10)
# importa uma serie de funcoes
from pyspark.sql.functions import *
cities.sort(asc("city_name")).show(10)
cities.orderBy(desc("uf_code"), "city_name").show(10)

######
## Ex3: qual são as 10 cidades que receberam os maiores repasses? 
##      qual são as 10 cidades que menos os maiores repasses? 
#transftotal = transftotal.toDF("city", "uf_acron", "year", "total")
#transftotal.orderBy(transftotal.total.desc()).show(10)
#transftotal.filter("total > 0").orderBy(transftotal.total.asc()).show(10)
#transftotal.orderBy(transftotal.total.asc()).show(10)
#transftotal.show()

# Filter filtra linhas de acordo com uma condição:
# Seleciona todas as informações de todas as cidades que começam com a letra 'X':
cities.filter(cities['city_name'].startswith('X')).show()

# Seleciona o nome e o código de todas as cidades que começam com a letra 'X':
cities.filter(cities['city_name'].startswith('X')).select(cities['city_name'], cities['ibge_code']).show()

# where é um alias para filter
cities.where(cities["uf_name"].startswith('R')).show()
transf.where(transf['ano'] < 1998).show()

# links para a documentação do GroupBy e suas funções de agregação
# Funções: avg, count, max, min, mean, pivot, sum, apply, agg
# https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.GroupedData
# groupby é um alias para groupBy
# Calcula a quantidade de cidades por Estado:
cities.groupBy(cities["uf_name"]).count().show()
cities.groupBy(cities["uf_name"]).count().sort("uf_name").show()

# Atenção: groupBy deve ser acompanhado por uma função de agregação logo após sua execução.
# o comando abaixo vai falhar.
#cities.groupBy(cities["uf_name"]).sort("uf_name").groupBy().show()

# Agrupa o valor repassado no primeiro decendio por estado
# ordena o resultado no Driver
sorted(transf.groupBy('uf').agg({'decendio': 'avg'}).collect())


######
## Ex4: seleciona o total dos repasses feitos do governo federal aos municipios, por ano.
#       selectiona o total dos repasses feitos pelo governo aos municipios do Paraná.
#transftotal = transftotal.toDF("city_name", "uf_acron", "year", "total")
#transftotal.show()
#transftotal = transftotal.groupBy("uf_acron", "city_name", "year").agg(F.round(F.sum("total"),2).alias("total")).orderBy("uf_acron", "city_name")
#transftotal.show()


######
## Ex5: quais são as 10 cidades que mais receberam repasse no Paraná?  
#       quais são as 10 cidades que menos receberam repasse no Paraná?  
#transftotal.groupBy("uf_acron", "city_name", "year").agg(F.round(F.sum("total"),2).alias("total")).filter("uf_acron = 'PR'").orderBy(desc("total")).show(10)
## para mostrar o valor em milhoes
#transftotal.groupBy("uf_acron", "city_name", "year").agg(F.round(F.sum("total"),2).alias("total")).filter("uf_acron = 'PR'").selectExpr("uf_acron", "city_name", "year", "round(total / power(10,6t),2)").orderBy(desc("total")).show(10)
#transftotal.groupBy("uf_acron", "city_name", "year").agg(F.round(F.sum("total"),2).alias("total")).filter("uf_acron = 'PR'").orderBy(asc("total")).show(10)

# Retorna uma amostra dos dados
# withReplacement: com ou sem substituição
# fraction: fração das linhas a serem geradas [0:1]
cities.sample(False, 0.145).show()

# Seleciona o total dos repasses do governo federal feitos aos municípios, por mes.
transf = transf.withColumnRenamed("2 decendio","decendio2")
transf = transf.withColumnRenamed("3 decendio","decendio3")

# importa uma serie de funcoes
import pyspark.sql.functions as F
# Retorna um novo DataFrame por adicionar uma coluna, ou substituir uma coluna existente que contenha o mesmo nome
transf = transf.withColumn('total', transf.decendio + transf.decendio2 + transf.decendio3).select("municipio", "uf", "ano",  "mes", F.round("total",2).alias("total"))
transf.show()

# Retorna um novo RDD sem a coluna
transf.printSchema()
transf.drop("total").printSchema()

# Seleciona o total dos repasses feitos do governo federal ao municipio de Curitiba, por ano.
transf.filter(transf.municipio == "Curitiba").groupBy(transf.municipio, "ano").agg(F.round(F.sum("total"),2).alias("total")).orderBy("municipio", "ano").show()

# Seleciona o total dos repasses feitos do governo federal aos municipios, por ano.
transf = transf.groupBy(transf.municipio, "ano").agg(F.round(F.sum("total"),2).alias("total")).orderBy("municipio", "ano")
transf.show()

#  Faz juncao de dois DataFrames
j = cities.join(geo, cities['ibge_code'] == geo['codigo_ibge'])
j.show(3)

######
## Ex6: Faça uma junção que resulta em uma tabela com: |city_name|uf_acron|year|total|latitude|longitude|
# Seleciona o nome, o código do IBGE, a latitude e longitude de todas as cidades.
#df1 = cities.join(geo, cities['ibge_code'] == geo['codigo_ibge']).select("uf", "city_name", "codigo_ibge", "latitude", "longitude")
#df1 = cities.join(geo, cities['ibge_code'] == geo['codigo_ibge']).select("uf", "city_name", "codigo_ibge", "latitude", "longitude").join(transftotal, [transftotal.city_name == cities.city_name, transftotal.uf_acron == geo.uf], "inner").select("uf", transftotal["city_name"], "year", "total", "latitude", "longitude")
#df1.show(3)

#############################################
# Transformar para outros tipos de dados

# importa uma serie de funcoes
import pyspark.sql.functions as F

# convert para JSON
json = transf.filter(transf.municipio == "Curitiba").groupBy(transf.municipio, "ano").agg(F.round(F.sum("total"),2).alias("total")).orderBy("municipio", "ano").toJSON()
json.first()

# convert para Pandas
#panda = transf.filter(transf.municipio == "Curitiba").groupBy(transf.municipio, "ano").agg(F.round(F.sum("total"),2).alias("total")).orderBy("municipio", "ano").toPandas()
#panda.first()

# retorna um novo DataFrame com novos nomes
mcmv.toDF("city", "ibge_code", "year").collect()

#############################################
# Cria tabelas temporarias acessiveis via SparkSQL

cities.createOrReplaceTempView("cities")
transf.createOrReplaceTempView("transf")
geo.createOrReplaceTempView("geo")
mcmv.createOrReplaceTempView("mcmv")
pib.createOrReplaceTempView("pib")

query_result = spark.sql("SELECT * FROM cities")
query_result.show(3)

