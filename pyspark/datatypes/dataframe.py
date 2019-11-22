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
cities = cities.withColumnRenamed("Nome_Municipio", "nome").withColumnRenamed("Codigo Municipio Completo", "codigo")
cities.printSchema()

# Select: projeta uma série de expressões e retorna um novo DataFrame
# Seleciona determinadas colunas de um DataFrame:
cities.select("nome", "codigo").show()

# Seleciona 3 nomes:
cities.select("nome").limit(3).show()

# Selectiona todas as colunas de cities 
cities.select("*").show();

# selectExpr: é uma variação do select que aceita expressões SQL
cities.selectExpr("Municipio < 20").show()

# Conta quantas linhas existe:
cities.count()

# Retorna um novo DataFrame contendo as linhas unicas
cities.distinct()
cities.select("nome").distinct()

# Ordena por uma coluna
cities.sort("nome").show(3)
cities.sort(cities.nome.desc()).show(3)
cities.sort("nome", ascending=False).show(3)

# Orderna por uma coluna
cities.orderBy(cities.nome.desc()).show(10)
cities.orderBy(["UF", "nome"], ascending=[0, 1]).show(10)
# importa uma serie de funcoes
from pyspark.sql.functions import *
cities.sort(asc("nome")).show(10)
cities.orderBy(desc("UF"), "nome").show(10)

# Filter filtra linhas de acordo com uma condição:
# Seleciona todas as informações de todas as cidades que começam com a letra 'X':
cities.filter(cities['nome'].startswith('X')).show()

# Seleciona o nome e o código de todas as cidades que começam com a letra 'X':
cities.filter(cities['nome'].startswith('X')).select(cities['nome'], cities['codigo']).show()

# where é um alias para filter
cities.where(cities["Nome_UF"].startswith('R')).show()
transf.where(transf['ano'] < 1998).show()

# links para a documentação do GroupBy e suas funções de agregação
# Funções: avg, count, max, min, mean, pivot, sum, apply, agg
# https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.GroupedData
# groupby é um alias para groupBy
# Calcula a quantidade de cidades por Estado:
cities.groupBy(cities["Nome_UF"]).count().show()
cities.groupBy(cities["Nome_UF"]).count().sort("Nome_UF").show()

# Atenção: groupBy deve ser acompanhado por uma função de agregação logo após sua execução.
# o comando abaixo vai falhar.
#cities.groupBy(cities["Nome_UF"]).sort("Nome_UF").groupBy().show()

# Agrupa o valor repassado no primeiro decendio por estado
# ordena o resultado no Driver
sorted(transf.groupBy('uf').agg({'decendio': 'avg'}).collect())

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
j = cities.join(geo, cities['codigo'] == geo['codigo_ibge']).show()
j.show(10)

# Seleciona o nome, o código do IBGE, a latitude e longitude de todas as cidades.
df1 = cities.join(geo, cities['codigo'] == geo['codigo_ibge']).select("nome", "codigo_ibge", "latitude", "longitude")
df1.show(10)

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
