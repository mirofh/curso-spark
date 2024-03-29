%spark.pyspark


########################################
# Cria um DataFrame a partir de uma lista

dados = [2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019]
spark.createDataFrame(dados).show()
spark.createDataFrame(dados, ["ano"]).show()

########################################
# Cria um DataFrame a partir de uma tupla

dados = [('Curitiba', 2019)]
spark.createDataFrame(dados).show()
spark.createDataFrame(dados, ["cidade", "ano"]).show()

########################################
# Cria um DataFrame a partir de um dicionario
dados = [{'cidade': 'Curitiba', 'ano': 2019}]
spark.createDataFrame(dados).show()
spark.createDataFrame(dados, ["cidade", "ano"]).show()

########################################
# Cria um DataFrame a partir de RDD
dados = [('Curitiba', 2019)]
rdd = sc.parallelize(dados)
spark.createDataFrame(rdd).show()
df = spark.createDataFrame(rdd, ['cidade', 'ano'])

########################################
# Contém informações geográficas e código
# do IBGE para cada município do Brasil.
cities = spark.read.format("csv")\
    .option("delimiter", ";")\
    .option("header", "true")\
    .option("encoding", "ascii")\
    .load("file:///home/erlfilho/git/zeppelin/data/municipios/municipios.csv")

########################################
# Todas as transferências feitas do governo
# federal para os municípios em 2018
# dividídas por decêndios.
transf = spark.read.format("csv")\
    .option("delimiter", ";")\
    .option("header", "true")\
    .option("encoding", "ascii")\
    .load("file:///home/erlfilho/git/zeppelin/data/municipios/municipios-transf.csv")

########################################
#  Localização geográfica de cada município.
geo = spark.read.json("file:///home/erlfilho/git/zeppelin/data/municipios/municipios-geo.json",  multiLine=True)

########################################
# Quantidade de unidades financiadas pelo
# Minha Casa Minha Vida em cada município.

from pyspark.sql.types import *

# 1) identificar o schema
"""
    <entry>
      <unidades>879</unidades>
      <municipio_ibge>130356</municipio_ibge>
      <ano>2014</ano>
    </entry>
"""

# 2) criar um objeto que represente o schema do XML
xsd = [StructField("unidades", IntegerType(), True),\
       StructField("municipio_ibge", IntegerType(), True),\
       StructField("ano", IntegerType(), True)]

customSchema = StructType(xsd)

# 3) utilizar o contexto do spark para ler o XML
# rowTag: Exemplo, em <unidades> <uu><uu> ...</unidades>, <u> seria a rowTag
mcmv = sqlContext.read.format('com.databricks.spark.xml')\
    .options(rowTag='entry')\
    .load('file:///home/erlfilho/git/zeppelin/data/municipios/municipios-mcmv.xml', schema = customSchema)

########################################
# Contém informações sobre o PIB de cada
# município do Brasil.

# 1) identificar o schema
"""
  <ValorDescritoPorSuasDimensoes>
    <D1C>Brasil (Codigo)</D1C>
    <D1N>Brasil</D1N>
    <D2C>Variavel (Codigo)</D2C>
    <D2N>Variavel</D2N>
    <D3C>Ano (Codigo)</D3C>
    <D3N>Ano</D3N>
    <MC>Unidade de Medida (Codigo)</MC>
    <MN>Unidade de Medida</MN>
    <V>Valor</V>
  </ValorDescritoPorSuasDimensoes>
"""

# 2) criar um objeto que represente o schema do XML
xsd = [ StructField("D1C", DoubleType(), True),\
        StructField("D1N", StringType(), True),\
        StructField("D2C", DoubleType(), True),\
        StructField("D2N", StringType(), True),\
        StructField("D3C", DoubleType(), True),\
        StructField("D3N", DoubleType(), True),\
        StructField("MC", DoubleType(), True),\
        StructField("MN", StringType(), True),\
        StructField("V", DoubleType(), True)]

customSchema = StructType(xsd)

# 3) utilizar o contexto do spark para ler o XML
# algum item da tabela pode ser null porque nao ``casou'' com o tipo definido no schema
# rowTag: Exemplo, em <valores> <vv><vv> ...</valores>, <vv> seria a rowTag
pib = sqlContext.read.format('com.databricks.spark.xml')\
    .options(rowTag='ValorDescritoPorSuasDimensoes')\
    .load('file:///home/erlfilho/git/zeppelin/data/municipios/municipios-pib.xml', schema = customSchema)


########################################
# Mostra as duas primeiras linhas de cada arquivo

print("Todas as cidades do Brasil com o código do IBGE (Codigo Municipio Completo).")
cities.show()

print("Todas as cidades do Brasil com suas localizações geográficas e código do IBGE (código_ibge).")
geo.show()

print("Todas as transferências feitas do governo federal para os municípios em 2018, dividídas por decêndios.")
transf.show()

print("O Produto Interno Bruto (PIB) de cada município brasileiro.")
pib.show()

print("Quantidade de unidades habitacionais contratadas em todas as faixas (1, 2 e 3) do MCMV, por município, entre 2009 e 2014.")
mcmv.show()

########################################
# Todos os dados lidos estão no formato
cities
transf
geo
mcmv
pib

########################################
# Mostra o schema de cada DataFrame

print("Todas as cidades do Brasil com o código do IBGE (Codigo Municipio Completo).")
cities.printSchema()

print("Todas as cidades do Brasil com suas localizações geográficas e código do IBGE (código_ibge).")
geo.printSchema()

print("Todas as transferências feitas do governo federal para os municípios em 2018, divididas por decêndios.")
transf.printSchema()

print("O Produto Interno Bruto (PIB) de cada município brasileiro.")
pib.printSchema()

print("Quantidade de unidades habitacionais contratadas em todas as faixas (1, 2 e 3) do MCMV, por município, entre 2009 e 2014.")
mcmv.printSchema()


