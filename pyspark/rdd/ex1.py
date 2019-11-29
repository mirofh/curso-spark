## Exemplo 1
#
# Exemplo:
#   1. Utilizar spark para descobrir qual são as 10 chamadas mais comuns
#      na cidade de Curitiba.
#
# Dados:
#   * Estes dados estão registros oficiais da prefeitura de Curitiba.
#   * Disponíveis em http://dadosabertos.c3sl.ufpr.br
#   * /data/156.csv contém dados das chamadas ao 156
#   * /data/156-dicionario.csv contém a descrição de cada campo (header)
#
# Estes exemplos usam ações e transformações
#
# Exercício:
#   * Fazer um gráfico das 10 maiores SUBDIVISAO do assunto
#   * Quais são os 10 bairros onde mais acontecem ocorrências?
#   * Quais são os 10 bairros onde menos acontecem ocorrências?


data = sc.textFile("file:///home/erlfilho/git/zeppelin/data/156/156.csv")
#data.take(1)

dicionario = sc.textFile("file:///home/erlfilho/git/zeppelin/data/156/156-dicionario.csv")
#for line in dicionario.collect():
#    print(line)


# seleciona os tipos de cada abordagem
assunto = data.map(lambda x: (x.split(';')[6].replace("\"", "").strip(), 1))

#for item in assunto.take(200):
#    print(item[0], item[1])


# conta quantas abordagens de cada tipo
agrupados = assunto.reduceByKey(lambda x,y: x + y)

#for chave, valor in agrupados.take(200):
#    print(chave, valor)


# seleciona as 10 chamadas mais realizadas
agrupados = assunto.reduceByKey(lambda x,y: x + y).takeOrdered(10, lambda x: x[1] *-1)

#############################
# Cria tabela no SparkSQL
#df = spark.createDataFrame(agrupados, ["assunto", "valor"])
#df.select("assunto", "valor").orderBy("valor", ascending=False).show()
#df.createOrReplaceTempView("most_called")

#############################
# Cria lista no Driver

chaves = []
valores = []
for chave, valor in agrupados:
    chaves.append(chave)
    valores.append(valor)
# print(chaves)
# print(valores)

import matplotlib.pyplot as plt
# Plot
plt.figure(figsize=(5,5))
plt.rcParams.update({'font.size': 8})
plt.pie(valores,  labels=zip(chaves, valores))
# plt.pie(valores,  labels=chaves)
plt.show(bbox_inches='tight')
