## Exemplo 3
#
# Exemplo:
#   1. Utilizar spark para descobrir qual são os 10 bairros com mais
#      chamadas ao 156.
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
dicionario = sc.textFile("file:///home/erlfilho/git/zeppelin/data/156/156-dicionario.csv")

for line in dicionario.collect():
    print(line)

tipo = data.map(lambda x: (x.split(';')[9].encode('utf-8').replace("\"", "").strip(), 1))
print(tipo.take(3))

aggr = tipo.reduceByKey(lambda x,y: x+y).filter(lambda x: '' not in x).takeOrdered(10, lambda x: x[1] * -1)

keys = []
values = []
for key, value in aggr:
    keys.append(key)
    values.append(value)

import matplotlib.pyplot as plt

# Plot
plt.figure(figsize=(5,5))
plt.rcParams.update({'font.size': 8})
plt.pie(values,  labels=aggr)
# plt.pie(values,  labels=keys)
plt.show(bbox_inches='tight')
