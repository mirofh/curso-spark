#
# Documentação oficial:
# https://spark.apache.org/docs/latest/api/python/pyspark.html#pyspark.RDD
#

########################################
# Ações
########################################

########################################
# parallelize()
# Paraleliza dados no Spark (cria RDDs)
# - RDD é uma coleção distribuída e imutável
# - RDD é dividido em múltiplas partições.
# - RDD pode conter qualquer tipo de objeto.
# - Existem dois super grupos:
#     * RDDs: são operados por diversas transformações
#     * PairRDD: tem transformações especiais que consideram <chave, valor>
#
# Documentação oficial:
# - https://spark.apache.org/docs/latest/api/python/pyspark.html#pyspark.RDD

import random
data = [ random.randint(100,999) for i in range(0,1000)]
x = sc.parallelize(data)

########################################
# x resulta em ParallelCollectionRDD()
x

# Criando um RDD, ou ainda, distribuindo uma colecao
# x resulta em ParallelCollectionRDD()
# x = sc.parallelize(["b", "a", "c"])

########################################
# getNumPartitions()
# retorna o número de partições de um RDD

y = x.getNumPartitions()
print(y)

########################################
# count()
# conta quantos elementos existem no RDD

y = x.count()
print(y)

########################################
# mean()
# calcula a média dos elementos no RDD

y = x.mean()
print(y)

########################################
# first()
# retorna o primeiro elemento do RDD

y = x.first()
print(y)

########################################
# take()
# retorna n elementos do RDD

y = x.take(6)
print(y)

########################################
# max()
# retorna o maior elemento do RDD

y = x.max()
print(y)

########################################
# min()
# retorna o menor elemento do RDD

y = x.min()
print(y)

########################################
# sum()
# calcula a soma

y = x.sum()
print(y)

########################################
# stdev()
# calcula o desvio padrão

y = x.stdev()
print(y)

########################################
# takeSample()
# retorna uma amostra do RDD

# rdd.takeSample(withReplacement, number, seed)
#x.takeSample(False, 3, 12345)

x.takeSample(True, 20, 1)

########################################
# takeOrdered()
# se a função key() retorna um valor negativo, a order é decrescente.

# crescente
x.takeOrdered(10, lambda s:  s )

# decrescente
x.takeOrdered(10, lambda s:  s * -1)

########################################
# aggregate()
# agrega todos os elementos de um RDD por:
#    1. aplicar uma  função do usuário para combinar os elementos dos RDDs
#    2. combinar os resultados por uma segunda função
#    3. retornar um resultados para o driver

seqOp = lambda data, item: (data[0] + [item], data[1] + item)
combOp = lambda d1, d2: (d1[0] + d2[0], d1[1] + d2[1])

def seqOp(data, item):
    return (data[0] + [item], data[1] + item)

def comOp(d1, d2):
    return (d1[0] + d2[0], d1[1] + d2[1])

#x = sc.parallelize([1,2,3,4])
y = x.aggregate(([], 0), seqOp, combOp)

print(x.collect())
print(y)

########################################
# foreach()
# Executa uma função qualquer sobre um RDD
# É utilizado geralmente para:
# 1. atualizar um acumulador
# 2. interagir com  sistemas de arquivos externos.

def par (n):
    if n % 2 == 0:
        return n

x.foreach(par)

########################################
# reduce()
# reduz os elementos do RDD original usando uma funcao cumulativa

x = sc.parallelize([1, 2, 3, 4, 5])
x.reduce(lambda x,y: x+y)

########################################
# Transformações
########################################

########################################
# map()
# retorna um novo RDD após aplicar uma dada função em cada elemento do RDD

x = sc.parallelize(["b", "a", "c"])
y = x.map(lambda z: (z, 1))
print(x.collect())
print(y.collect())

########################################
# filter()
# retorna um novo RDD contendo somente elementos que safisfaçam um predicado.

x = sc.parallelize([1,2,3])

# mantém valores ímpares
impar = x.filter(lambda x: x%2 == 1)
print(impar.collect())

def pega_pares(x):
    if x % 2 == 0:
        return x

# mantém valores pares
par = x.filter(pega_pares)
print(par.collect())

########################################
# flatMap()
# retorna um novo RDD após primeiramente aplicar uma função em cada elemento do
# RDD e depois nivelar os resultados

# map é uma transformação um-pra-um. Transforma cada elemento de um RDD em um outro elemento.
# flatMap é uma transformação um-para-muitos. Transforma cada elemento de um RDD em um ou mais elementos.

x = sc.parallelize([1,2,3])
y = x.flatMap(lambda x: (x, x*100, 42))
print(x.collect())
print(y.collect())

########################################
# GroupBy()
# Agrupa os dados do RDD original.
# Cria pares onde:
#     a chave é a saída de uma função do usuário
#     o valor são todos os itens quais satisfazem essa função

x = sc.parallelize(['John', 'Fred', 'Anna', 'James'])
y = x.groupBy(lambda w: w[0])

print(x.collect())
print(y.collect())
print [(k, list(v)) for (k, v) in y.collect()]

########################################
# GroupByKey()
# Agrupa valores para cada chave no RDD original.
# Cria um novo par onde a chave original corresponde a esse grupo de valores
# coletados.

# groupByKey() é executado sobre PairRDDs e é usado para agrupar todos os
# valores relacioados a uma dada chave.
# groupBy() pode ser usado em ambos unpaired e paired RDDs.

x = sc.parallelize([('B',5),('B',4),('A',3),('A',2),('A',1)])
y = x.groupByKey()
print(x.collect())
print(list((j[0], list(j[1])) for j in y.collect()))


########################################
# reduceByKey()
x = sc.parallelize([('B',5),('B',4),('A',3),('A',2),('A',1)])
y = x.reduceByKey(lambda x,y: x + y)
print(x.collect())
print(y.collect())

########################################
# glom()
# retorna um RDD com a uniao de todos os elementos de cada particao em uma lista

x = sc.parallelize([ i for i in range(0,10)], 5)
print(x.getNumPartitions())
print(x.collect())
print(x.glom().collect())

########################################
# mapPartitions()
# retorna um novo RDD após aplicar uma função para cada partição do RDD original

# parallelize(dados, particoes)
x = sc.parallelize([1,2,3], 2)

# yield é um comando como o return, mas retorna um generator
# um generator é um iterable, mas que pode ser operado uma única vez.

def f(iterator):
    yield sum(iterator);
    yield 42

y = x.mapPartitions(f)

print(x.collect())
print(y.collect())
print(x.glom().collect())
print(y.glom().collect())

########################################
# union()
# Retorna um novo RDD contendo todos os itens dos dois RDDs originais.
# Items duplicados não são selecionados

x = sc.parallelize([1,2,3], 2)
y = sc.parallelize([3,4], 1)
z = sc.parallelize([3,4], 1)

xy = x.union(y)
yz = y.union(z)

print(xy.glom().collect())
print(yz.glom().collect())

########################################
# join()
# retorna um novo RDD contendo todos os pares de elementos tendo uma mesma chave

x = sc.parallelize([("a", 1), ("b", 2)])
y = sc.parallelize([("a", 3), ("a", 4), ("b", 5)])
z = x.join(y)
# saida: [('a', (1, 3)), ('a', (1, 4)), ('b', (2, 5))]

print(z.collect())

########################################
# cartesian()
# Retorna o produto cartesiano entre dois RDDs.

# problemas:
# ocupa muita memoria

x = sc.parallelize([1,2,3,4])
y = sc.parallelize(["a","b","c"])

z = x.cartesian(y)
print(z.count())
print(z.collect())

########################################
# distinct()
# retorna um novo RDD contendo todos os items do RDD original, mas omitindo as replicas.

x = sc.parallelize([1,2,3,3,4])
y = x.distinct()
print(y.collect())

########################################
# coalesce()
# retorna um novo RDD, qual é reduzido para um número menor de partições
# poderiam ser chamado de partitionUnion/partitionAggregation?

x = sc.parallelize([1, 2, 3, 4, 5], 3)
y = x.coalesce(2)
print(x.glom().collect())
print(y.glom().collect())

########################################
# keyBy()
# cria um novo PairRDD, formando um par para cada item no RDD original.
# a chave é calculada por uma função do usuário

x = sc.parallelize(['John', 'Fred', 'Anna', 'James'])
y = x.keyBy(lambda w: w[0])
print y.collect()

########################################
# partitionBy()
# retorna um novo RDD com o número especificado de partições, colocando
# alguns items originals na partição retornada, caso atendar uma função do usuário

x = sc.parallelize([('J','James'),('F','Fred'), ('A','Anna'),('J','John')], 3)
y = x.partitionBy(2, lambda w: 0 if w[0] < 'H' else 1)

print x.glom().collect()
print y.glom().collect()

########################################
# zip()
# retorna um novo RDD contendo pares onde
#    a chave é o item no RDD original
#    o valor é o elemento correspondente ao item (mesma partição, mesmo índice) em um segundo RDD

x = sc.parallelize([1, 2, 3])
y = x.map(lambda n: n*n)
z = x.zip(y)

print(x.collect())
print(y.collect())
print(z.collect())

