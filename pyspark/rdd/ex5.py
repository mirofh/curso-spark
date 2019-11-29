## Exemplo 4
#
# Exemplo:
#   1. Este exemplo contém a caracterização das palavras da Biblia.
#
# Dados:
#      * Texto da Bíblia, disponibilizado pela Universidade de Princeton.
#        Não contém a indexação de versículos e capítulos.
#        ftp://ftp.cs.princeton.edu/pub/cs226/textfiles/bible.txt
#
# Exercício:
#       *
#       *
#
# Word2Vec:
#   Cria um vetor de representacao das palavras de um dado texto. O algoritmo
#   constroi um vocabulario a partir do texto. Depois, aprende os vetores de
#   representacao de cada palavra do vocabulario. O vetor de representacao pode ser
#   usado como um vetor de caracteristicas em Processamento de Linguagem Natural.
#
# Documentação do Word2Vec:
#   - Para a implementacao original em C, veja https://code.google.com/p/word2vec/
#   - Para a documentacao do Word2Vec in PySpark:
#     https://spark.apache.org/docs/latest/mllib-feature-extraction.html#word2vec
#   - Para o artigo original: https://dl.acm.org/citation.cfm?id=2999959

# le o dataset
rdd = sc.textFile('file:///home/erlfilho/git/zeppelin/data/bible/bible.txt')
rdd.take(10)

# divide cada linha em palavras
words = rdd.map(lambda x: x.split(' '))
words.take(10)


# le as lista de palavras nao permitidas (stop words) em um array
sw = sc.textFile('file:///home/erlfilho/git/zeppelin/data/bible/stopwords.txt')
sw.count()
stopwords = sw.collect()
print(stopwords)

###################################
# tratamento do texto

# transforma todas as palavras em minusculas
lowercase = words.map(lambda x: [i.lower() for i in x])
lowercase.take(10)

# remove todas as 'stop words' do texto
wo_stopwords = lowercase.map(lambda x: [ word for word in x if word.lower() not in stopwords ])
wo_stopwords.take(10)

# remove palavras vazias
wo_emptywords = wo_stopwords.map(lambda x: [ i for i in x if len(i)>0 ])
wo_emptywords.take(10)

# remove caracteres especiais
wo_specialchars = wo_emptywords.map(lambda _list: [ ''.join(_char for _char in _word if _char.isalnum()) for _word in _list ])
wo_specialchars.take(10)

###################################
# TODO: produz erro ao transformar para DataFrame
#parsedRDD = wo_specialchars

# transforma para tupla, o formato necessario para criar um RDD
parsedRDD = wo_specialchars.map(lambda x: (x,))
parsedRDD.take(1)

# Transforma o RDD em um DataFrame
# o DF contem uma coluna chamada Texto
# cada linha da coluna texto contem uma lista de palavras
# cada lista de palavras representa as palavras de um versiculo
df = spark.createDataFrame(parsedRDD, ["text"])
df.select("text").show()


###################################
#

# importa-se a biblioteca word2vec do PySpark
from pyspark.ml.feature import Word2Vec

# configura o modelo
# - vectorSize: o tamanho do vetor de caracteristicas 
# - minCount: o numero minimo de que uma palavra deve ocorrer para fazer parte do vocabulario (o padrao e 5)
# - inputCol: nome da coluna contendo as palavras
# - outputCol: nome da coluna contendo o vetor de caracteristicas
word2vec = Word2Vec(vectorSize=300, minCount=0, inputCol="text", outputCol="features")

# aprendizagem do modelo
model = word2vec.fit(df)

# transformacao de palavras para vetor
results = model.transform(df)
results.printSchema()

###################################
#

# word2vec fornece um vetor de caracteristica para cada palavra
# para recuperar o vetor de cada palavra basta:
vectors = model.getVectors()
print(vectors.count())
vectors.show()

# word2vec permite calcular-mos sinonimos
model.findSynonyms("jesus", 20).show()


###################################
# Mostre os sinonimos de uma dada palavra
# de acordo com a distancia coseno (cosineDistance)
good = ["jesus","john","mark","luke","saint","jerusalem","heaven"]
evil = ["hell","baal","devil","evil","egypt","fear","mourn"]

for w in evil:
    print "synonyms for: ", w
    model.findSynonyms(w, 5).show()


###################################
# word2vec cria um vetor de caracteristica para cada palavra
# que possibilita calcular distancia entre palavra e encontrar seus sinonimos.
# Entretanto, nao podemos fazer agrupamento de palavras similares.
# para tal, utilizamos qualquer metodo de clusterizacao.
#
# o exemplo abaixo emprega o k-means sobre os vetores de caracteristicas
#
# Documentacao oficial da versao deste curso:
# https://spark.apache.org/docs/2.3.2/api/python/pyspark.ml.html#pyspark.ml.clustering.KMeans

# importamos o kmeans
from pyspark.ml.clustering import KMeans

# a regra de ouro para determinar o k: sqrt(n/2)
import math
nmeans = int(math.floor(math.sqrt(float(vectors.count()/2))))
#nmeans = 50

# criamos uma instancia do k-means com:
# - k: representa o numero de clusters a serem criados pelo k-means
# - seed: representa o valor da semente para gerar aleatoriamente os valor inicial de cada particula
# - featuresCol: coluna com as caracteristicas a ser utilizada na clusterizacao
kmeans = KMeans(k=nmeans, seed=1, featuresCol='vector')

# treinamos o modelo do kmeans
model_kmeans = kmeans.fit(vectors)

# realiza a clusterizacao
kmeans_clusters = model_kmeans.transform(vectors)
kmeans_clusters.count()

# o kmeans retorna um DataFrame com os valores transformamos rotulados com as classes do k-means
kmeans_clusters.show()

# e preciso agrupar cada palavra por seu cluster [<cluster-id>, [words]]
from pyspark.sql import functions as F
clusters = kmeans_clusters.select("prediction", "word").groupBy("prediction").agg(F.collect_list("word")).orderBy("prediction", ascending=True)
clusters.show()

# Quantos elementos existem em cada cluster?
# retorna uma lista [<cluster-id>,<quantidade>]
clusters.rdd.map(lambda x: [x[0], len(x[1])]).collect()

# Quantos elementos existem em cada cluster?
# retorna uma lista [<quantidade>]
# clusters.select("collect_list(word)").rdd.flatMap(lambda x: list(x)).map(lambda x: len(x)).collect()


# note que cada elemento desta lista e do tipo Row
#clusters.select("collect_list(word)").take(1)

# transformamos o valor de Row para List
clusters_list = clusters.select("prediction").rdd.map(lambda x: x[0]).collect()
words_list = clusters.select("collect_list(word)").rdd.map(lambda x: x[0]).collect()

# mostra todos os valores
#for k,v in clusters.collect():
#    print(k, list(v))


###################################
# Criar a nuvem de palavras
from wordcloud import WordCloud, STOPWORDS, ImageColorGenerator
import matplotlib.pyplot as plt

# recebe uma lista de palavras
def plot_cloud (text):
    text = " ".join(w for w in text)
    print(text)

    # cria uma nuvem-de-palavras
    # wordcloud = WordCloud(background_color="white",max_font_size=20).generate(text)
    wordcloud = WordCloud(background_color="white").generate(text)

    # mostra a imagem gerada
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis("off")
    plt.show()

for w in words_list:
    if "gold" in w:
    #if 20 < len(w) and len(w) < 30:
    #if len(w) > 300:
        plot_cloud(w)

#index=3
#print(words_list[index])
#plot_cloud(words_list[index])
