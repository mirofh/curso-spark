## Exemplo 4
#
# Exemplo:
#   1. Clusterizar as palavras de discursos de personagens historicos
#
# Dados:
#       *
#
# Estes exemplos usam ações e transformações
#
# Exercício:



# ftp://ftp.cs.princeton.edu/pub/cs226/textfiles/bible.txt
rdd = sc.textFile("file:///home/erlfilho/git/zeppelin/data/discursos/churchill/*.txt")
words = rdd.map(lambda phrase: phrase.split(' '))
words.take(1)

# stop words
sw = sc.textFile("/data/stopwords.txt")
stopwords = sw.collect()
lowercase = words.map(lambda x: [i.lower() for i in x])
wo_stopwords = lowercase.map(lambda x: [i for i in x if i not in stopwords])
wo_emptywords = wo_stopwords.map(lambda x:  [i for i in x if len(i)>0 ])
wo_specialchars = wo_emptywords.map(lambda x: [ ''.join(c for c in w if c.isalnum()) for w in x ])
parsedRDD = wo_specialchars.map(lambda x: (x,))
parsedRDD.take(1)

df  = spark.createDataFrame(parsedRDD, ["text"])
df.show()

from pyspark.ml.feature import Word2Vec

word2vec = Word2Vec(vectorSize=300, minCount=0, inputCol="text", outputCol="features")
model = word2vec.fit(df)
results = model.transform(df)
results

vectors = model.getVectors()
vectors.take(1)

model.findSynonyms("eat", 10).show()
print("so pra nao dar erro")

from pyspark.ml.clustering import KMeans

import math
nmeans = int(math.floor(math.sqrt(float(vectors.count()/2))))
kmeans = KMeans(k=nmeans, seed=1, featuresCol='vector')
kmodel = kmeans.fit(vectors)
kmodel

kclusters = kmodel.transform(vectors)
kclusters.printSchema()

from pyspark.sql import functions as F
clusters = kclusters.select("prediction", "word").groupBy("prediction").agg(F.collect_list("word")).orderBy("prediction",ascending=True)

clusters.rdd.map(lambda x: [x[0], len(x[1])]).collect()
clusters.take(2)

wordlist = clusters.select("collect_list(word)").rdd.map(lambda x: x[0]).collect()
print(wordlist[1:10])

from wordcloud import WordCloud, ImageColorGenerator
import matplotlib.pyplot as plt

def plot_cloud (text):
    text = " ".join(w for w in text)
    wordcloud = WordCloud(background_color='white').generate(text)
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis("off")
    plt.show()

for w in wordlist:
    # print(w)
    #if len(w) > 10:
    if "russia" in w:
        plot_cloud(w)
