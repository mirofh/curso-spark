# Curso de Spark

Curso de Spark para a especialização em Big Data da UFPR

<erlfilho@gmail.com>

# Configurando o Spark no Linux

As instruções aqui descritas seguem o [manual oficial de instalação (standalone)](https://spark.apache.org/docs/latest/spark-standalone.html).

## Download & Unpacking

```bash
wget -c https://www-us.apache.org/dist/spark/spark-3.0.0-preview/spark-3.0.0-preview-bin-hadoop3.2.tgz
tar xvzf spark-3.0.0-preview-bin-hadoop3.2.tgz
mv spark-3.0.0-preview-bin-hadoop3.2/ spark
cd spark

vim ~/.bash\_aliases
SPARK\_HOME="/home/${USER}/spark/"
PATH=${SPARK\_HOME}/bin:${SPARK\_HOME}/sbin:$PATH
```
## Spark executando sozinho (Standalone)

Para iniciar todos os serviços necessários, configure todos os 
nós escravos no arquivo *conf/slaves* (um por linha).
Caso esteja configurando em sua máquina, este arquivo conterá apenas *localhost*.

```bash
vim conf/slaves

# inicia todos os serviços do Spark (mestre e escravos)
start-all.sh
```
Para iniciar os serviços individualmente você pode:

```bash
# Este comando inicia a aplicação mestre do Spark e
# imprime o endereço onde o Spark está escutando (master-spark-URL)
./sbin/start-master.sh

# Para connectar os escravos ao mestre 
./sbin/start-slave.sh <master-spark-URL>
```

### Spark executando com YARN

Caso você queira executar o Spark no [http://ftp.unicamp.br/pub/apache/hadoop/common/hadoop-3.2.1/](YARN), siga estas instruções para configurar o [https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html#Standalone_Operation](Hadoop com YARN). Então, [https://spark.apache.org/docs/latest/running-on-yarn.html](configure o Spark).
Utilize Java 8.

```bash
export HADOOP_CONF_DIR=/home/${USER}/hadoop/etc/hadoop
export SPARK_HOME=/home/${USER}/spark
export LD_LIBRARY_PATH=/home/${USER}/hadoop/lib/native:$LD_LIBRARY_PATH

cp ${SPARK\_HOME}/conf/spark-defaults.conf.template ${SPARK\_HOME}/conf/spark-defaults.conf
echo "spark.master yarn" >> ${SPARK\_HOME}/conf/spark-defaults.conf
echo "spark.driver.memory 512m" >> ${SPARK\_HOME}/conf/spark-defaults.conf
```

# Executando Spark 

Todos os shells disponíveis pelo estão em *${SPARK_HOME}/bin*, 

```bash
pyspark
sparkR
spark-shell
spark-sql
spark-submit
```

# Instalando o Zeppelin 

[Zeppelin](https://zeppelin.apache.org/docs/0.6.0/install/install.html#starting-apache-zeppelin-with-command-line)


```bash
wget -c http://mirror.nbtelecom.com.br/apache/zeppelin/zeppelin-0.8.2/zeppelin-0.8.2-bin-all.tgz
tar xvzf zeppelin-0.8.2-bin-all.tgz 
cd zeppelin-0.8.2-bin-all
```

Você pode garantir que todos os interpretadores estão instalados e instalar possiveis interpretadores que estejam faltando.

```bash
./bin/install-interpreter.sh --list
./bin/install-interpreter.sh -n python,pig,bigquery
```

```bash
cp conf/zeppelin-env.sh.template conf/zeppelin-env.sh
cp conf/zeppelin-site.xml.template conf/zeppelin-site.xml
vim -O conf/zeppelin-env.sh  conf/zeppelin-site.xml
```

Atualize a porta do Zeppelin para não ser a mesma do Spark (8080)

```xml
<property>
  <name>zeppelin.server.port</name>
  <value>8081</value>
  <description>Server port.</description>
</property>
```

Todos os storages (Google, Amazon, Azure) bem como acesso ao Hadoop e MongoDB, são configurados no arquivo *conf/zeppelin-site.xml*.

```xml
<property>
    <name>zeppelin.notebook.mongo.uri</name>
    <value>mongodb://localhost</value>
    <description>MongoDB connection URI used to connect to a MongoDB database server</description>
</property>
```

Depois de configurar, inicie o serviço do Zeppelin.

```bash
./bin/zeppelin-daemon.sh start
```


# Configurando o Zeppelin

conf/zeppelin-env.sh

```bash
export JAVA_HOME="/usr/lib/jvm/java-8-oracle/"
export MASTER="spark://10.254.231.59:7077"
export ZEPPELIN_MEM="-Xmx4096m -XX:MaxPermSize=512m"
export SPARK_HOME="/home/erlfilho/git/spark/"
export SPARK_SUBMIT_OPTIONS="--driver-memory 4G --executor-memory 4G --packages com.databricks:spark-csv_2.10:1.2.0,com.databricks:spark-xml_2.11:0.4.1,com.github.fommil.netlib:all:1.1.2"
export PYSPARK_PYTHON="/usr/bin/python2.7"
export PYTHONPATH="${SPARK_HOME}/python:$PYTHONPATH"
```


```xml
<property>
  <name>zeppelin.server.addr</name>
  <value>10.254.231.59</value>
  <description>Server binding address</description>
</property>

<property>
  <name>zeppelin.server.port</name>
  <value>8085</value>
  <description>Server port.</description>
</property>
```
