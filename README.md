# Curso de Spark

Curso de Spark para a especialização em Big Data da UFPR

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
In case you want Spark to use YARN:

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
<property>                                                                                                                                                                                                             <name>zeppelin.notebook.mongo.uri</name>                                                                                                                                                                             <value>mongodb://localhost</value>                                                                                                                                                                                   <description>MongoDB connection URI used to connect to a MongoDB database server</description>                                                                                                                     </property>    
```

Depois de configurar, inicie o serviço do Zeppelin.

```bash
./bin/zeppelin-daemon.sh start
```
