# Curso de PySpark


## Glossário

Spark [https://spark.apache.org/docs/latest/cluster-overview.html](define) suas aplicações e serviços em:

| Termo           | Descrição                                                                                                                                                                                                                                                |
|-----------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Application     | Programa do usuário construído com a API do Spark. Consiste de um programa Driver e um programa Executor.                                                                                                                                                |
| Application jar | Um jar contendo a aplicação do usuário. Em alguns casos os usuários criam um .jar contendo a aplicação com todas as dependências requeridas. Este jar não deve conter as bibliotecas do Hadoop ou Spark, pois estas são adicionadas em tempo de execução |
| Driver program  | O processo que executa a função main() da Application. Contém o contexto do Spark.                                                                                                                                                                       |
| Cluster manager | Um serviço externo para aquisição de recursos em um cluster (standalone manager, Mesos, YARN)                                                                                                                                                            |
| Deploy mode     | Distingue onde o processo Driver é executado. No modo “cluster” o Driver é executado no cluster. No modo “client”, o Driver não é executado no cluster, mas na máquina do usuário.                                                                       |
| Worker          | Qualquer nó do cluster que pode executar a Application.  Executa tarefas e armazena os dados.                                                                                                                                                            |
| Executor        | Um processo iniciado pela Application em um nó Worker. Cada Application tem seus próprios Executors.                                                                                                                                                     |
| Task            | Uma tarefa que é enviada ao Executor para ser processada.                                                                                                                                                                                                |
| Job             | A coleção de tarefas (Tasks) que são iniciadas no cluster em resposta a uma ação do Spark.                                                                                                                                                               |
| Stage           | Cada Job é dividito em conjunto de tarefas (Tasks) que são chamadas de Stages.                                                                                                                                                                           |


![Cluster Overview](https://spark.apache.org/docs/latest/img/cluster-overview.png "Cluster Overview")


## Tipos de Gerenciadores de Cluster

[Cluster Overview](https://spark.apache.org/docs/latest/cluster-overview.html)

O Spark (v2.4) suporta diversos gerenciadores de recursos:

    - Standalone:  Um gerenciador de cluster simples que acompanha a distribuição do Spark.
    - Apache Mesos: Um gerenciador de cluster genérico que pode executar Hadoop MapReduce entre outros serviços.
    - Hadoop YARN: O gerenciador de recursos do Hadoop 2.
    - Kubernetes: Um gerenciador de recursos de código aberto para aplicação em containers.

## Enviando aplicações ao Spark para serem processadas.

```bash
./bin/spark-submit \
  --class <main-class> \
  --master <master-url> \
  --deploy-mode <deploy-mode> \
  --conf <key>=<value> \
  ... # other options
  <application-jar> \
  [application-arguments]
```

As opções mais comuns são:

- --class: A classe escrita pelo usuário (e.g. org.apache.spark.examples.SparkPi).
- --master: O endereço do serviço do Spark (e.g. spark://127.0.0.1:7077)
- --deploy-mode: Onde o Driver será executado, se no *cluster* ou no *cliente* (default).
- --conf: Quaisquer outras configurações do Spark.
- application-jar: Caminho para o jar da aplicação escrita pelo usuário.
- application-arguments: Argumentos que serão repassados a classe do usuário a ser executada.

# Exemplos

- [Tipos de dados](datatypes)
- [Ações](actions)
- [Transformações](transformations)
