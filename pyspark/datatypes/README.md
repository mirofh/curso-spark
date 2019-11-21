# Abstração de Dados (Variáveis)

Antes de Spark 2.0, a principal interface de programação do Spark era o [Resilient Distributed Dataset](https://spark.apache.org/docs/latest/rdd-programming-guide.html) (RDD). Em versões depois do Spark 2.0, o RDD foi substituido pelo Dataset, que é fortemente tipado, como o RDD, mas que tem melhores optimizações. A interface RDD ainda é suportada, mas você pode ter mais detalhes em seu [Manual de Programação](https://spark.apache.org/docs/latest/rdd-programming-guide.html). Entretanto, a databricks recomenda que todos os usuários usem o tipo Dataset, que tem melhor performance que o RDD.

A primeira abstração de dados do Spark é uma coleção distribuída de dados chamada Dataset. Datasets podem ser criados a partir de  Hadoop InputFormat (como arquivos do HDFS) ou de outros Datasets por meio de transformações. Por causa da natureza do Python, os Datasets não precisam ser fortemente tipados em python. Consequentemente, todos os Datasets em Python são da classe *Dataset[Row]*, que é denominado DataFrame (para ser consistente com o conceito de DataFrames do Pandas em R).

# Dados de Exemplo

Todos os arquivos se encontram em [curso-spark/data/](../../data/). Você encontrará alguns dados da cidade de Curitiba. Graças ao [c3sl](http://c3sl.ufpr.br) você pode baixar versões mais atualizadas, entre outros dados, pelo projeto de [Dados Abertos](http://dadosabertos.c3sl.ufpr.br/). Demais dados (e.g., municipios) foram retirados do projeto de dados abertos do [Governo Federal](http://dados.gov.br/).

- [156](http://dadosabertos.c3sl.ufpr.br/curitiba/156/): Base de Dados contendo as solicitações geradas na Central 156, principal canal de comunicação entre o cidadão e a Prefeitura Municipal de Curitiba. Inclui todas as demandas direcionadas às Secretarias e Órgãos da Administração Municipal. Estes dados são oriundos do  Sistema Integrado de Atendimento ao Cidadão - SIAC.
- [alvaras](http://dadosabertos.c3sl.ufpr.br/curitiba/BaseAlvaras/): Relação de alvarás para liberação de atividades comercias e edificações dentro do município de Curitiba.
- municipios: Dados sobre todos os municípios do Brasil.
- bible: Uma versão da Bíblia em texto puro.
- discursos: Discursos de personalidades históricas.

Vamos tomar como exemplo os dados dos municípios do Brasil. Temos dados salvos em três formatos: csv, xml e json.

- data/municipios/municipios.csv: Contém informações geográficas e código do IBGE para cada município do Brasil.

```csv
| UF| Nome_UF|Mesorregiao Geografica| Nome_Mesorregiao|Microrregiao Geografica|Nome_Microrregiao|Municipio|Codigo Municipio Completo| Nome_Municipio|
```

- *data/municipios/municipios-geo.json*: Localização geográfica de cada município.

```csv
|capital|codigo_ibge|codigo_uf| estado|latitude|longitude| nome_municipio| uf|
```
- *data/municipios/municipios-mcmv.xml*: Quantidade de unidades financiadas pelo Minha Casa Minha Vida em cada município.

```xml
    <entry>
      <unidades>36</unidades>
      <municipio_ibge>354370</municipio_ibge>
      <ano>2014</ano>
    </entry>
```

- *data/municipios/municipios-pib.xml*: Contém informações sobre o PIB de cada município do Brasil.

```xml
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
```

- *data/municipios/municipios-transf.csv*: Todas as transferências feitas do governo federal para os municípios desde 1997 até 2018, dividídas por decêndios."

```csv
| municipio| uf| ano|mes| decendio|2 decendio|3 decendio| item transferencia|transferencia| _c9|
```

- *data/municipios/municipios-base.csv*: Contém informações como nome do município, estado a qual ele pertence e seus respectivos códigos.

```csv
"nome_uf";"code_uf";"municipio_macro";"municipio_micro";"municipio_nome"
```

# Acessando os Dados
