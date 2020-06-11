---
layout: post
title: "Implementando uma pipeline de dados simples em Python"
subtitle: "Aprendendo como implementar uma rotina em Python seguindo o processo ETL no dataset público de cotações de fundos de investimento."
date: 2020-06-11 18:00:13 -0300
author: Matheus Batista
background: 'img/post_etl_bg.jpg'
---

# Introdução

Nesse tutorial vamos aprender o básico sobre as etapas de uma rotina de ETL(extract, transform and load). A ideia aqui é utilizar um dataset público real para demonstrar como implementar as diferentes etapas dessa rotina. É importante lembrar que esse tutorial exige um conhecimento básico de [Python](https://www.python.org/) e [Jupyter Notebook](https://jupyter.org/). As ferramentas necessárias para seguir esse tutorial são:

- [Python 3.6 ou maior](https://www.python.org/downloads/);
- [Jupyter Notebook](https://jupyter.org/install);
- [Pip](https://pip.pypa.io/en/stable/installing/).


Após instalar o `pip` é necessário instalar as dependências descritas no arquivo `requiremets.txt`. Sugiro utilizar um [ambiente virtual do python](https://docs.python.org/3/library/venv.html) para isolar a instalação das dependências. Para seguir com a instalação execute:

```bash
$ pip install -r requirements.txt
```

## A fonte de dados

Escolhi a fonte de dados abertos da CVM contendo a cotação diária dos fundos de investimentos negociados no mercado brasileiro. Essa fonte é atualizada diariamente com os dados de fechamento do dia anterior e as contações são agrupadas por arquivos correspondentes a cada mês do ano.

Esse é um cenário bem comum em fontes de dados abertas, uma série de arquivos no formato CSV agrupando informações de acordo com a data, então a solução que vamos implementar durante o tutorial é reaproveitável para outras fontes de dados.

Primeiro é importante analisar a fonte de dados, entender como ela está estruturada, quais campos compõem o dataset e como nós podemos automatizar a coleta dos dados.

A fonte de dados contendo a cotação diária dos fundos pode ser acessada através do [portal de dados abertos](http://www.dados.gov.br) pelo link:

[http://www.dados.gov.br/dataset/fi-doc-inf_diario](http://www.dados.gov.br/dataset/fi-doc-inf_diario)

Aqui tem uma descrição rápida sobre as informações que existem no dataset:
![](/img/posts/descricao_dataset.png)

Além de um link para o recurso contendo o dicionário de dados que descreve cada campo do dataset, é **sempre muito importante analisar o dicionário de dados do dataset, se disponível.** Não vou entrar em detalhes sobre as descrições do dicionário para não estender o tutorial, mas sugiro que você olhe para ser familiarizar com esse tipo de formato.

![](/img/posts/link_dicionario.png)

## Como automatizar o download 

Agora partindo para como nós podemos automatizar a extração dos datasets, podemos observar que no site existe um link para todos os datasets dos últimos 12 meses. Primeiro precisamos analisar se existe um padrão nas urls de download dos arquivos, para isso vamos copiar alguns endereços e compará-los.

Para copiar os endereços clique com o botão direito no botão **"Ir para recurso"** e selecione a opção **"Copiar link"**:

![](/img/posts/link_recurso.png)

Copiei três links e vamos compará-los a seguir:

[http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_201907.csv](http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_201907.csv)

[http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_201910.csv](http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_201910.csv)

[http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_202003.csv](http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_202003.csv)

Como podemos observar existe um padrão claro nos links:

`dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS/inf_diario_fi_YYYYMM.csv`

A única variação é o mês e o ano de referência do dataset. Com essa análise nossa tarefa de automatizar o download dos datasets ficou mais simples, porque agora podemos gerar sistematicamente o intervalo de datas que queremos baixar.

# Let's CODE

## Extração

O processo de extração consiste nesse caso em fazer o download de todos os arquivos da janela de tempo q nos interessa. As etapas pra nós atingirmos esse objetivo são:

1. Automatizar a geração do nome dos arquivos: já que a única coisa que varia nos links é o nome dos arquivos precisamos automatizar a geração desses nomes de acordo com a janela de tempo do nosso interesse;

2. Requisitar o arquivo: precisamos enviar uma requisição para o portal de dados abertos do arquivo que queremos fazer o download;

3. Salvar arquivo: o portal de dados abertos vai nos enviar o arquivo requisitado e precisamos salvá-lo no nosso computador.

### Gerando datas


```python
from datetime import datetime, timedelta
```

Primeiramente vamos criar uma função para facilitar a criação da lista de datas que nós queremos baixar do site:


```python
def generate_dates(initial_date, final_date, increment, format):
    """
    Função para gerar uma lista de datas no formato string.
    
    Parâmetros:
        initial_date = Data inicial da contagem;
        
        final_date = Data final da contagem;
        
        increment = Quanto deve ser incrementado da data, em dias, para cada iteração;
        
        format = Formato da data de saída obedecendo os códigos disponíveis na documentação do python:
                 https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes
    """
    date_list = []
    
    while initial_date < final_date:
        date_list.append(initial_date.strftime(format))
        initial_date = initial_date + timedelta(days=increment)
        
    return date_list
```

### Fazendo a requisição e salvando os arquivos 

Vamos trabalhar agora com a biblioteca python [requests](https://requests.readthedocs.io/en/master/) que facilita a implementação de requisições HTTP, que é essencialmente o tipo de requisição que precisamos fazer para baixar os arquivos da nossa fonte de dados. 

Para fazer a requisição utilizaremos o método `requests.get()` que recebe como primeiro parâmetro a URL que queremos requisitar, podemos então checar se ocorreu algum erro através do [_status code_](https://developer.mozilla.org/en-US/docs/Web/HTTP/Status) da resposta, o _status code_ `200` significa que a requisição foi bem sucedida. No fim, o conteúdo da resposta pode ser acessado através do atributo `.content`. Recomendo explorar a excelente [documentação da biblioteca](https://requests.readthedocs.io/en/latest/) caso queira conhecer melhor.

```python
>>> import requests

>>> response = requests.get(url)

>>> response.content
b"Conteúdo da página"
```


```python
import requests
```


```python
def download_files(dates, file_base_name='inf_diario_fi', dir='data/'):
    """
    Função para fazer o download de todos os arquivos das datas disponibilizadas.
    
    Parâmetros:
        dates = Lista de datas;
        
        file_base_name = Nome base para os arquivos que serão salvos;
        
        dir = Diretório onde os arquivo serão salvos.
    """
    saved_files = []
    # URL base para baixar os arquivos
    base_url = "http://dados.cvm.gov.br/dados/FI/DOC/INF_DIARIO/DADOS"
    
    for date in dates:
        # Contrói a URL de download do arquivo a partir da data
        url = "{}/inf_diario_fi_{}.csv".format(base_url, date)
        
        # Realiza a requisição
        response = requests.get(url)
                
        if response.status_code != 200:
            print("Erro ao requisitar o arquivo {}".format(url))
            
            # Caso ocorra um erro durante a requisição pular para a próxima data
            continue
        
        
        file_name = "{}{}_{}.csv".format(dir, file_base_name, date)
        
        # Salva o arquivo
        with open(file_name, 'wb') as f:
            f.write(response.content)

        saved_files.append(file_name)
        print('Arquivo {} salvo.'.format(file_name))
    
    return saved_files
```

Estamos prontos agora para fazer o download dos arquivos, vamos definir duas variáveis para a data inicial e final do período que queremos extrair:


```python
initial_date = datetime(year=2019, month=6, day=1)
final_date = datetime(year=2020, month=6, day=1)
```

Com a função `generate_dates` vamos gerar a lista de datas do período definido. Precisamos passar a data inicial e final, o quanto queremos incrementar em dias para cada iteração, nesse caso como nossa fonte de dados tem um arquivo por mês devemos incrementar 31 dias a cada iteração. Por último, precisamos especificar o formato da data resultante, devemos utilizar os [códigos de formatação descritos na documentação do Python](https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes) para construir a data no formato que queremos, na fonte de dados a data dos arquivos está no formato: YYYYMM.


```python
date_list = generate_dates(initial_date, final_date, 31, "%Y%m")
```

```python
print(date_list)
```

```txt
['201906', '201907', '201908', '201909', '201910', '201911', '201912', '202001', '202002', '202003', '202004', '202005']
```
    


Vamos execuar a função de download dos arquivos e assistir a magia acontecer:


```python
file_list = download_files(date_list)
```

```txt
Arquivo data/inf_diario_fi_201906.csv salvo.
Arquivo data/inf_diario_fi_201907.csv salvo.
Arquivo data/inf_diario_fi_201908.csv salvo.
Arquivo data/inf_diario_fi_201909.csv salvo.
Arquivo data/inf_diario_fi_201910.csv salvo.
Arquivo data/inf_diario_fi_201911.csv salvo.
Arquivo data/inf_diario_fi_201912.csv salvo.
Arquivo data/inf_diario_fi_202001.csv salvo.
Arquivo data/inf_diario_fi_202002.csv salvo.
Arquivo data/inf_diario_fi_202003.csv salvo.
Arquivo data/inf_diario_fi_202004.csv salvo.
Arquivo data/inf_diario_fi_202005.csv salvo.
```


## Tratamento


Antes de entrar na etapa de tratamente é importante que você explore os dados para assim conseguir identificar exatamente quais transformações você deseja aplicar, se é necessário tratar algum campo ou filtrar alguma informação. Não vou demonstrar a etapa de análise pois é necessário um tutorial dedicado somente a esclarecer essa fase. Vamos partir do pressuposto que é necessário realizar as seguintes atividades:

1. Juntar todos os arquivos em um só;
2. Manter somente as colunas: `CNPJ_FUNDO`, `VL_QUOTA`, `DT_COMPTC`, `VL_PATRIM_LIQ` e `NR_COTST`;
3. Transformar a coluna `DT_COMPTC` em objetos [datetime](https://docs.python.org/3/library/datetime.html), que é a estrutura de dados que representa datas no python.

Daqui para frente vamos utilizar principalmente a biblioteca [Pandas](https://pandas.pydata.org/), que oferece uma série de ferramentas para facilitar a manipulação e análise de dados. O Pandas oferece uma estrutura de dados tabular chamada [DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html) que permite carregar dados de diferentes formatos.


```python
import pandas as pd
```

### Juntando todos arquivos em um

Vamos utilizar a função [read_csv](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html) do pandas para ler os arquivos baixados e a função [concat](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.concat.html?highlight=concat#pandas-concat) para juntá-los em um só [DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html). É importante observar que o separador dos arquivos `csv` baixados não é a vírgula(`,`) e sim o ponto e vírgula(`;`), é necessário especificar essa informação ao ler o arquivo usando a função [read_csv](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html). O jeito mais rápido de descobrir qual é o separador de um arquivo `csv` é abrindo-o como um arquivo de texto através de um editor de texto, é possível observar qual o separador utilizado no conteúdo do arquivo.

Vamos então criar uma função para concatenar todos os arquivos lidos em um só [DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html):


```python
def concat_csvs(files, sep=','):
    """
    Concatena todos os arquivos em um só DataFrame
    
    Parâmetros:
        files = Lista do nome e caminho dos arquivos a serem concatenados;
        sep = Separador dos arquivos csv's.
    """
    # Gera uma lista onde cada DataFrame é um item da lista
    df_list = [pd.read_csv(file, sep=sep) for file in files]
    
    # Concatena todos os itens da lista em um só DataFrame
    df = pd.concat(df_list)
    
    return df
```

Vamos aproveitar da capacidade do Jupyter Notebook de executar [comandos shell](https://www.geeksforgeeks.org/basic-shell-commands-in-linux/) para listar todos os arquivos salvos no diretório `data/` e armazenar o resultado em uma variável:


```python
files = !ls -d data/*
```


```python
print(files)
```

```txt
['data/inf_diario_fi_202001.csv', 'data/inf_diario_fi_202002.csv', 'data/inf_diario_fi_202003.csv', 'data/inf_diario_fi_202004.csv']
```



Agora podemos executar a função `concat_csvs` para gerar nosso [DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html) resultante:


```python
df = concat_csvs(files, sep=';')
```


```python
print("Quantidade de linhas do DataFrame: {}".format(df.shape[0]))
print("Quantidade de colunas do DataFrame: {}".format(df.shape[1]))
```

```txt
Quantidade de linhas do DataFrame: 1398741
Quantidade de colunas do DataFrame: 8
```




```python
print("Colunas do DataFrame: {}".format(df.columns.values))
```

```txt
Colunas do DataFrame: ['CNPJ_FUNDO' 'DT_COMPTC' 'VL_TOTAL' 'VL_QUOTA' 'VL_PATRIM_LIQ'
 'CAPTC_DIA' 'RESG_DIA' 'NR_COTST']
```


Podemos observar as primeiras colunas do [DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html) através do método `head()`.


```python
df.head()
```

```txt
╔═══╦════════════════════╦════════════╦════════════╦═══════════╦═══════════════╦═══════════╦══════════╦══════════╗
║   ║ CNPJ_FUNDO         ║ DT_COMPTC  ║ VL_TOTAL   ║ VL_QUOTA  ║ VL_PATRIM_LIQ ║ CAPTC_DIA ║ RESG_DIA ║ NR_COTST ║
╠═══╬════════════════════╬════════════╬════════════╬═══════════╬═══════════════╬═══════════╬══════════╬══════════╣
║ 1 ║ 00.017.024/0001-53 ║ 2020-01-02 ║ 1132491.66 ║ 27.225023 ║ 1123583.00    ║ 0.0       ║ 0.0      ║ 1        ║
╠═══╬════════════════════╬════════════╬════════════╬═══════════╬═══════════════╬═══════════╬══════════╬══════════╣
║ 2 ║ 00.017.024/0001-53 ║ 2020-01-03 ║ 1132685.12 ║ 27.224496 ║ 1123561.25    ║ 0.0       ║ 0.0      ║ 1        ║
╠═══╬════════════════════╬════════════╬════════════╬═══════════╬═══════════════╬═══════════╬══════════╬══════════╣
║ 3 ║ 00.017.024/0001-53 ║ 2020-01-06 ║ 1132881.43 ║ 27.225564 ║ 1123605.31    ║ 0.0       ║ 0.0      ║ 1        ║
╠═══╬════════════════════╬════════════╬════════════╬═══════════╬═══════════════╬═══════════╬══════════╬══════════╣
║ 4 ║ 00.017.024/0001-53 ║ 2020-01-07 ║ 1133076.85 ║ 27.226701 ║ 1123652.24    ║ 0.0       ║ 0.0      ║ 1        ║
╠═══╬════════════════════╬════════════╬════════════╬═══════════╬═══════════════╬═══════════╬══════════╬══════════╣
║ 5 ║ 00.017.024/0001-53 ║ 2020-01-08 ║ 1132948.59 ║ 27.227816 ║ 1123698.26    ║ 0.0       ║ 0.0      ║ 1        ║
╚═══╩════════════════════╩════════════╩════════════╩═══════════╩═══════════════╩═══════════╩══════════╩══════════╝
```


### Removendo colunas

Vamos remover as colunas que não nos interessam nesse momento, que são: `VL_TOTAL`, `CAPTC_DIA` e `RESG_DIA`. Para isso vamos  utilizar a função [drop](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.drop.html?highlight=drop#pandas-dataframe-drop) do pandas:


```python
df_res = df.drop(columns=['VL_TOTAL', 'CAPTC_DIA', 'RESG_DIA'])
```


```python
print("Colunas do DataFrame resultante: {}".format(df_res.columns.values))
```

```txt
Colunas do DataFrame resultante: ['CNPJ_FUNDO' 'DT_COMPTC' 'VL_QUOTA' 'VL_PATRIM_LIQ' 'NR_COTST']
```


O novo [DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html) possui somente as informações que consideramos necessárias.

### Transformando coluna de data

O pandas oferece a função [to_datetime](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.to_datetime.html?highlight=to_datetime#pandas-to-datetime) para facilitar a transformação de colunas em objetos [datetime](https://docs.python.org/3/library/datetime.html). Mas por que transformar as datas que estão em _string_ para objetos [datetime](https://docs.python.org/3/library/datetime.html)? Esse tipo de estrutura de dados foi criado justamente para facilitar a manipulação de datas, permitindo ordenar, somar e subtrair períodos(como fizemos na função `generate_dates`) e ao exportar nossos dados para um banco de dados a informação que aquela coluna contém datas se manterá(dependendo do banco).

A função [to_datetime](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.to_datetime.html?highlight=to_datetime#pandas-to-datetime) recebe como principais argumentos a coluna que deve ser transformada e o formato que se encontram as datas. O formato pode ser especificado seguindo [os códigos de formatação descritos na documentação do Python](https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes).


```python
# Carregando a coluna DT_COMPTC com resultado a função to_datetime

df_res.DT_COMPTC = pd.to_datetime(df.DT_COMPTC, format="%Y-%m-%d")
```

## Carregamento 

Vou apresentar **duas** maneiras de exportar o resultado do processo de extração e transformação. O primeiro é mais simples, vamos exportar o resultado no formato `csv`, já a segunda abordagem demonstra como exportar o resultado para um banco de dados [MongoDB](https://www.mongodb.com/). A segunda abordagem é comum em cenários onde os dados resultantes devem ser consumidos por alguma aplicação web ou ferramenta de _Business intelligence_.

### Exportando no formato CSV

Para exportar o [DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html) resultando em formato `csv` vamos utilizar a função [to_csv](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_csv.html) do pandas:


```python
df_res.to_csv('cotacoes_fundos.csv')
```

### Exportando para o banco [MongoDB](https://www.mongodb.com/) 

O [MongoDB](https://www.mongodb.com/) é um banco de dados não relacional que ganhou popularidade nos últimos anos por sua escalabilidade e facilidade de configuração. Para exportar o resultado para o banco [MongoDB](https://www.mongodb.com/) vamos utilizar a biblioteca [pymongo](https://pymongo.readthedocs.io/en/stable/) que oferece um conjunto de métodos para facilitar esse tipo de operação de carregamento, atualização, etc.

Primeiro vamos importar a classe [MongoClient](https://pymongo.readthedocs.io/en/stable/tutorial.html?highlight=MongoClient#making-a-connection-with-mongoclient) que serve para realizar a conexão com o banco de destino:


```python
from pymongo import MongoClient
```

Criamos a conexão passando as informações de endereço(_host_) e porta(_port_):


```python
client = MongoClient(host='localhost', port=27017)
```

O MongoDB é estruturado através de [bases de dados e coleções](https://docs.mongodb.com/manual/core/databases-and-collections/). As bases de dados guardam coleções de documentos. Onde cada coleção é composta por um conjunto de dados(ou como é chamado pelo MongoDB, documentos) que são relacionados. No nosso caso vamos guardar os dados em uma coleção chamada `fundos`, já que os dados correspondem aos fundos de investimento.
 
 
![](https://docs.mongodb.com/manual/_images/crud-annotated-collection.bakedsvg.svg)

Selecionamos o banco com o nome `web` e a coleção com o nome `fundos`:


```python
db = client.web
collection = db.fundos
```

Para conseguir fazer o upload do resultado no banco MongoDB precisamos transformar as informações que se encontram em formato tabular(DataFrame) para um [dicionário](https://docs.python.org/3/tutorial/datastructures.html#dictionaries). O pandas oferece a função [to_dict]() para realizar esse tipo de operação. Precisamos especificar que queremos transformar cada linha da tabela do DataFrame em um registro no dicionário gerado, para isso usamos o parâmetro `orient`:


```python
dict_res = df_res.to_dict(orient='records')
```

Por fim, carregamos os registros na coleção:


```python
db.fundos.insert_many(dict_res)
```

# Conclusão 

Durante o tutorial seguimos o processo conhecido como **ETL(_Extract, Transform and Load_)**, que é composto primeiramente da etapa de extração, onde é realizada a coleta dos dados das diferentes fontes. A segunda etapa consiste na fase de transformação, onde são executadas operações de agregação e limpeza, aplicando também um conjunto de regras que são definidas a partir do contexto negocial. Finalmente, a terceira etapa é composta pela fase de carregamento, onde as informações resultantes das transformações são enviadas para uma base de dados onde as aplicações cliente poderão consumir o resultado.

Claro que a solução que implementamos aqui é simples e trabalha com um pequeno volume de dados, a partir do momento em que o volume de dados aumenta é preciso buscar ferramentas otimizadas, mas o processo no geral continua o mesmo.

## Código fonte

O Jupyter notebook contendo todo o código do tutorial pode ser encontrado na organização do Typekast no GitHub: [https://github.com/typekast/tutorial-basic-etl](https://github.com/typekast/tutorial-basic-etl).

## Referência 

VASSILIADIS, P.; SIMITSIS, A.; SKIADOPOULOS, S. Conceptual modeling for ETL processes. In: Proceedings of the 5th ACM international workshop on Data Warehousing and OLAP - DOLAP. ACM Press, 2002. Disponível em: <https://doi.org/10.1145%2F583890.583893>.
