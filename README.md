# fastapi-pyspark-mongodb-pipeline 

Projeto experimental de **Pipeline de dados** com **FASTAPI + PYSPARK + MONGODB**.
Basicamente a ideia é manipular os dados com PYSPARK armazenada snapshot do DataFrames no MONGODB a cada transformação. 
E no final apresentar a evolução do pipeline de forma visualmente (isso mesmo em HTML como se fosse um front).

# Arquitetura
![arquitetura](image/pipeline-fastapi-arquitetura.drawio.png)

**Dockerfile** com imagem *python:3.12-slim* que sustenta nossa aplicação em **Python** com **FastAPI**. Por sua vez esta se conectando com **PySpark** e realiza a manipulação de dados. A cada alteração do DataFrame é persistindo um snapshop dos dados no **MongoDB**. 
O MongoExpress esta aqui somente como utilitário, para visualizar os dados persistidos no MongoDB. 

| :city_sunrise: |Aplicação| O que é|
|-----|:-----:|-------------|
| <img src="image/fastapi_icon.png" alt="fastapi ico" style="width:200px; height:100%"> | **[FastAPI](https://fastapi.tiangolo.com/)**| Framework web Python, rápido e moderno, para criar APIs com suporte a validações automáticas e documentação integrada.|
| <img src="image/pyspark_icon.png" alt="pyspark ico" style="width:200px; height:100%"> | **[PySpark](https://spark.apache.org/docs/latest/api/python/index.html)** | Interface Python para o Apache Spark, usada para processamento distribuído de grandes volumes de dados em cluster. |
| <img src="image/mongodb_icon.jpg" alt="mongodb ico" style="width:200px; height:100%"> | **[MongoDB](https://www.mongodb.com/pt-br/docs/manual/administration/install-community/)** | Banco de dados NoSQL orientado a documentos, que armazena dados em formato JSON-like (BSON), permitindo flexibilidade e escalabilidade para aplicações modernas. |
| <img src="image/docker_icon.png" alt="docker ico" style="width:200px; height:100%"> | **[Docker](https://www.docker.com/get-started/)** | Plataforma para criar, distribuir e executar aplicações em containers isolados.|
| <img src="image/podman_icon.png" alt="podman ico" style="width:200px; height:100%"> | **[Podman](https://podman.io/get-started)** | Alternativa para executar container em relação ao Docker. Consome menos recursos de máquina no desenvolvimento local ***(super recomendo!)*** :rocket:.|

### Estrutura principal de diretórios
    root
    ├── docker-compose.yml
    ├── dockerfile.yml
    ├── requirements.yml
    └── app
        ├── __init__.py
        ├── main.py
        ├── api
        │   └── aggregator.py
        │   └── eventProcessor.py
        │   └── writer.py
        └── test
            └── test.py

## Caso de uso: Fluxo de Processamento
![caso de uso](image/case-use.png)

O diagrama representa o fluxo completo de processamento da Pipeline, que é exatamente nessa sequencia:
1. [EventProcessor.process_events()](http://localhost:8000/docs#/Main/evt_process_events_eventprocessor_process_events__get)
    > Método que realiza o carregamento do arquivo JSON, tratamento dos dados (limpeza e enriquecimento) e filtro dos registros
2. [Aggregator.aggregate_data()](http://localhost:8000/docs#/Main/agg_aggregate_data_aggregator_aggregate_data__get)
    > Método de análise e relatórios: preço médio por rota e classe de serviço, total de assentos disponíveis por rota e companhia e rota mais popular por companhia de viagem.
3. [Write.write_data()](http://localhost:8000/docs#/Main/wrt_write_data_writer_write_data__get)
    > Método que processamento do arquivo Parquet, no caso precisei adaptar para tivesse saída em FileResponse/Download.

![fluxo](image/fluxo.gif)

# Como instalar
Considerações gerais:
* Necessário ter Docker e Docker-compose _(ou Podman + Podman Compose)_ instalado
* Utilizar o comando `docker-compose up` ou `podman-compose up` no diretório do repositório
* Aplicação deve ser executada em [http://0.0.0.0:8000/](http://0.0.0.0:8000/) ou [http://localhost:8000/](http://localhost:8000/)
* Ler a [documentação](http://localhost:8000/docs): ([http://localhost:8000/docs](http://localhost:8000/docs));
* Caso queira executar fora do Container, instalar as bibliotecas `requirements.txt` e execute os arquivos em python.

### Pré-requisitos:

- DOCKER / PODMAN 
- JAVA 11


### 1. Clone o projeto:
```
git clone https://github.com/rudaruda/fastapi-pyspark-mongodb-pipeline.git
```

### 2. Instale imagem do docker-compose:

Estando no diretório do projeto, com **Docker** ou **Podman**:

| Docker | Podman _(recomendado)_ |
|:--------:|:--------:|
| ```docker-compose up``` | ```podman-compose up``` |

### 3. Execute os testes no Swagger
   ![Pipeline](image/testes-compress.gif)

   Todos os testes podem ser executados diretamente pelo Swagger:
   - [localhost:8000/docs/Testes/test_all](http://localhost:8000/docs#/Testes/mongodb_test_all_get)
     > Reforço: No Swagger temos a documentação mais detalhada de cada endpoint / funcionalidade
   - Ou executando o método `Test.execute()` em `/app/tests/test.py`


### 4. Executatando localmente...

#### 4.1 Instale as dependencias

Depende do sistema operacional, acesse o site dos desenvolvedores para mais informação:
- [Docker/get-start](https://docs.docker.com/get-started/get-docker/)
- [Podman/installation](https://podman.io/docs/installation)
- [Java/download](!https://www.java.com/download/ie_manual.jsp) _(versão 11)_


**Ative o ambiente virtual**

É recomendável que faça a execução dentro do ambiente virtual do python.
   
O **Poetry** faz isso de forma mais automática com o comando:
```
poetry run python <file.py> <args>
```

Porém, é necessário ter ele instalado... para instalar digite o comando:
```
pip install poetry
```

O modo tradicional de ativar o ambiente virtual do Python é com o comando:
```
source .venv/bin/activate
```

**Instale as bibliotecas do Python**
Estando no diretório do projeto, instale com **pip** ou **Poetry**:

| pip | Poetry |
|:-----------:|:--------------:|
| ```pip install -r requirements.txt``` | ```poetry install```|

**Instale o MongoDB**
Ainda será necessário ter o **MongoDB** instalado com as mesmas configurações registradas no docker-compose:

- `host: localhost / mongodb`
- `port: 27017`
- `user: root`
- `password: root`

**Dada a complexidade... É ALTAMENTE RECOMENDÁVEL** que execute o projeto **somente pelo container**.

#### 4.2 Execute o uvicorn
É preciso executar o servidor da aplicação web para que a Api e Swagger fiquem ativos.

Estando no diretório raiz do projeto:
```c
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

ou com **Poetry**
```c
poetry run uvicorn app.main:app --reload
````

# Como usar...

_(recomendável via container)_
Estando no diretório do projeto, com **Docker** ou **Podman**:

| Docker | Podman |
|--------|--------|
| ```docker-compose up``` | ```podman-compose up``` |



**Reforçando:** No Swagger podemos executar, pontualmente cada um dos métodos da pipeline, podendo repetir cada etapa. 

Acesse o endpoint no link do item acima, depois (no Swagger) em "Try it out" -> "Execute" -> Visualise o resultado em "Details". A animação em cima esclarece melhor em caso de dúvida.

# Pipeline
![Pipeline](image/pipeline-speed.gif)

Você pode executar a pipeline através da URL com visualização HTML: 
- [http://localhost:8000/pipe_show](http://localhost:8000/pipe_show)
    > Aqui esta sendo executado process_events, aggregate_data e write_data em sequencia. No final aparece link para fazer Download do arquivo Parquet ou Visualizar os Insights.
Para visualizar os insights você deve acessar a URL: 
- [http://localhost:8000/pipe_insights](http://localhost:8000/pipe_insights)
    > Executando as análises: relatório de preço médio por rota e classe de serviço, total de assentos disponíveis por rota e companhia e rota mais popular por companhia de viagem.

## Documentação da API (Swagger)
![arquitetura](image/docs.png)
```
http://localhost:8000/docs
```

O Swagger fica disponível assim que a aplicação é executada com o comando em Docker/Podman:

| Docker | Podman |
|--------|--------|
| ```docker-compose up``` | ```podman-compose up``` |

Teremos lá o detalhe de cada Endpoint / Função da Pipeline agrupadas por Tags / Funcionalidades. Você pode realizar as execuções de cada etapa diretamente por lá (inclusive é muito fácil).

# Conclusão
Documentação nunca é demais.
Temos o Readme aqui do github, com imagens e gifs animados e além disso temos o Swagger que fica automaticamente disponível quando fazemos uso do FastAPI no desenvolvimento.

A construção do Dockerfile inicialmente pode ser complexa, mas depois que consegue identificar a imagem correta e versões do recursos corretos, o desenvolvimento fica muito mais fluído. Uma grande descoberta para mim foi o PODMAN, ele realmente consome menos recurso da máquina e fica melhor de desenvolver.

Visualizar a Pipeline em HTML, penso que faz muito sentido. Por isso coloquei esse esforço adicional no projeto. Porque quando falamos de dados sempre queremos **visualizar os dados**, já em backend parece que sempre ficam "escondidos". Precisamos fazer uso dos recursos para "mostrar nosso ouro".

Acaba existindo um esforço adicional para desenvolver as Classes, Funções e também os endpoints da API. Porém, no final a qualidade fica superior. Temos acesso detalhe maior de cada funcionalidade desenvolvida, penso em até padronizar meus próximos desenvolvimento todos com FastAPI, mesmo que para objetivos mais simples.

Me diverti com esse teste e resolvi fazer dele um experiência, algo que eu pudesse aprender alguma coisa nova. E no caso foi conectar tecnologias com foco na entrega end-to-end.