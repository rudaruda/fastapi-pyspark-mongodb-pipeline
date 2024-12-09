# fastapi-pyspark-mongodb-pipeline 

Projeto experimental de pipeline com FASTAPI + PYSPARK + MONGODB.
Básicamente a ideia é manipular os dados com PYSPARK armazenada snap_shot do DataFrames no MONGODB a cada transformação. 
E no final apresentar a evolução do pipeline de forma visualmente (isso mesmo em HTML como se fosse um front).

## Como utilizar
* Necessário ter Docker e Docker-compose instalado;
* Utilizar o comando `docker-compose up` ou `podman-compose up` no diretório do repositório;
* Aplicação roda em [0.0.0.0:8000/](0.0.0.0:8000/) por padrão;
* Verificar a [documentação](0.0.0.0:8000/docs) (rota `/` ou  `/docs`);
* Caso queira rodar fora do Container, instalar as biblitecas `requirements.txt` e execute os arquivos em python.

### Pré-requisito:

- DOCKER / PODMAN 
- JAVA 11

##### Instalandoll as dependencies

Variavel por sistema operacional.
Acesse o site dos desenvolvedores para mais informações:
 - [docker/get-start](https://docs.docker.com/get-started/get-docker/)
 - [podman/installation]https://podman.io/docs/installation


### Executando localmente
- Clone o projeto:
  ```
  git clone x
  ```
 Execute o conteiner com DOCKER:
 ```
 docker run x
 ```
... ou com PODMAN:
```
podman-compose run x
```

### Testes
Todos os testes são executados via URL:
- 
- 
- 

## Run with docker

### Run server

```
docker-compose up -d --build
```

### Run test

```
docker run -p 8000:8000 --network backend -it fastapi-pipeline --network backend
```
docker-compose exec app pytest test/test.py


podman network connect --ip 0.0.0.0:27017 -- ip 0.0.0.0:8000

docker network connect --ip 10.10.36.122 multi-host-network container2

podman network inspect nome_da_rede

# Arquitetura da solução
[Desenho]
Resumo da explicação

### Algumas ferramentas utilizadas na construção
* Docker / Podman
* Docker-compose / Podman-compose
* PySpark
* FastAPI
* Uvicorn
* Java 11
* Python


## API documentation (provided by Swagger UI)

```
http://127.0.0.1:8000/docs
```

Prints de caso de uso

### Run server

```
docker-compose exec db psql --username=fastapi --dbname=fastapi_dev
```

# api-pipeline-fastapi
EventProcessor, Aggregator, Writer com FastApi lendo arquivos Json

fastapi
    ├── docker-compose.yml
    └── src
        ├── Dockerfile
        ├── app
        │   ├── __init__.py
        │   └── main.py
        └── requirements.txt

""""
http://127.0.0.1:8000/eventprocessor/1-loadfile

http://127.0.0.1:8000/eventprocessor/2-searchitemlist/

http://127.0.0.1:8000/eventprocessor/3-featurecols/

http://127.0.0.1:8000/eventprocessor/4-futuredeparture

http://127.0.0.1:8000/eventprocessor/5-availableseats

http://127.0.0.1:8000/aggregator/6-avgrota

http://127.0.0.1:8000/aggregator/7-availableseats

http://127.0.0.1:8000/aggregator/8-popularroute

http://127.0.0.1:8000/eventprocessor/10-process_events

http://127.0.0.1:8000/aggregator/11-aggregate_data

"""