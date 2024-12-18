import argparse
from app.api import eventProcessor, utilities, aggregator, writer
from app.tests import test
from fastapi import FastAPI, Request, Query, Path
from fastapi.staticfiles import StaticFiles
from starlette.responses import HTMLResponse, StreamingResponse, FileResponse, RedirectResponse
from fastapi.openapi.docs import get_swagger_ui_html
import sys

descriptionx = """### Objetivo\nDesenvolver rotina de pipeline de dados, com uso das tecnologias: **PYTHON** + **PYSPARK** + **FASTAPI** + **MONGDDB** e **DOCKER**\n\nO que era para ser apenas um teste para uma vaga, se transformou numa oportunidade de fazer algo novo (pelo menos pra mim)\n\n    Não havia visto nada parecido no GitHub...\n    Espero esse projeto possa agregar em algo novo para você também\n\n## Execute os testes\n\nAqui pelo próprio SWAGGER você pode executar todos os métodos.\n\n[TEST_ALL](./docs#/Testes/mongodb_test_all_get) faz isso rapidamente para você. **É necessário que "Tudo esteja OK!"**\n\n## Visualize a Pipeline\n\nFoi desenvolvido um frontend para a Pipeline, isso mesmo! [ACESSAR A PIPELINE](http://localhost:8000/pipe_show)\n\nOu execute em sequência os métodos: [EventProcessor.process_events()](./Main/evt_process_events_eventprocessor_process_events__get/) > [Aggregator.aggregate_data()](./Main/agg_aggregate_data_aggregator_aggregate_data__get) > [Write.write_data()](./Main/agg_aggregate_data_writer_write_data__get)"""

app = FastAPI(
    title="fastapi-pyspark-mongodb-pipeline",
    description=descriptionx,
    summary="""API para solução de pipeline de dados.""",
    version="0.0.1",
    contact={
        "name": "Filipe Rudá",
        "url": "https://www.linkedin.com/in/filiperuda/",
        "email": "filiperuda@gmail.com",
    },
)

# Mount files statics
app.mount("/static", StaticFiles(directory="app/static"), name="static")


@app.get("/", include_in_schema=False)
def root():
    """
    Redireciona para documentação /docs (Swagger)
    """
    return RedirectResponse(url="/docs")


@app.get("/eventprocessor_load/", tags=["EventProcessor"]) # {p:path}
async def load(p: str = Query('json', enum=['json','html','df'], description='Tipo de formato de saída') ):
    """
    **Parte 1: Criar uma Estrutura de Classes em POO**

    Classe EventProcessor

    *1.1.1) Ler o JSON em um DataFrame PySpark.*
    
    Carregar arquivo JSON para DataFrame SPARK
    """
    #print('URL executada:',req.url)
    print('p value:',p)
    res = eventProcessor.dfJsonLoad( utilities.lchar(p) )
    return utilities.jsonOrHtmlResponse(res, "Arquivo 'input-data.json', carregado DataFrame SPARK")


@app.get("/eventprocessor_searchitem/", tags=["EventProcessor"])
async def searchitem(p: str = Query('json', enum=['json','html','df'], description='Tipo de formato de saída') ):
    """
    **Parte 1: Criar uma Estrutura de Classes em POO**

    Classe EventProcessor

    *1.1.2) Explodir o array searchItemsList para normalizar os dados.*

    Feature de tratamento de dados: Seleção do atributo 'searchItemsList'
    """
    res = eventProcessor.dfJsonSearchItemList( utilities.lchar(p) )
    # "Dados do atributo searchItemsList"
    return utilities.jsonOrHtmlResponse(res, "Limpeza do dado: dfJsonSearchItemList()")


@app.get("/eventprocessor_featurecols/", tags=["EventProcessor"])
def featurecols(p: str = Query('json', enum=['json','html','df'], description='Tipo de formato de saída') ):
    """
    **Parte 1: Criar uma Estrutura de Classes em POO**

    Classe EventProcessor

    *1.1.3) Criar colunas derivadas: 'departure_datetime', 'arrival_datetime' e 'route'*

    Feature de adição de colunas: 'departure_datetime', 'arrival_datetime' e 'route'
    """
    res = eventProcessor.dfJsonFeatureCols( utilities.lchar(p))
    return utilities.jsonOrHtmlResponse(res, """Feature aplicada: 'departure_datetime', 'arrival_datetime', 'route'.
        * Somente quando as colunas depentes existem no DataFrame 
        ** Exemplo 'arrival_datetime' não esta no Schema pois 'arrivalDate' e 'arrivalHour' não existem neste DataFrame.""")


@app.get("/eventprocessor_futuredeparture/", tags=["EventProcessor"])
def futuredeparture(p: str = Query('json', enum=['json','html','df'], description='Tipo de formato de saída') ):
    """
    **Parte 1: Criar uma Estrutura de Classes em POO**

    Classe EventProcessor

    *1.1.4) Viagens futuras (baseadas no departure_datetime).*

    Feature de viagens futuras: Filtra registros do DataFrame: 'departure_datetime' > NOW
    """
    res = eventProcessor.dfFilterDeparturesFutures( utilities.lchar(p) )
    return utilities.jsonOrHtmlResponse(res, "Filtro de viagens futuras, departure_datetime > NOW")


@app.get("/eventprocessor_availableseats/", tags=["EventProcessor"])
def availableseats(p: str = Query('json', enum=['json','html','df'], description='Tipo de formato de saída') ):
    """
    **Parte 1: Criar uma Estrutura de Classes em POO**

    Classe EventProcessor

    *1.1.5) Viagens com availableSeats > 0*

    Feature de assentos disponíveis: Filtra registros do DataFrame: 'availableSeats' > 0
    """
    res = eventProcessor.dfFilterSeatsAvailables( utilities.lchar(p) )
    return utilities.jsonOrHtmlResponse(res, "Filtro de viagens com acentos disponíveis, availableSeats > 0")


@app.get("/agregator_avgrota/", response_class=HTMLResponse, tags=["Agregator"])
async def avgrota(p: str = Query('json', enum=['json','html','df'], description='Tipo de formato de saída') ):
    """
    **Parte 1: Criar uma Estrutura de Classes em POO**

    Classe Aggregator

    *1.2.1) Calcular o preço médio por rota e classe de serviço.*
    """
    res = aggregator.dfGetPriceAvgRouteClasse( utilities.lchar(p) )
    return utilities.jsonOrHtmlResponse(res) 


@app.get("/agregator_availableseats/", response_class=HTMLResponse, tags=["Agregator"])
async def availableseats(p: str = Query('json', enum=['json','html','df'], description='Tipo de formato de saída') ):
    """
    **Parte 1: Criar uma Estrutura de Classes em POO**

    Classe Aggregator

    *1.2.2) Determinar o total de assentos disponíveis por rota e companhia.*
    """
    return aggregator.dfGetTotalAvalSeatsRouteClasse( utilities.lchar(p) )


@app.get("/agregator_popularroute/", response_class=HTMLResponse, tags=["Agregator"])
async def porpularroute(p: str = Query('json', enum=['json','html','df'], description='Tipo de formato de saída') ):
    """
    **Parte 1: Criar uma Estrutura de Classes em POO**

    Classe Aggregator

    *1.2.3) Identificar a rota mais popular por companhia de viagem.*
    """
    return aggregator.dfGetFrequenceRoute( utilities.lchar(p) )


@app.get("/eventprocessor_process_events/", tags=["Main"])
def evt_process_events(p: str = Query('json', enum=['json','html','df'], description='Tipo de formato de saída') ):
    """
    **Parte 2: Implementar as Transformações**

    EventProcessor.process_events()

    *■ Leia o JSON. ■ Normalize os dados. ■ Retorne o DataFrame processado*

    Executa em sequencia todas a funções do EventProcessor de forma procedural e devolver do DataFrame final.
    """
    a = eventProcessor.EventProcessor()
    r = a.process_events(p)
    print('**Parte 2: Implementar as Transformações**')
    print(r)
    # eventProcessor.process_events( utilities.lchar(p+req.url.query) )
    return [{ "message":"eventProcessor.process_events() -> EXECUTADO!"
             ,"object":r }]


@app.get("/aggregator_aggregate_data/", tags=["Main"])
def agg_aggregate_data(p: str = Query('json', enum=['json','html','df'], description='Tipo de formato de saída') ):
    """
    **Parte 2: Implementar as Transformações**

    Aggregator.aggregate_data

    *■ Receba o DataFrame processado. ■ Gere as agregações solicitadas. ■ Retorne um DataFrame com os insights.*

    Executa em sequencia todas a funções do Aggregator de forma procedural e devolve as analises feitas.
    """
    a = aggregator.Aggregator()
    r = a.aggregate_data()
    # aggregator.aggregate_data( utilities.lchar(p+req.url.query)
    return [{ "message":"aggregator.aggregate_data() -> EXECUTADO!"
             ,"object": r }]


@app.get("/writer_write_data/", tags=["Main"], response_class=FileResponse)
def wrt_write_data():
    """
    **Parte 2: Implementar as Transformações**

    Writer.write_data

    *■ Salve os dados processados em Parquet.*
    """
    a = writer.Writer()
    r = a.write_data()
    # aggregator.aggregate_data( utilities.lchar(p+req.url.query)
    response = StreamingResponse(r, media_type="application/x-zip-compressed")
    response.headers["Content-Disposition"] = "attachment; filename=dataframe_parquet.zip"
    return response
    #return [{ "message":"Writer.write_data() -> EXECUTADO!","object": r }]



@app.get("/writer/download/file", response_class=FileResponse, tags=["Writer"]) 
async def file():
    """
    **Parte 1: Criar uma Estrutura de Classes em POO**

    1.3.1) Writer.write_data(): 
    
    *■ Salve os dados processados em Parquet. ■ Garantir que os arquivos sejam particionados por originState
e destinationState.*
    
    Retorna com arquivos do DataFrame em formato PARQUET.
    
    Os dados do DataFrame são consultado no MONGODB, por isso não precisa de parametros de saída.

    O arquivo de saída é ZIP e contem todos os arquivos do PARQUET.
    """
    response = StreamingResponse(writer.write_data(), media_type="application/x-zip-compressed")
    response.headers["Content-Disposition"] = "attachment; filename=dataframe_parquet.zip"
    return response


@app.get("/writer/download", tags=["Writer"])
async def download():
    """
    **Parte 2: Implementar as Transformações**
    
    2.3) Writer.write_data(): 
    
    *Salve os dados processados em Parquet.*

    É apenas uma página HTML para aguardar o download do arquivo PARQUET enquanto aguarda o download :-)
    """
    html_content = """
    <html>
        <head>
            <title>Download do Parquet</title>
        </head>
        <body>
            <h1>Preparando o arquivo para download...</h1>
            <p>O download começará em instantes.</p>
            <p>O arquivo esta zipado, basta descompactar pra visualizar o conteudo de arquivos parquet.<br>Reforço que esta é apenas uma solução paliativa para completar o objetivo<br>Que nós provoca, sobre o que mais podemos fazer. Num ambiente produtivo teriamos outras alternativas,<br>compartilhando recursos de rede e automatizando mais processo.</p>
            <script>
                setTimeout(function() {
                    window.location.href = '/writer/download/file';
                }, 2000);
            </script>
        </body>
    </html>
    """
    return HTMLResponse(content=html_content)


@app.get("/pipe_show", response_class=HTMLResponse, tags=["Pipeline"])
async def show():
    """
    **Parte 3: Testar o Pipeline**

    Página dinamica que executa o pipeline.
    
    Apresenta visualmente com um front o pipeline sendo executado.
    """
    return StreamingResponse(eventProcessor.runPipeline(), media_type="text/html")


@app.get("/pipe_insights", response_class=HTMLResponse, tags=["Pipeline"])
async def insights():
    """
    **Parte 3: Testar o Pipeline / 2.2) Retorne um DataFrame com os insights.**

    Página dinámica que executa as analises, gerando as agregações solicitadas.
    
    Apresenta os DataFrames em HTML e também o comentário dos insights.
    """
    return StreamingResponse(aggregator.runInsights(), media_type="text/html")


@app.get("/test_file", tags=["Testes"])
async def read_file():
    """
    Teste de leitura de diretorio DATA e arquivo JSON.
    Simula mesmo nivel de hirarquia do diretório "api"

    Resposta esperada:
    > { "id": 1, "name": "Testado", "mail": "leu@oaquivo.ok", "phone": "987654321" }
    """
    return test.read_file_json()

@app.get("/test_env_vars", tags=["Testes"])
def mongodb():
    """
    Teste de variaveis de ambiemte:
    - MONGO_INITDB_ROOT_USERNAME,
    - MONGO_INITDB_ROOT_PASSWORD
    - ME_CONFIG_MONGODB_SERVER
    Verifica se as variaveis necessarias para o projeto esta registradas no ambiente
    
    Resposta esperada:
    > "Variaveis {MONGO_INITDB_ROOT_USERNAME, MONGO_INITDB_ROOT_PASSWORD, ME_CONFIG_MONGODB_SERVER} estão registradas com sucesso"
    """
    return test.enviroment_get()


@app.get("/test_mongodb", tags=["Testes"])
def mongodb():
    """
    Teste de conectividade com MONGODB
    E na sequencia realiza insert de registro na coleção 'test'

    Resposta esperada:
    > "Registro inserido no MONGODB com sucesso!"
    """
    return test.mongodb_insert()


@app.get("/test_spark", tags=["Testes"])
def spark():
    """
    Teste de conectividade com SPARK
    Basicamente verifica se o ambiente consegue realizar a criação de DataFrame
    
    Resposta esperada:
    > "DataFrame criado com sucesso! 48 bytes"
    """
    return test.spark_create_dataframe()

@app.get("/test_spark_load", tags=["Testes"])
def spark():
    """
    Teste de carga de arquivo JSON com SPARK
    
    Resposta esperada:
    > "SPARK carregou arquivo JSON com sucesso! 48 bytes"
    """
    return test.spark_load_file()

@app.get("/test_all", tags=["Testes"])
def mongodb():
    """
    Executa todos os testes:
    - File: Leitura simples de arquivo JSON com PYTHON
    - Enronment Vars: Se existe variaveis setadas
    - MongoDB: Teste de conectividade e insert na coleção 'test'
    - PySpark: Teste básico de criação de um DataFrame
    - PySpark: Teste de carregamento de arquivo JSON
    
    Resposta esperada:
    > "Tudo OK!"
    """
    a = test.Tests()
    return a.execute()


@app.get("/docs", include_in_schema=False)
async def swagger_ui_html():
    return get_swagger_ui_html(
        openapi_url="/openapi.json",
        title="Pipeline",
        swagger_favicon_url="/static/favicon.png"
    )


def main(args:str):
    if args in ['pipeline','pipe']: 
        print('> PIPELINE start...')
        arg = 'pipeline'

    if args in ['pipeline','etl','ingest','eventprocessor','processor']:
        print('> eventProcessor.EventProcessor(), start...')
        event_proc = a = eventProcessor.EventProcessor()
        df = event_proc.process_events('df') # retorna o DataFrame
        print('> eventProcessor.EventProcessor(), finish OK!')
        print('  DataFrame final do EventProcessor():')
        df.show()

    if args in ['pipeline','elt','aggregator','agg']:
        print('> aggregator.Aggregator(), start...')
        aggregator = aggregator.Aggregator()
        aggregator_result = aggregator.aggregate_data()
        print('> aggregator.Aggregator(), finish OK!')
        print('  Resultado final do Aggregator():')
        print(aggregator_result)
    

    # DataFrame TO PARQUET
    # Write

    
    if args in ['tests','test','teste']:
        print('> test.Tests(), start...')
        tests = test.Tests()
        tests_result = tests.execute()
        print('> test.Tests(), finish OK!')
        print('  Resultado final de Tests():')
        print(tests_result)
    

    return 0

# __main__
if __name__ == "__main__":
    args = "".join(sys.argv[1:])
    print(f"args:'{args}'")
    if args in ['pipe','pipeline','pipeline','etl','ingest','eventprocessor','processor','pipeline','elt','aggregator','agg','tests','test','teste']:
        print("Irá executar o main()")
        main(str(args))