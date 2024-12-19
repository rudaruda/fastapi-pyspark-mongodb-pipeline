from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from datetime import datetime
import pymongo
import json, os, sys, traceback


def test_ping(test_app):
    response = test_app.get("/ping")
    assert response.status_code == 200
    assert response.json() == {"ping": "pong!"}


def read_file_json():
    try:
        with open('data/test.json') as stream:
            x = json.load(stream)
    except Exception as e:
        print(traceback.format_exc())
        print("An error occurred:", type(e).__name__, "-", e)
        return {'Falha!'}

    return x

def enviroment_get():
    try:
        v = dict(os.environ)
        if 'MONGO_INITDB_ROOT_USERNAME'in v and \
            'MONGO_INITDB_ROOT_PASSWORD' in v and \
            'ME_CONFIG_MONGODB_SERVER' in v:
            v = 'Variaveis MONGO_INITDB_ROOT_USERNAME, MONGO_INITDB_ROOT_PASSWORD, ME_CONFIG_MONGODB_SERVER estão registradas com sucesso'
        else:
            raise Exception("Variaveis não setadas no ambiente")
    except Exception as e:
        print(traceback.format_exc())
        print("An error occurred:", type(e).__name__, "-", e)
        return {'Falha!'}
    return v

def mongodb_insert():
    # Get Variable of Enviroment
    MONGO_USER = os.environ.get("MONGO_INITDB_ROOT_USERNAME")
    MONGO_PASS = os.environ.get("MONGO_INITDB_ROOT_PASSWORD")
    MONGO_SERVER = os.environ.get("ME_CONFIG_MONGODB_SERVER")

    if MONGO_SERVER is None: print('! MONGO_SERVER is  None!!')
    try:
        mdb_cli = pymongo.MongoClient(f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_SERVER}:27017/")
        mdb_db = mdb_cli["project"]
        mdb_col = mdb_db["test"]
        row_ins = { "dataframe":'str(x_df.collect())', "schema":'str(x_df.schema)', "dthr":datetime.now()}
        mdb_col.insert_one(row_ins)
    except Exception as e:
        print(traceback.format_exc())
        print("An error occurred:", type(e).__name__, "-", e)
        return {'Falha!'}

    return {'Registro inserido no MONGODB com sucesso!'}


def spark_create_dataframe():
    try:
        spark = SparkSession.builder.appName("ReadJSON").getOrCreate()
        data = [("James",3000), ("Michael",4000)]
        schema = StructType([ StructField("firstname",StringType(),True), StructField("salary", IntegerType(), True) ])
        df = spark.createDataFrame(data=data,schema=schema)
    except Exception as e:
        print(traceback.format_exc())
        print("An error occurred:", type(e).__name__, "-", e)
        return {'Falha!'}

    return {'DataFrame criado com sucesso! ' + str(sys.getsizeof(df)) + ' bytes'}


def spark_load_file():
    try:
        spark = SparkSession.builder.appName("ReadJSON").getOrCreate()
        x_path = "data/input_data.json"
        df = spark.read.json(x_path, multiLine=True)
    except Exception as e:
        print(traceback.format_exc())
        print("An error occurred:", type(e).__name__, "-", e)
        return {'Falha!'}
    
    return {'SPARK carregou arquivo JSON com sucesso! '+ str(sys.getsizeof(df)) + ' bytes'}


# Instanciando os Testes **
class Tests:
    ## Classe adicional de Testes
    def __init__(self):
        self.data = None
    
    def test(self, test_function):
        r = True
        try:
            v=test_function()
            v=str(v)[:10]
            if 'Falha' in v: r = False
        except Exception as e:
            print(traceback.format_exc())
            print("Tests.test: An error occurred:", type(e).__name__, "-", e)
            return False
        return r

    def execute(self):
        try:
            c, f, t = 1, 0, []
            # Testando diferentes funções e atualizando o set de falhas
            if self.test(read_file_json) is False: 
                f+=1
                v='Falha Arquivo JSON'
                t.append(v)
            else:
                t.append('Simples carregamento de arquivo JSON: OK!')
            
            c+=1
            if self.test(enviroment_get) is False: 
                f+=1
                v='Falha ENVIRONMENTS VARS'
                t.append(v)
            else:
                t.append('Teste variáveis de ambiente: OK!')
            
            c+=1
            if self.test(mongodb_insert) is False: 
                f+=1
                v='Falha MONGO'
                t.append(v)
            else:
                t.append('Conectividade com MongoDB: OK!')
            
            c+=1
            if self.test(spark_create_dataframe) is False: 
                f+=1
                v='Falha SPARK, create dataframe'
                t.append(v)
            else:
                t.append('Conectividade com SPARK: OK!')
            
            c+=1
            if self.test(spark_load_file) is False: 
                f+=1
                v='Falha SPARK, load file'
                t.append(v)
            else:
                t.append('Teste de carregar arquivo JSON com SPARK: OK!')
            
            # Verificando o estado dos testes
            c+=1
            if f==0:
                v='Tudo OK!'
                t.append(v)
                print(v)
            
            return t
        except Exception as e:
            print(traceback.format_exc())
            print("Tests.execute: An error occurred:", type(e).__name__, "-", e)