import os
from io import BytesIO
from pyspark.sql import DataFrame, SparkSession
import shutil
import tempfile
from . import utilitiesDataframe
import zipfile
from fastapi import HTTPException

# create a SparkSession
spark = SparkSession.builder.appName("ReadJSON").getOrCreate()


# Instanciando *Writer*
class Writer:
    ## 3 Instancie as classes EventProcessor, Aggregator e *Writer*.
    def __init__(self):
        self.data = None
    
    def getLastParquet(self)->DataFrame:
        self.data = utilitiesDataframe.dfGetMongo()
        print("Get last DataFrame ok!")
        return self.data

    def write_data(self, df:DataFrame=None)->DataFrame:
        df = type(df) is DataFrame and df or self.getLastParquet()
        if not type(df) is DataFrame:
            raise HTTPException(status_code=404, detail="Item not found")
        else:
            df = write_data('df',df)
            print("Writer ok! write_data")
            return df


def write_data(x_dataframe:DataFrame=None,x_name_file:str='dataframe_parquet.zip'):
    print('**  Running... write_data()')
    ## 2.3) Writer.write_data(): ■ Salve os dados processados em Parquet.
    ## Garantir que os arquivos sejam particionados por originState e destinationState.
    # Particionado por "originState", "destinationState"
    df = type(x_dataframe) is DataFrame and x_dataframe or utilitiesDataframe.dfGetMongo()
    # Cria diretório temporario
    tmpdir = tempfile.mkdtemp()
    #os.chmod(tmpdir, 0o444)
    print('> executando parquet')
    df.write.mode("overwrite").partitionBy("originState", "destinationState").parquet(tmpdir)
    # Cria um objeto BytesIO
    memory_file = BytesIO()
    # Compacta o diretório temporário em um arquivo ZIP e armazena no BytesIO
    with zipfile.ZipFile(memory_file, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(tmpdir):
            for file in files:
                print(' . add to zip',root, file)
                file_path = os.path.join(root, file)
                # Adiciona o arquivo ao ZIP
                zf.write(file_path, arcname=os.path.relpath(file_path, tmpdir))
    print(f"> tamanho do arquivo zip em memória: {len(memory_file.getvalue())} bytes")
    # Cursor para inicio
    memory_file.seek(0)
    # Remove diretório temporario
    shutil.rmtree(tmpdir)
    print('** Finish! write_data()') 
    return memory_file

