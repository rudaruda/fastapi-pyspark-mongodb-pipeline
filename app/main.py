from api import eventProcessor, utilities, aggregator, writer
from tests import test
import sys

def main(args:str):
    if args in ['pipeline','pipe']: 
        print('> PIPELINE start...')
        args = 'pipeline'
    elif args in ['etl','ingest','eventprocessor','processor']:
        print('> EVENTPROCESSOR start...')
        args = 'eventprocessor'
    elif args in ['elt','aggregator','agg']:
        print('> AGGREGATOR start...')
        args = 'aggregator'
    elif args in ['wrt','writer','write']:
        print('> WRITER start...')
        args = 'write'
    elif args in ['tests','test','teste']:
        print('> TESTS start...')
        args = 'tests'
    else:
        print('NENHUM PARAMETRO FOI INFORMADO: pipeline, eventprocessor, aggregator, write ou tests')
        sys.exit()
    # EventProcessor()
    if args in ['pipeline','eventprocessor']:
        print('> eventProcessor.EventProcessor(), start...')
        event_proc = a = eventProcessor.EventProcessor()
        df = event_proc.process_events('df') # retorna o DataFrame
        print('> eventProcessor.EventProcessor(), finish OK!')
        print('  DataFrame final do EventProcessor():')
        df.show()
    # Aggregator()
    if args in ['pipeline','aggregator']:
        print('> aggregator.Aggregator(), start...')
        aggreg = aggregator.Aggregator()
        aggreg_result = aggreg.aggregate_data() # retorna o Json
        print('> aggregator.Aggregator(), finish OK!')
        print('  Resultado final do Aggregator():')
        if not type(aggreg_result) is list:
            print('Falha no aggregator.Aggregator(), resultado não é lista')
            sys.exit()
        for line in aggreg_result:
            if not type(aggreg_result) is list:
                print('Falha no aggregator.Aggregator(), resultado não é dict')
                sys.exit()
            for key, row in line.items(): 
                print(' -',key)
                for rline in row: print('  ',rline)
    # DataFrame TO PARQUET
    # Writer()
    if args in ['pipeline','write']:
        print('> writer.Writer(), start...')
        writ = writer.Writer()
        writ_return = writ.write_data()
        print('> writer.Writer(), finish OK!')
        print('Arquivos PARQUET disponíveis em /parquet_files/dados')
    # Tests    
    if args in ['tests']:
        print('> test.Tests(), start...')
        tests = test.Tests()
        tests_result = tests.execute()
        print('> test.Tests(), finish OK!')
        print('  Resultado final de Tests():')
        for line in tests_result: print('  -',line)
    return

# __main__
if __name__ == "__main__":
    args = "".join(sys.argv[1:])
    print(f"args:'{args}'")
    if args in ['pipe','pipeline','pipeline','etl','ingest','eventprocessor','processor','pipeline','elt','aggregator','agg','wrt','writer','write','tests','test','teste']:
        print("Irá executar o main()")
        main(str(args))
    else:
        print('NENHUM PARAMETRO FOI INFORMADO: pipeline, eventprocessor, aggregator, write ou tests')
        sys.exit()