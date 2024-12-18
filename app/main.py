from api import eventProcessor, utilities, aggregator, writer
from tests import test
import sys

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
    

    return

# __main__
if __name__ == "__main__":
    args = "".join(sys.argv[1:])
    print(f"args:'{args}'")
    if args in ['pipe','pipeline','pipeline','etl','ingest','eventprocessor','processor','pipeline','elt','aggregator','agg','tests','test','teste']:
        print("Irá executar o main()")
        main(str(args))