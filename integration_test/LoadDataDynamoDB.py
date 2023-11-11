import logging
import random
from datetime import datetime
from datetime import timedelta

import boto3
from localstack_client.patch import enable_local_endpoints

logging.basicConfig(format='[%(levelname)s] %(asctime)s %(filename)s: %(funcName)s %(message)s', level=logging.INFO)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

enable_local_endpoints()


def carregar_dados():
    logger.info('preparando para iniciar o teste integrado')

    logger.info('carregando o client do dynamoddb')

    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

    table_name = 'poc_stream'
    dynamodb_table = dynamodb.Table(table_name)

    logger.info(f'preparando para carregar a referencia da tabela {table_name}')

    random.seed(42)

    for item in range(10):
        time_to_exist = datetime.now() + timedelta(minutes=2)
        i = int(random.random() * 10000)
        logger.info(f'preparando para inserir registro [{i}] na tabela {table_name}')
        dynamodb_table.put_item(Item={
            'id': str(i),
            'name': 'nome ' + str(i),
            'timeToExist': int(time_to_exist.strftime('%s'))
        })

    logger.info(f'carregamento da tabela [{table_name}] foi concluído com sucesso')


carregar_dados()
