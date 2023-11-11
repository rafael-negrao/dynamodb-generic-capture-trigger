import decimal
import json
import logging
import os
from copy import deepcopy

import boto3
from kafka import KafkaProducer
from kafka.errors import KafkaError
from retry import api

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ENV_KEY_KAFKA_HOSTS = 'KAFKA_HOSTS'
ENV_KEY_KAFKA_SECRET_ID = 'KAFKA_SECRET_ID'
RETRY_TRIES = 3
RETRY_DELAY = 1


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return str(o)
        return super(DecimalEncoder, self).default(o)


def get_table_name(event):
    arn = event['Records'][0]['eventSourceARN']
    start = arn.index('table/') + len('table/')
    end = arn.index('/stream/')
    return arn[start:end]


def get_kakfa_secret(kakfa_secret_id):
    secretsmanager = boto3.client('secretsmanager')
    secretsmanager.get_secret_value(SecretId=kakfa_secret_id)
    return secretsmanager['SecretString']


def get_kakfa_secret_id():
    kakfa_secret_id = os.getenv(ENV_KEY_KAFKA_SECRET_ID, None)
    logger.info(f'kakfa_secret_id = {kakfa_secret_id}')
    return kakfa_secret_id


def get_credentials() -> dict:
    credentials = {}
    kakfa_secret_id = get_kakfa_secret_id()
    if kakfa_secret_id:
        secret_value = get_kakfa_secret(kakfa_secret_id=kakfa_secret_id)
        credentials = json.loads(secret_value)
    return credentials


def key_serializer(v):
    return json.dumps(v, cls=DecimalEncoder).encode('utf-8')


def value_serializer(v):
    return json.dumps(v, cls=DecimalEncoder).encode('utf-8')


def get_kafka_hosts() -> list:
    kafka_hosts = os.getenv(ENV_KEY_KAFKA_HOSTS, None)
    if kafka_hosts is None:
        raise ValueError(f'Environment variable [{ENV_KEY_KAFKA_HOSTS}] had not found!!!')
    logger.info(f'kafka_hosts = {kafka_hosts}')
    return kafka_hosts.split(',')


def get_config_kakfa_producer() -> dict:
    config = {
        'bootstrap_servers': get_kafka_hosts(),
        'acks': 'all',
        'request_timeout_ms': 5000,
        'max_block_ms': 10000,
        'api_version': (2, 7, 0),
        'key_serializer': key_serializer,
        'value_serializer': value_serializer
    }
    config.update(get_credentials())
    return config


def create_kakfa_producer() -> KafkaProducer:
    return KafkaProducer(**get_config_kakfa_producer())


def transform(deserializer, element):
    _element = deepcopy(element)
    _element['dynamodb']['Keys'] = deserializer_element(deserializer, _element['dynamodb']['Keys'])
    if 'OldImage' in element['dynamodb']:
        _element['dynamodb']['OldImage'] = deserializer_element(deserializer, _element['dynamodb']['OldImage'])
    if 'NewImage' in element['dynamodb']:
        _element['dynamodb']['NewImage'] = deserializer_element(deserializer, _element['dynamodb']['NewImage'])
    return _element


def deserializer_element(deserializer, element):
    return {k: deserializer.deserialize(v) for k, v in element.items()}


def send_to_kafka(producer, topic, key, value):
    api.retry_call(
        producer.send,
        fkwargs={
            'topic': topic,
            'key': key,
            'value': value
        },
        exceptions=KafkaError,
        tries=RETRY_TRIES,
        delay=RETRY_DELAY,
        logger=logger
    )
