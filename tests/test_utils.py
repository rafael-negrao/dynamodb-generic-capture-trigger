import os
from decimal import Decimal
from unittest import mock
from unittest.mock import patch, MagicMock

import pytest
from boto3.dynamodb.types import TypeDeserializer
from kafka.errors import KafkaError

from kafka_utils.utils import (
    get_table_name, get_kafka_hosts, get_credentials, get_config_kakfa_producer, key_serializer, value_serializer, send_to_kafka, transform,
    deserializer_element, create_kakfa_producer)


@pytest.fixture
def events_keys_only_insert():
    return {'Records': [
        {'eventID': 'fe991aba5fc176fc760f31055aec395c', 'eventName': 'INSERT', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
         'dynamodb': {'ApproximateCreationDateTime': 1691160109.0, 'Keys': {'id': {'S': '6211_insert'}}, 'SequenceNumber': '71689300000000011890998248',
                      'SizeBytes': 13, 'StreamViewType': 'KEYS_ONLY'},
         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-08-04T14:38:45.244'},
        {'eventID': 'f7af0bfca73f61f58f3a15c2cd365522', 'eventName': 'MODIFY', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
         'dynamodb': {'ApproximateCreationDateTime': 1691160109.0, 'Keys': {'id': {'S': '6211_insert'}}, 'SequenceNumber': '71689400000000011890998320',
                      'SizeBytes': 13, 'StreamViewType': 'KEYS_ONLY'},
         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-08-04T14:38:45.244'},
        {'eventID': '6d5118f096ee063d9500b51030300c65', 'eventName': 'INSERT', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
         'dynamodb': {'ApproximateCreationDateTime': 1691160109.0, 'Keys': {'id': {'S': '2764_update'}}, 'SequenceNumber': '71689500000000011890998321',
                      'SizeBytes': 13, 'StreamViewType': 'KEYS_ONLY'},
         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-08-04T14:38:45.244'},
        {'eventID': 'a32e4b8f0219d7c8d3842e09cd20bb87', 'eventName': 'MODIFY', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
         'dynamodb': {'ApproximateCreationDateTime': 1691160109.0, 'Keys': {'id': {'S': '2764_update'}}, 'SequenceNumber': '71689600000000011890998324',
                      'SizeBytes': 13, 'StreamViewType': 'KEYS_ONLY'},
         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-08-04T14:38:45.244'},
        {'eventID': 'ade38897c53e3fd566382f85f1eaef24', 'eventName': 'MODIFY', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
         'dynamodb': {'ApproximateCreationDateTime': 1691160109.0, 'Keys': {'id': {'S': '2764_update'}}, 'SequenceNumber': '71689700000000011890998325',
                      'SizeBytes': 13, 'StreamViewType': 'KEYS_ONLY'},
         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-08-04T14:38:45.244'},
        {'eventID': '760eedd7d431e38474fbe935e33dc149', 'eventName': 'INSERT', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
         'dynamodb': {'ApproximateCreationDateTime': 1691160109.0, 'Keys': {'id': {'S': '4381_delete'}}, 'SequenceNumber': '71689800000000011890998326',
                      'SizeBytes': 13, 'StreamViewType': 'KEYS_ONLY'},
         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-08-04T14:38:45.244'},
        {'eventID': 'a036582b184cc8abcca2c197642d9fd6', 'eventName': 'REMOVE', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
         'dynamodb': {'ApproximateCreationDateTime': 1691160109.0, 'Keys': {'id': {'S': '4381_delete'}}, 'SequenceNumber': '71689900000000011890998327',
                      'SizeBytes': 13, 'StreamViewType': 'KEYS_ONLY'},
         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-08-04T14:38:45.244'}
    ]}


def test_get_table_name(events_keys_only_insert):
    # GIVEN
    expected_table_name = 'laboratorio_eventos_stream_lambda'

    # WHEN
    table_name = get_table_name(event=events_keys_only_insert)

    # THEN
    assert table_name == expected_table_name


@mock.patch.dict(os.environ, {'KAFKA_HOSTS': 'localhost:9091'}, clear=True)
def test_get_kafka_hosts():
    # GIVEN
    expected_kafka_hosts = 'localhost:9091'.split(',')

    # WHEN
    kafka_hosts = get_kafka_hosts()

    # THEN
    assert kafka_hosts == expected_kafka_hosts


@mock.patch.dict(os.environ, {'KAFKA_HOSTS': 'localhost:9091,localhost:9092'}, clear=True)
def test_get_kafka_two_hosts():
    # GIVEN
    expected_kafka_hosts = 'localhost:9091,localhost:9092'.split(',')

    # WHEN
    kafka_hosts = get_kafka_hosts()

    # THEN
    assert kafka_hosts == expected_kafka_hosts
    assert len(kafka_hosts) == len(expected_kafka_hosts)


def test_get_kafka_hosts_not_found_fails():
    # GIVEN
    expected_exception = ValueError

    # WHEN / THEN
    with pytest.raises(expected_exception):
        get_kafka_hosts()


@mock.patch.dict(os.environ, {'KAFKA_HOSTS': 'localhost:9091', 'KAFKA_SECRET_ID': 'test_kakfa_secret_id'}, clear=True)
def test_get_credentials():
    # GIVEN
    expected_credentials = {
        'security_protocol': 'SASL_SSL',
        'sasl_mechanism': 'SCRAM-SHA-512',
        'sasl_plain_username': 'b1',
        'sasl_plain_password': 'b1'
    }
    kakfa_secret = '{"security_protocol": "SASL_SSL", "sasl_mechanism":"SCRAM-SHA-512", "sasl_plain_username": "b1", "sasl_plain_password": "b1"}'
    with patch('kafka_utils.utils.get_kakfa_secret', return_value=kakfa_secret):
        # WHEN
        credentials = get_credentials()

        # THEN
        assert credentials == expected_credentials


@mock.patch.dict(os.environ, {'KAFKA_HOSTS': 'localhost:9091'}, clear=True)
def test_get_config_kakfa_producer_default():
    # GIVEN
    expected_credentials = {
        'bootstrap_servers': get_kafka_hosts(),
        'acks': 'all',
        'request_timeout_ms': 5000,
        'max_block_ms': 10000,
        'api_version': (2, 7, 0),
        'key_serializer': key_serializer,
        'value_serializer': value_serializer
    }

    kakfa_secret = '{"security_protocol": "SASL_SSL", "sasl_mechanism":"SCRAM-SHA-512", "sasl_plain_username": "b1", "sasl_plain_password": "b1"}'
    with patch('kafka_utils.utils.get_kakfa_secret', return_value=kakfa_secret):
        # WHEN
        credentials = get_config_kakfa_producer()

        # THEN
        assert credentials == expected_credentials


@mock.patch.dict(os.environ, {'KAFKA_HOSTS': 'localhost:9091', 'KAFKA_SECRET_ID': 'test_kakfa_secret_id'}, clear=True)
def test_get_config_kakfa_producer_with_credential():
    # GIVEN
    expected_credentials = {
        'bootstrap_servers': get_kafka_hosts(),
        'security_protocol': 'SASL_SSL',
        'sasl_mechanism': 'SCRAM-SHA-512',
        'sasl_plain_username': 'b1',
        'sasl_plain_password': 'b1',
        'acks': 'all',
        'request_timeout_ms': 5000,
        'max_block_ms': 10000,
        'api_version': (2, 7, 0),
        'key_serializer': key_serializer,
        'value_serializer': value_serializer
    }

    kakfa_secret = '{"security_protocol": "SASL_SSL", "sasl_mechanism":"SCRAM-SHA-512", "sasl_plain_username": "b1", "sasl_plain_password": "b1"}'
    with patch('kafka_utils.utils.get_kakfa_secret', return_value=kakfa_secret):
        # WHEN
        credentials = get_config_kakfa_producer()

        # THEN
        assert credentials == expected_credentials


def test_count_call_send_to_kafka_and_throw_kafka_error():
    # GIVEN
    expected_send_call_count = 3
    expected_exception = KafkaError
    producer = MagicMock()
    producer.send.side_effect = KafkaError()

    # WHEN / THEN
    with pytest.raises(expected_exception):
        send_to_kafka(producer=producer, topic='abc-topic', key='abc-key', value='abc-value')

    # THEN
    assert producer.send.call_count == expected_send_call_count


def test_deserializer_element():
    # GIVEN
    element = {'name': {'S': 'nome 8885_insert'}, 'id': {'S': '8885_insert'}, 'time_to_exist': {'N': '1691155958'}}
    expected_element = {'name': 'nome 8885_insert', 'id': '8885_insert', 'time_to_exist': Decimal('1691155958')}

    # WHEN
    result_element = deserializer_element(deserializer=TypeDeserializer(), element=element)

    # THEN
    assert result_element == expected_element


def test_transform_old_and_new_images():
    # GIVEN
    element = {
        'eventID': '5aba71ff56bad43985c5aba577277906', 'eventName': 'MODIFY', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
        'dynamodb': {'ApproximateCreationDateTime': 1691159558.0, 'Keys': {'id': {'S': '8885_insert'}},
                     'NewImage': {'name': {'S': 'nome 8885_insert'}, 'id': {'S': '8885_insert'}, 'time_to_exist': {'N': '1691155958'}},
                     'OldImage': {'name': {'S': 'nome 8885_insert'}, 'id': {'S': '8885_insert'}, 'time_to_exist': {'N': '1691159618'}},
                     'SequenceNumber': '71404200000000059650943607', 'SizeBytes': 117, 'StreamViewType': 'NEW_AND_OLD_IMAGES'},
        'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-07-20T18:21:17.673'}
    expected_element = {
        'eventID': '5aba71ff56bad43985c5aba577277906', 'eventName': 'MODIFY', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
        'dynamodb': {'ApproximateCreationDateTime': 1691159558.0, 'Keys': {'id': '8885_insert'},
                     'NewImage': {'name': 'nome 8885_insert', 'id': '8885_insert', 'time_to_exist': Decimal('1691155958')},
                     'OldImage': {'name': 'nome 8885_insert', 'id': '8885_insert', 'time_to_exist': Decimal('1691159618')},
                     'SequenceNumber': '71404200000000059650943607', 'SizeBytes': 117, 'StreamViewType': 'NEW_AND_OLD_IMAGES'},
        'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-07-20T18:21:17.673'}

    # WHEN
    result_element = transform(deserializer=TypeDeserializer(), element=element)

    # THEN
    assert result_element == expected_element


@mock.patch.dict(os.environ, {'KAFKA_HOSTS': 'localhost:9091', 'KAFKA_SECRET_ID': 'test_kakfa_secret_id'}, clear=True)
def test_create_kakfa_producer():
    # GIVEN
    kafka_config_keys = [
        'bootstrap_servers',
        'security_protocol',
        'sasl_mechanism',
        'sasl_plain_username',
        'sasl_plain_password',
        'acks',
        'request_timeout_ms',
        'max_block_ms',
        'api_version',
        'key_serializer',
        'value_serializer'
    ]
    expected_kakfa_producer_config = {
        'bootstrap_servers': get_kafka_hosts(),
        'security_protocol': 'SASL_SSL',
        'sasl_mechanism': 'SCRAM-SHA-512',
        'sasl_plain_username': 'b1',
        'sasl_plain_password': 'b1',
        'acks': -1,
        'request_timeout_ms': 5000,
        'max_block_ms': 10000,
        'api_version': (2, 7, 0),
        'key_serializer': key_serializer,
        'value_serializer': value_serializer
    }

    kakfa_secret = '{"security_protocol": "SASL_SSL", "sasl_mechanism":"SCRAM-SHA-512", "sasl_plain_username": "b1", "sasl_plain_password": "b1"}'
    with patch('kafka_utils.utils.get_kakfa_secret', return_value=kakfa_secret):
        # WHEN
        kafka_producer = create_kakfa_producer()
        kafka_producer.close()
        kakfa_config = {key: kafka_producer.config[key] for key in kafka_config_keys}

        # THEN
        assert kakfa_config == expected_kakfa_producer_config


def test_key_serializer():
    # GIVEN
    key = {'Keys': {'id': '8885_insert'}}
    expected_key = b'{"Keys": {"id": "8885_insert"}}'

    # WHEN
    result_key = key_serializer(key)

    # THEN
    assert result_key == expected_key


def test_value_serializer():
    # GIVEN
    value = {'NewImage': {'name': 'nome 8885_insert', 'id': '8885_insert', 'time_to_exist': Decimal('1691155958')}}
    expected_value = b'{"NewImage": {"name": "nome 8885_insert", "id": "8885_insert", "time_to_exist": "1691155958"}}'

    # WHEN
    result_value = value_serializer(value)

    # THEN
    assert result_value == expected_value
