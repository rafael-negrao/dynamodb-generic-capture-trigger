from unittest.mock import patch, MagicMock

import pytest
from kafka.errors import KafkaError

from kafka_utils import utils
from send_to_kafka_function import lambda_handler


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


@pytest.fixture
def events_keys_only_insert_ttl():
    return {'Records': [{
        'eventID': '7bd858d6433c57b3753ddb9fcf437c1b', 'eventName': 'REMOVE', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
        'dynamodb': {'ApproximateCreationDateTime': 1691160110.0, 'Keys': {'id': {'S': '2764_update'}}, 'SequenceNumber': '71690000000000011890998825',
                     'SizeBytes': 13, 'StreamViewType': 'KEYS_ONLY'}, 'userIdentity': {'principalId': 'dynamodb.amazonaws.com', 'type': 'Service'},
        'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-08-04T14:38:45.244'},
        {'eventID': '673c8923520a815e60b3f54936175c0c', 'eventName': 'REMOVE', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
         'dynamodb': {'ApproximateCreationDateTime': 1691160110.0, 'Keys': {'id': {'S': '6211_insert'}}, 'SequenceNumber': '71690100000000011890998826',
                      'SizeBytes': 13, 'StreamViewType': 'KEYS_ONLY'}, 'userIdentity': {'principalId': 'dynamodb.amazonaws.com', 'type': 'Service'},
         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-08-04T14:38:45.244'}]}


@pytest.fixture
def events_keys_only_update():
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
         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-08-04T14:38:45.244'}]}


@pytest.fixture
def events_keys_only_update_ttl():
    return {'Records': [
        {'eventID': '7bd858d6433c57b3753ddb9fcf437c1b', 'eventName': 'REMOVE', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
         'dynamodb': {'ApproximateCreationDateTime': 1691160110.0, 'Keys': {'id': {'S': '2764_update'}}, 'SequenceNumber': '71690000000000011890998825',
                      'SizeBytes': 13, 'StreamViewType': 'KEYS_ONLY'}, 'userIdentity': {'principalId': 'dynamodb.amazonaws.com', 'type': 'Service'},
         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-08-04T14:38:45.244'},
        {'eventID': '673c8923520a815e60b3f54936175c0c', 'eventName': 'REMOVE', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
         'dynamodb': {'ApproximateCreationDateTime': 1691160110.0, 'Keys': {'id': {'S': '6211_insert'}}, 'SequenceNumber': '71690100000000011890998826',
                      'SizeBytes': 13, 'StreamViewType': 'KEYS_ONLY'}, 'userIdentity': {'principalId': 'dynamodb.amazonaws.com', 'type': 'Service'},
         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-08-04T14:38:45.244'}]}


@pytest.fixture
def events_keys_only_delete():
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
         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-08-04T14:38:45.244'}]}


@pytest.fixture
def events_new_and_old_images_insert():
    return {'Records': [
        {'eventID': '14ae2baba0b8955f73a150187a1666ee', 'eventName': 'INSERT', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
         'dynamodb': {'ApproximateCreationDateTime': 1691159558.0, 'Keys': {'id': {'S': '8885_insert'}},
                      'NewImage': {'name': {'S': 'nome 8885_insert'}, 'id': {'S': '8885_insert'}, 'time_to_exist': {'N': '1691159618'}},
                      'SequenceNumber': '71404100000000059650943606', 'SizeBytes': 65, 'StreamViewType': 'NEW_AND_OLD_IMAGES'},
         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-07-20T18:21:17.673'},
        {'eventID': '5aba71ff56bad43985c5aba577277906', 'eventName': 'MODIFY', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
         'dynamodb': {'ApproximateCreationDateTime': 1691159558.0, 'Keys': {'id': {'S': '8885_insert'}},
                      'NewImage': {'name': {'S': 'nome 8885_insert'}, 'id': {'S': '8885_insert'}, 'time_to_exist': {'N': '1691155958'}},
                      'OldImage': {'name': {'S': 'nome 8885_insert'}, 'id': {'S': '8885_insert'}, 'time_to_exist': {'N': '1691159618'}},
                      'SequenceNumber': '71404200000000059650943607', 'SizeBytes': 117, 'StreamViewType': 'NEW_AND_OLD_IMAGES'},
         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-07-20T18:21:17.673'},
        {'eventID': '6ebe1fcb06ae52254327e00a78151157', 'eventName': 'INSERT', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
         'dynamodb': {'ApproximateCreationDateTime': 1691159558.0, 'Keys': {'id': {'S': '5758_update'}},
                      'NewImage': {'name': {'S': 'nome 5758_update'}, 'id': {'S': '5758_update'}, 'time_to_exist': {'N': '1691159618'}},
                      'SequenceNumber': '71404300000000059650943608', 'SizeBytes': 65, 'StreamViewType': 'NEW_AND_OLD_IMAGES'},
         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-07-20T18:21:17.673'},
        {'eventID': '5ee2cb5065944338eea79a8c37355e37', 'eventName': 'MODIFY', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
         'dynamodb': {'ApproximateCreationDateTime': 1691159558.0, 'Keys': {'id': {'S': '5758_update'}},
                      'NewImage': {'name': {'S': 'nome 5758_update _update'}, 'id': {'S': '5758_update'}, 'time_to_exist': {'N': '1691159618'}},
                      'OldImage': {'name': {'S': 'nome 5758_update'}, 'id': {'S': '5758_update'}, 'time_to_exist': {'N': '1691159618'}},
                      'SequenceNumber': '71404400000000059650943610', 'SizeBytes': 125, 'StreamViewType': 'NEW_AND_OLD_IMAGES'},
         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-07-20T18:21:17.673'},
        {'eventID': '18c0a34f07542352814ce52d86dbfcc8', 'eventName': 'MODIFY', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
         'dynamodb': {'ApproximateCreationDateTime': 1691159558.0, 'Keys': {'id': {'S': '5758_update'}},
                      'NewImage': {'name': {'S': 'nome 5758_update _update'}, 'id': {'S': '5758_update'}, 'time_to_exist': {'N': '1691155958'}},
                      'OldImage': {'name': {'S': 'nome 5758_update _update'}, 'id': {'S': '5758_update'}, 'time_to_exist': {'N': '1691159618'}},
                      'SequenceNumber': '71404500000000059650943611', 'SizeBytes': 133, 'StreamViewType': 'NEW_AND_OLD_IMAGES'},
         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-07-20T18:21:17.673'},
        {'eventID': '8c8bca6ab51f9cd9f568acbe75693ca4', 'eventName': 'INSERT', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
         'dynamodb': {'ApproximateCreationDateTime': 1691159558.0, 'Keys': {'id': {'S': '5153_delete'}},
                      'NewImage': {'name': {'S': 'nome 5153_delete _delete'}, 'id': {'S': '5153_delete'}, 'time_to_exist': {'N': '1691159618'}},
                      'SequenceNumber': '71404600000000059650943612', 'SizeBytes': 73, 'StreamViewType': 'NEW_AND_OLD_IMAGES'},
         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-07-20T18:21:17.673'},
        {'eventID': 'd59eb1e24d64f192ff96783ce425ae62', 'eventName': 'REMOVE', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
         'dynamodb': {'ApproximateCreationDateTime': 1691159558.0, 'Keys': {'id': {'S': '5153_delete'}},
                      'OldImage': {'name': {'S': 'nome 5153_delete _delete'}, 'id': {'S': '5153_delete'}, 'time_to_exist': {'N': '1691159618'}},
                      'SequenceNumber': '71404700000000059650943613', 'SizeBytes': 73, 'StreamViewType': 'NEW_AND_OLD_IMAGES'},
         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-07-20T18:21:17.673'}
    ]}


@pytest.fixture
def events_new_and_old_images_insert_and_update():
    return {'Records': [
        {'eventID': '14ae2baba0b8955f73a150187a1666ee', 'eventName': 'INSERT', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
         'dynamodb': {'ApproximateCreationDateTime': 1691159558.0, 'Keys': {'id': {'S': '8885_insert'}},
                      'NewImage': {'name': {'S': 'nome 8885_insert'}, 'id': {'S': '8885_insert'}, 'time_to_exist': {'N': '1691159618'}},
                      'SequenceNumber': '71404100000000059650943606', 'SizeBytes': 65, 'StreamViewType': 'NEW_AND_OLD_IMAGES'},
         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-07-20T18:21:17.673'},
        {'eventID': '5aba71ff56bad43985c5aba577277906', 'eventName': 'MODIFY', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
         'dynamodb': {'ApproximateCreationDateTime': 1691159558.0, 'Keys': {'id': {'S': '8885_insert'}},
                      'NewImage': {'name': {'S': 'nome 8885_insert'}, 'id': {'S': '8885_insert'}, 'time_to_exist': {'N': '1691155958'}},
                      'OldImage': {'name': {'S': 'nome 8885_insert'}, 'id': {'S': '8885_insert'}, 'time_to_exist': {'N': '1691159618'}},
                      'SequenceNumber': '71404200000000059650943607', 'SizeBytes': 117, 'StreamViewType': 'NEW_AND_OLD_IMAGES'},
         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-07-20T18:21:17.673'},
        {'eventID': '6ebe1fcb06ae52254327e00a78151157', 'eventName': 'INSERT', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
         'dynamodb': {'ApproximateCreationDateTime': 1691159558.0, 'Keys': {'id': {'S': '5758_update'}},
                      'NewImage': {'name': {'S': 'nome 5758_update'}, 'id': {'S': '5758_update'}, 'time_to_exist': {'N': '1691159618'}},
                      'SequenceNumber': '71404300000000059650943608', 'SizeBytes': 65, 'StreamViewType': 'NEW_AND_OLD_IMAGES'},
         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-07-20T18:21:17.673'},
        {'eventID': '5ee2cb5065944338eea79a8c37355e37', 'eventName': 'MODIFY', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
         'dynamodb': {'ApproximateCreationDateTime': 1691159558.0, 'Keys': {'id': {'S': '5758_update'}},
                      'NewImage': {'name': {'S': 'nome 5758_update _update'}, 'id': {'S': '5758_update'}, 'time_to_exist': {'N': '1691159618'}},
                      'OldImage': {'name': {'S': 'nome 5758_update'}, 'id': {'S': '5758_update'}, 'time_to_exist': {'N': '1691159618'}},
                      'SequenceNumber': '71404400000000059650943610', 'SizeBytes': 125, 'StreamViewType': 'NEW_AND_OLD_IMAGES'},
         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-07-20T18:21:17.673'},
        {'eventID': '18c0a34f07542352814ce52d86dbfcc8', 'eventName': 'MODIFY', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
         'dynamodb': {'ApproximateCreationDateTime': 1691159558.0, 'Keys': {'id': {'S': '5758_update'}},
                      'NewImage': {'name': {'S': 'nome 5758_update _update'}, 'id': {'S': '5758_update'}, 'time_to_exist': {'N': '1691155958'}},
                      'OldImage': {'name': {'S': 'nome 5758_update _update'}, 'id': {'S': '5758_update'}, 'time_to_exist': {'N': '1691159618'}},
                      'SequenceNumber': '71404500000000059650943611', 'SizeBytes': 133, 'StreamViewType': 'NEW_AND_OLD_IMAGES'},
         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-07-20T18:21:17.673'},
        {'eventID': '8c8bca6ab51f9cd9f568acbe75693ca4', 'eventName': 'INSERT', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
         'dynamodb': {'ApproximateCreationDateTime': 1691159558.0, 'Keys': {'id': {'S': '5153_delete'}},
                      'NewImage': {'name': {'S': 'nome 5153_delete _delete'}, 'id': {'S': '5153_delete'}, 'time_to_exist': {'N': '1691159618'}},
                      'SequenceNumber': '71404600000000059650943612', 'SizeBytes': 73, 'StreamViewType': 'NEW_AND_OLD_IMAGES'},
         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-07-20T18:21:17.673'},
        {'eventID': 'd59eb1e24d64f192ff96783ce425ae62', 'eventName': 'REMOVE', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
         'dynamodb': {'ApproximateCreationDateTime': 1691159558.0, 'Keys': {'id': {'S': '5153_delete'}},
                      'OldImage': {'name': {'S': 'nome 5153_delete _delete'}, 'id': {'S': '5153_delete'}, 'time_to_exist': {'N': '1691159618'}},
                      'SequenceNumber': '71404700000000059650943613', 'SizeBytes': 73, 'StreamViewType': 'NEW_AND_OLD_IMAGES'},
         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-07-20T18:21:17.673'}]
    }


@pytest.fixture
def events_new_and_old_images_insert_and_update_ttl():
    return {'Records': [
        {'eventID': '3ee9c752f2643d2f3a584c0dc751e9a1', 'eventName': 'REMOVE', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
         'dynamodb': {'ApproximateCreationDateTime': 1691159559.0, 'Keys': {'id': {'S': '5758_update'}},
                      'OldImage': {'name': {'S': 'nome 5758_update _update'}, 'id': {'S': '5758_update'}, 'time_to_exist': {'N': '1691155958'}},
                      'SequenceNumber': '71404800000000059650945124', 'SizeBytes': 73, 'StreamViewType': 'NEW_AND_OLD_IMAGES'},
         'userIdentity': {'principalId': 'dynamodb.amazonaws.com', 'type': 'Service'},
         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-07-20T18:21:17.673'},
        {'eventID': '40aeb0b46603034565db321147e9cf7e', 'eventName': 'REMOVE', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
         'dynamodb': {'ApproximateCreationDateTime': 1691159559.0, 'Keys': {'id': {'S': '8885_insert'}},
                      'OldImage': {'name': {'S': 'nome 8885_insert'}, 'id': {'S': '8885_insert'}, 'time_to_exist': {'N': '1691155958'}},
                      'SequenceNumber': '71404900000000059650945125', 'SizeBytes': 65, 'StreamViewType': 'NEW_AND_OLD_IMAGES'},
         'userIdentity': {'principalId': 'dynamodb.amazonaws.com', 'type': 'Service'},
         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-07-20T18:21:17.673'}
    ]}


@pytest.fixture
def events_new_and_old_images_delete():
    return {'Records': [
        {'eventID': '14ae2baba0b8955f73a150187a1666ee', 'eventName': 'INSERT', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
         'dynamodb': {'ApproximateCreationDateTime': 1691159558.0, 'Keys': {'id': {'S': '8885_insert'}},
                      'NewImage': {'name': {'S': 'nome 8885_insert'}, 'id': {'S': '8885_insert'}, 'time_to_exist': {'N': '1691159618'}},
                      'SequenceNumber': '71404100000000059650943606', 'SizeBytes': 65, 'StreamViewType': 'NEW_AND_OLD_IMAGES'},
         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-07-20T18:21:17.673'},
        {'eventID': '5aba71ff56bad43985c5aba577277906', 'eventName': 'MODIFY', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
         'dynamodb': {'ApproximateCreationDateTime': 1691159558.0, 'Keys': {'id': {'S': '8885_insert'}},
                      'NewImage': {'name': {'S': 'nome 8885_insert'}, 'id': {'S': '8885_insert'}, 'time_to_exist': {'N': '1691155958'}},
                      'OldImage': {'name': {'S': 'nome 8885_insert'}, 'id': {'S': '8885_insert'}, 'time_to_exist': {'N': '1691159618'}},
                      'SequenceNumber': '71404200000000059650943607', 'SizeBytes': 117, 'StreamViewType': 'NEW_AND_OLD_IMAGES'},
         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-07-20T18:21:17.673'},
        {'eventID': '6ebe1fcb06ae52254327e00a78151157', 'eventName': 'INSERT', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
         'dynamodb': {'ApproximateCreationDateTime': 1691159558.0, 'Keys': {'id': {'S': '5758_update'}},
                      'NewImage': {'name': {'S': 'nome 5758_update'}, 'id': {'S': '5758_update'}, 'time_to_exist': {'N': '1691159618'}},
                      'SequenceNumber': '71404300000000059650943608', 'SizeBytes': 65, 'StreamViewType': 'NEW_AND_OLD_IMAGES'},
         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-07-20T18:21:17.673'},
        {'eventID': '5ee2cb5065944338eea79a8c37355e37', 'eventName': 'MODIFY', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
         'dynamodb': {'ApproximateCreationDateTime': 1691159558.0, 'Keys': {'id': {'S': '5758_update'}},
                      'NewImage': {'name': {'S': 'nome 5758_update _update'}, 'id': {'S': '5758_update'}, 'time_to_exist': {'N': '1691159618'}},
                      'OldImage': {'name': {'S': 'nome 5758_update'}, 'id': {'S': '5758_update'}, 'time_to_exist': {'N': '1691159618'}},
                      'SequenceNumber': '71404400000000059650943610', 'SizeBytes': 125, 'StreamViewType': 'NEW_AND_OLD_IMAGES'},
         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-07-20T18:21:17.673'},
        {'eventID': '18c0a34f07542352814ce52d86dbfcc8', 'eventName': 'MODIFY', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
         'dynamodb': {'ApproximateCreationDateTime': 1691159558.0, 'Keys': {'id': {'S': '5758_update'}},
                      'NewImage': {'name': {'S': 'nome 5758_update _update'}, 'id': {'S': '5758_update'}, 'time_to_exist': {'N': '1691155958'}},
                      'OldImage': {'name': {'S': 'nome 5758_update _update'}, 'id': {'S': '5758_update'}, 'time_to_exist': {'N': '1691159618'}},
                      'SequenceNumber': '71404500000000059650943611', 'SizeBytes': 133, 'StreamViewType': 'NEW_AND_OLD_IMAGES'},
         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-07-20T18:21:17.673'},
        {'eventID': '8c8bca6ab51f9cd9f568acbe75693ca4', 'eventName': 'INSERT', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
         'dynamodb': {'ApproximateCreationDateTime': 1691159558.0, 'Keys': {'id': {'S': '5153_delete'}},
                      'NewImage': {'name': {'S': 'nome 5153_delete _delete'}, 'id': {'S': '5153_delete'}, 'time_to_exist': {'N': '1691159618'}},
                      'SequenceNumber': '71404600000000059650943612', 'SizeBytes': 73, 'StreamViewType': 'NEW_AND_OLD_IMAGES'},
         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-07-20T18:21:17.673'},
        {'eventID': 'd59eb1e24d64f192ff96783ce425ae62', 'eventName': 'REMOVE', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
         'dynamodb': {'ApproximateCreationDateTime': 1691159558.0, 'Keys': {'id': {'S': '5153_delete'}},
                      'OldImage': {'name': {'S': 'nome 5153_delete _delete'}, 'id': {'S': '5153_delete'}, 'time_to_exist': {'N': '1691159618'}},
                      'SequenceNumber': '71404700000000059650943613', 'SizeBytes': 73, 'StreamViewType': 'NEW_AND_OLD_IMAGES'},
         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-07-20T18:21:17.673'}
    ]}


@pytest.fixture
def events_new_image_insert():
    return {'Records': [
        {'eventID': 'e857c2f861534ae9c8aaaad4024156a9', 'eventName': 'INSERT', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
         'dynamodb': {'ApproximateCreationDateTime': 1691160331.0, 'Keys': {'id': {'S': '2340_insert'}},
                      'NewImage': {'name': {'S': 'nome 2340_insert'}, 'id': {'S': '2340_insert'}, 'time_to_exist': {'N': '1691160391'}},
                      'SequenceNumber': '71711400000000017377709940', 'SizeBytes': 65, 'StreamViewType': 'NEW_IMAGE'},
         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-08-04T14:44:11.713'},
        {'eventID': '1afa8e7979d64ae24aef5bbeeaaf550a', 'eventName': 'MODIFY', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
         'dynamodb': {'ApproximateCreationDateTime': 1691160331.0, 'Keys': {'id': {'S': '2340_insert'}},
                      'NewImage': {'name': {'S': 'nome 2340_insert'}, 'id': {'S': '2340_insert'}, 'time_to_exist': {'N': '1691156731'}},
                      'SequenceNumber': '71711500000000017377709941', 'SizeBytes': 65, 'StreamViewType': 'NEW_IMAGE'},
         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-08-04T14:44:11.713'},
        {'eventID': 'b559650d728d52c77cafb1bb47a76bef', 'eventName': 'INSERT', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
         'dynamodb': {'ApproximateCreationDateTime': 1691160331.0, 'Keys': {'id': {'S': '8309_update'}},
                      'NewImage': {'name': {'S': 'nome 8309_update'}, 'id': {'S': '8309_update'}, 'time_to_exist': {'N': '1691160391'}},
                      'SequenceNumber': '71711600000000017377709942', 'SizeBytes': 65, 'StreamViewType': 'NEW_IMAGE'},
         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-08-04T14:44:11.713'},
        {'eventID': 'a519ea271e1238426cbcd5e4b96dda26', 'eventName': 'MODIFY', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
         'dynamodb': {'ApproximateCreationDateTime': 1691160331.0, 'Keys': {'id': {'S': '8309_update'}},
                      'NewImage': {'name': {'S': 'nome 8309_update _update'}, 'id': {'S': '8309_update'}, 'time_to_exist': {'N': '1691160391'}},
                      'SequenceNumber': '71711700000000017377709943', 'SizeBytes': 73, 'StreamViewType': 'NEW_IMAGE'},
         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-08-04T14:44:11.713'},
        {'eventID': 'e7fd58def4619385f332c4fdbc3784eb', 'eventName': 'MODIFY', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
         'dynamodb': {'ApproximateCreationDateTime': 1691160331.0, 'Keys': {'id': {'S': '8309_update'}},
                      'NewImage': {'name': {'S': 'nome 8309_update _update'}, 'id': {'S': '8309_update'}, 'time_to_exist': {'N': '1691156731'}},
                      'SequenceNumber': '71711800000000017377709944', 'SizeBytes': 73, 'StreamViewType': 'NEW_IMAGE'},
         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-08-04T14:44:11.713'}
    ]}


@pytest.fixture
def events_new_image_insert_ttl():
    return {'Records': [
        {'eventID': 'c3fe73772549e1becfd9db3a2f49f46f', 'eventName': 'REMOVE', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
         'dynamodb': {'ApproximateCreationDateTime': 1691160332.0, 'Keys': {'id': {'S': '2340_insert'}}, 'SequenceNumber': '71712100000000017377710644', 'SizeBytes': 13,
                      'StreamViewType': 'NEW_IMAGE'}, 'userIdentity': {'principalId': 'dynamodb.amazonaws.com', 'type': 'Service'},
         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-08-04T14:44:11.713'},
        {'eventID': 'efaf79485051b1b6738062183b2ede55', 'eventName': 'REMOVE', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
         'dynamodb': {'ApproximateCreationDateTime': 1691160332.0, 'Keys': {'id': {'S': '8309_update'}}, 'SequenceNumber': '71712200000000017377710645', 'SizeBytes': 13,
                      'StreamViewType': 'NEW_IMAGE'}, 'userIdentity': {'principalId': 'dynamodb.amazonaws.com', 'type': 'Service'},
         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-08-04T14:44:11.713'}
    ]}


@pytest.fixture
def events_new_image_update():
    return {'Records': [
        {'eventID': 'e857c2f861534ae9c8aaaad4024156a9', 'eventName': 'INSERT', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
         'dynamodb': {'ApproximateCreationDateTime': 1691160331.0, 'Keys': {'id': {'S': '2340_insert'}},
                      'NewImage': {'name': {'S': 'nome 2340_insert'}, 'id': {'S': '2340_insert'}, 'time_to_exist': {'N': '1691160391'}},
                      'SequenceNumber': '71711400000000017377709940', 'SizeBytes': 65, 'StreamViewType': 'NEW_IMAGE'},
         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-08-04T14:44:11.713'},
        {'eventID': '1afa8e7979d64ae24aef5bbeeaaf550a', 'eventName': 'MODIFY', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
         'dynamodb': {'ApproximateCreationDateTime': 1691160331.0, 'Keys': {'id': {'S': '2340_insert'}},
                      'NewImage': {'name': {'S': 'nome 2340_insert'}, 'id': {'S': '2340_insert'}, 'time_to_exist': {'N': '1691156731'}},
                      'SequenceNumber': '71711500000000017377709941', 'SizeBytes': 65, 'StreamViewType': 'NEW_IMAGE'},
         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-08-04T14:44:11.713'},
        {'eventID': 'b559650d728d52c77cafb1bb47a76bef', 'eventName': 'INSERT', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
         'dynamodb': {'ApproximateCreationDateTime': 1691160331.0, 'Keys': {'id': {'S': '8309_update'}},
                      'NewImage': {'name': {'S': 'nome 8309_update'}, 'id': {'S': '8309_update'}, 'time_to_exist': {'N': '1691160391'}},
                      'SequenceNumber': '71711600000000017377709942', 'SizeBytes': 65, 'StreamViewType': 'NEW_IMAGE'},
         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-08-04T14:44:11.713'},
        {'eventID': 'a519ea271e1238426cbcd5e4b96dda26', 'eventName': 'MODIFY', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
         'dynamodb': {'ApproximateCreationDateTime': 1691160331.0, 'Keys': {'id': {'S': '8309_update'}},
                      'NewImage': {'name': {'S': 'nome 8309_update _update'}, 'id': {'S': '8309_update'}, 'time_to_exist': {'N': '1691160391'}},
                      'SequenceNumber': '71711700000000017377709943', 'SizeBytes': 73, 'StreamViewType': 'NEW_IMAGE'},
         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-08-04T14:44:11.713'},
        {'eventID': 'e7fd58def4619385f332c4fdbc3784eb', 'eventName': 'MODIFY', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
         'dynamodb': {'ApproximateCreationDateTime': 1691160331.0, 'Keys': {'id': {'S': '8309_update'}},
                      'NewImage': {'name': {'S': 'nome 8309_update _update'}, 'id': {'S': '8309_update'}, 'time_to_exist': {'N': '1691156731'}},
                      'SequenceNumber': '71711800000000017377709944', 'SizeBytes': 73, 'StreamViewType': 'NEW_IMAGE'},
         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-08-04T14:44:11.713'}
    ]}


@pytest.fixture
def events_new_image_update_ttl():
    return {'Records': [
        {'eventID': 'c3fe73772549e1becfd9db3a2f49f46f', 'eventName': 'REMOVE', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
         'dynamodb': {'ApproximateCreationDateTime': 1691160332.0, 'Keys': {'id': {'S': '2340_insert'}}, 'SequenceNumber': '71712100000000017377710644', 'SizeBytes': 13,
                      'StreamViewType': 'NEW_IMAGE'}, 'userIdentity': {'principalId': 'dynamodb.amazonaws.com', 'type': 'Service'},
         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-08-04T14:44:11.713'},
        {'eventID': 'efaf79485051b1b6738062183b2ede55', 'eventName': 'REMOVE', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
         'dynamodb': {'ApproximateCreationDateTime': 1691160332.0, 'Keys': {'id': {'S': '8309_update'}}, 'SequenceNumber': '71712200000000017377710645', 'SizeBytes': 13,
                      'StreamViewType': 'NEW_IMAGE'}, 'userIdentity': {'principalId': 'dynamodb.amazonaws.com', 'type': 'Service'},
         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-08-04T14:44:11.713'}
    ]}


@pytest.fixture
def events_new_image_delete():
    return {'Records': [
        {'eventID': '4ae951045d64bdd1743bb665ceb01229', 'eventName': 'INSERT', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
         'dynamodb': {'ApproximateCreationDateTime': 1691160331.0, 'Keys': {'id': {'S': '2110_delete'}},
                      'NewImage': {'name': {'S': 'nome 2110_delete _delete'}, 'id': {'S': '2110_delete'}, 'time_to_exist': {'N': '1691160391'}},
                      'SequenceNumber': '71711900000000017377710036', 'SizeBytes': 73, 'StreamViewType': 'NEW_IMAGE'},
         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-08-04T14:44:11.713'},
        {'eventID': '324af8f0905d1c0daa927a2c856ada1c', 'eventName': 'REMOVE', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
         'dynamodb': {'ApproximateCreationDateTime': 1691160331.0, 'Keys': {'id': {'S': '2110_delete'}}, 'SequenceNumber': '71712000000000017377710037', 'SizeBytes': 13,
                      'StreamViewType': 'NEW_IMAGE'},
         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-08-04T14:44:11.713'}
    ]}


@pytest.fixture
def events_old_image_insert():
    return {'Records': [{'eventID': 'b3d2dc6466406b9a876b62e63fbd7af1', 'eventName': 'INSERT', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
                         'dynamodb': {'ApproximateCreationDateTime': 1691160630.0, 'Keys': {'id': {'S': '8056_insert'}}, 'SequenceNumber': '71728600000000014430921489',
                                      'SizeBytes': 13, 'StreamViewType': 'OLD_IMAGE'},
                         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-08-04T14:47:51.609'},
                        {'eventID': 'c1309188993261a8b5f0c35578c49846', 'eventName': 'MODIFY', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
                         'dynamodb': {'ApproximateCreationDateTime': 1691160630.0, 'Keys': {'id': {'S': '8056_insert'}},
                                      'OldImage': {'name': {'S': 'nome 8056_insert'}, 'id': {'S': '8056_insert'}, 'time_to_exist': {'N': '1691160690'}},
                                      'SequenceNumber': '71728700000000014430921564', 'SizeBytes': 65, 'StreamViewType': 'OLD_IMAGE'},
                         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-08-04T14:47:51.609'},
                        {'eventID': 'a400791e4a8803edc8ba72a4e9180bba', 'eventName': 'INSERT', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
                         'dynamodb': {'ApproximateCreationDateTime': 1691160630.0, 'Keys': {'id': {'S': '5682_update'}}, 'SequenceNumber': '71728800000000014430921565',
                                      'SizeBytes': 13, 'StreamViewType': 'OLD_IMAGE'},
                         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-08-04T14:47:51.609'},
                        {'eventID': '0ac4e169bdf5dd74b32c7fd1ba779a6b', 'eventName': 'MODIFY', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
                         'dynamodb': {'ApproximateCreationDateTime': 1691160630.0, 'Keys': {'id': {'S': '5682_update'}},
                                      'OldImage': {'name': {'S': 'nome 5682_update'}, 'id': {'S': '5682_update'}, 'time_to_exist': {'N': '1691160690'}},
                                      'SequenceNumber': '71728900000000014430921566', 'SizeBytes': 65, 'StreamViewType': 'OLD_IMAGE'},
                         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-08-04T14:47:51.609'},
                        {'eventID': '3f3a60109a0cc451482d875f9746b72b', 'eventName': 'MODIFY', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
                         'dynamodb': {'ApproximateCreationDateTime': 1691160630.0, 'Keys': {'id': {'S': '5682_update'}},
                                      'OldImage': {'name': {'S': 'nome 5682_update _update'}, 'id': {'S': '5682_update'}, 'time_to_exist': {'N': '1691160690'}},
                                      'SequenceNumber': '71729000000000014430921567', 'SizeBytes': 73, 'StreamViewType': 'OLD_IMAGE'},
                         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-08-04T14:47:51.609'},
                        {'eventID': 'e9e044f94fdaf79a92dad646dc0cef52', 'eventName': 'INSERT', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
                         'dynamodb': {'ApproximateCreationDateTime': 1691160630.0, 'Keys': {'id': {'S': '1485_delete'}}, 'SequenceNumber': '71729100000000014430921568',
                                      'SizeBytes': 13, 'StreamViewType': 'OLD_IMAGE'},
                         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-08-04T14:47:51.609'},
                        {'eventID': '7d884471ae35f9774d45372f651d2707', 'eventName': 'REMOVE', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
                         'dynamodb': {'ApproximateCreationDateTime': 1691160630.0, 'Keys': {'id': {'S': '1485_delete'}},
                                      'OldImage': {'name': {'S': 'nome 1485_delete _delete'}, 'id': {'S': '1485_delete'}, 'time_to_exist': {'N': '1691160690'}},
                                      'SequenceNumber': '71729200000000014430921569', 'SizeBytes': 73, 'StreamViewType': 'OLD_IMAGE'},
                         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-08-04T14:47:51.609'}]}


@pytest.fixture
def events_old_image_insert_ttl():
    return {'Records': [{'eventID': 'd15fa050b8ec4ac6d532b4a1eec01b4a', 'eventName': 'REMOVE', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
                         'dynamodb': {'ApproximateCreationDateTime': 1691160631.0, 'Keys': {'id': {'S': '8056_insert'}},
                                      'OldImage': {'name': {'S': 'nome 8056_insert'}, 'id': {'S': '8056_insert'}, 'time_to_exist': {'N': '1691157030'}},
                                      'SequenceNumber': '71729300000000014430922230', 'SizeBytes': 65, 'StreamViewType': 'OLD_IMAGE'},
                         'userIdentity': {'principalId': 'dynamodb.amazonaws.com', 'type': 'Service'},
                         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-08-04T14:47:51.609'},
                        {'eventID': 'e6b2f95ecd50135f8f84cc37f472a09b', 'eventName': 'REMOVE', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
                         'dynamodb': {'ApproximateCreationDateTime': 1691160631.0, 'Keys': {'id': {'S': '5682_update'}},
                                      'OldImage': {'name': {'S': 'nome 5682_update _update'}, 'id': {'S': '5682_update'}, 'time_to_exist': {'N': '1691157030'}},
                                      'SequenceNumber': '71729400000000014430922231', 'SizeBytes': 73, 'StreamViewType': 'OLD_IMAGE'},
                         'userIdentity': {'principalId': 'dynamodb.amazonaws.com', 'type': 'Service'},
                         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-08-04T14:47:51.609'}]}


@pytest.fixture
def events_old_image_update():
    return {'Records': [{'eventID': 'b3d2dc6466406b9a876b62e63fbd7af1', 'eventName': 'INSERT', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
                         'dynamodb': {'ApproximateCreationDateTime': 1691160630.0, 'Keys': {'id': {'S': '8056_insert'}}, 'SequenceNumber': '71728600000000014430921489',
                                      'SizeBytes': 13, 'StreamViewType': 'OLD_IMAGE'},
                         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-08-04T14:47:51.609'},
                        {'eventID': 'c1309188993261a8b5f0c35578c49846', 'eventName': 'MODIFY', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
                         'dynamodb': {'ApproximateCreationDateTime': 1691160630.0, 'Keys': {'id': {'S': '8056_insert'}},
                                      'OldImage': {'name': {'S': 'nome 8056_insert'}, 'id': {'S': '8056_insert'}, 'time_to_exist': {'N': '1691160690'}},
                                      'SequenceNumber': '71728700000000014430921564', 'SizeBytes': 65, 'StreamViewType': 'OLD_IMAGE'},
                         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-08-04T14:47:51.609'},
                        {'eventID': 'a400791e4a8803edc8ba72a4e9180bba', 'eventName': 'INSERT', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
                         'dynamodb': {'ApproximateCreationDateTime': 1691160630.0, 'Keys': {'id': {'S': '5682_update'}}, 'SequenceNumber': '71728800000000014430921565',
                                      'SizeBytes': 13, 'StreamViewType': 'OLD_IMAGE'},
                         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-08-04T14:47:51.609'},
                        {'eventID': '0ac4e169bdf5dd74b32c7fd1ba779a6b', 'eventName': 'MODIFY', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
                         'dynamodb': {'ApproximateCreationDateTime': 1691160630.0, 'Keys': {'id': {'S': '5682_update'}},
                                      'OldImage': {'name': {'S': 'nome 5682_update'}, 'id': {'S': '5682_update'}, 'time_to_exist': {'N': '1691160690'}},
                                      'SequenceNumber': '71728900000000014430921566', 'SizeBytes': 65, 'StreamViewType': 'OLD_IMAGE'},
                         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-08-04T14:47:51.609'},
                        {'eventID': '3f3a60109a0cc451482d875f9746b72b', 'eventName': 'MODIFY', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
                         'dynamodb': {'ApproximateCreationDateTime': 1691160630.0, 'Keys': {'id': {'S': '5682_update'}},
                                      'OldImage': {'name': {'S': 'nome 5682_update _update'}, 'id': {'S': '5682_update'}, 'time_to_exist': {'N': '1691160690'}},
                                      'SequenceNumber': '71729000000000014430921567', 'SizeBytes': 73, 'StreamViewType': 'OLD_IMAGE'},
                         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-08-04T14:47:51.609'},
                        {'eventID': 'e9e044f94fdaf79a92dad646dc0cef52', 'eventName': 'INSERT', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
                         'dynamodb': {'ApproximateCreationDateTime': 1691160630.0, 'Keys': {'id': {'S': '1485_delete'}}, 'SequenceNumber': '71729100000000014430921568',
                                      'SizeBytes': 13, 'StreamViewType': 'OLD_IMAGE'},
                         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-08-04T14:47:51.609'},
                        {'eventID': '7d884471ae35f9774d45372f651d2707', 'eventName': 'REMOVE', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
                         'dynamodb': {'ApproximateCreationDateTime': 1691160630.0, 'Keys': {'id': {'S': '1485_delete'}},
                                      'OldImage': {'name': {'S': 'nome 1485_delete _delete'}, 'id': {'S': '1485_delete'}, 'time_to_exist': {'N': '1691160690'}},
                                      'SequenceNumber': '71729200000000014430921569', 'SizeBytes': 73, 'StreamViewType': 'OLD_IMAGE'},
                         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-08-04T14:47:51.609'}]}


@pytest.fixture
def events_old_image_update_ttl():
    return {'Records': [{'eventID': 'd15fa050b8ec4ac6d532b4a1eec01b4a', 'eventName': 'REMOVE', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
                         'dynamodb': {'ApproximateCreationDateTime': 1691160631.0, 'Keys': {'id': {'S': '8056_insert'}},
                                      'OldImage': {'name': {'S': 'nome 8056_insert'}, 'id': {'S': '8056_insert'}, 'time_to_exist': {'N': '1691157030'}},
                                      'SequenceNumber': '71729300000000014430922230', 'SizeBytes': 65, 'StreamViewType': 'OLD_IMAGE'},
                         'userIdentity': {'principalId': 'dynamodb.amazonaws.com', 'type': 'Service'},
                         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-08-04T14:47:51.609'},
                        {'eventID': 'e6b2f95ecd50135f8f84cc37f472a09b', 'eventName': 'REMOVE', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
                         'dynamodb': {'ApproximateCreationDateTime': 1691160631.0, 'Keys': {'id': {'S': '5682_update'}},
                                      'OldImage': {'name': {'S': 'nome 5682_update _update'}, 'id': {'S': '5682_update'}, 'time_to_exist': {'N': '1691157030'}},
                                      'SequenceNumber': '71729400000000014430922231', 'SizeBytes': 73, 'StreamViewType': 'OLD_IMAGE'},
                         'userIdentity': {'principalId': 'dynamodb.amazonaws.com', 'type': 'Service'},
                         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-08-04T14:47:51.609'}]}


@pytest.fixture
def events_old_image_delete():
    return {'Records': [{'eventID': 'b3d2dc6466406b9a876b62e63fbd7af1', 'eventName': 'INSERT', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
                         'dynamodb': {'ApproximateCreationDateTime': 1691160630.0, 'Keys': {'id': {'S': '8056_insert'}}, 'SequenceNumber': '71728600000000014430921489',
                                      'SizeBytes': 13, 'StreamViewType': 'OLD_IMAGE'},
                         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-08-04T14:47:51.609'},
                        {'eventID': 'c1309188993261a8b5f0c35578c49846', 'eventName': 'MODIFY', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
                         'dynamodb': {'ApproximateCreationDateTime': 1691160630.0, 'Keys': {'id': {'S': '8056_insert'}},
                                      'OldImage': {'name': {'S': 'nome 8056_insert'}, 'id': {'S': '8056_insert'}, 'time_to_exist': {'N': '1691160690'}},
                                      'SequenceNumber': '71728700000000014430921564', 'SizeBytes': 65, 'StreamViewType': 'OLD_IMAGE'},
                         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-08-04T14:47:51.609'},
                        {'eventID': 'a400791e4a8803edc8ba72a4e9180bba', 'eventName': 'INSERT', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
                         'dynamodb': {'ApproximateCreationDateTime': 1691160630.0, 'Keys': {'id': {'S': '5682_update'}}, 'SequenceNumber': '71728800000000014430921565',
                                      'SizeBytes': 13, 'StreamViewType': 'OLD_IMAGE'},
                         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-08-04T14:47:51.609'},
                        {'eventID': '0ac4e169bdf5dd74b32c7fd1ba779a6b', 'eventName': 'MODIFY', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
                         'dynamodb': {'ApproximateCreationDateTime': 1691160630.0, 'Keys': {'id': {'S': '5682_update'}},
                                      'OldImage': {'name': {'S': 'nome 5682_update'}, 'id': {'S': '5682_update'}, 'time_to_exist': {'N': '1691160690'}},
                                      'SequenceNumber': '71728900000000014430921566', 'SizeBytes': 65, 'StreamViewType': 'OLD_IMAGE'},
                         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-08-04T14:47:51.609'},
                        {'eventID': '3f3a60109a0cc451482d875f9746b72b', 'eventName': 'MODIFY', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
                         'dynamodb': {'ApproximateCreationDateTime': 1691160630.0, 'Keys': {'id': {'S': '5682_update'}},
                                      'OldImage': {'name': {'S': 'nome 5682_update _update'}, 'id': {'S': '5682_update'}, 'time_to_exist': {'N': '1691160690'}},
                                      'SequenceNumber': '71729000000000014430921567', 'SizeBytes': 73, 'StreamViewType': 'OLD_IMAGE'},
                         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-08-04T14:47:51.609'},
                        {'eventID': 'e9e044f94fdaf79a92dad646dc0cef52', 'eventName': 'INSERT', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
                         'dynamodb': {'ApproximateCreationDateTime': 1691160630.0, 'Keys': {'id': {'S': '1485_delete'}}, 'SequenceNumber': '71729100000000014430921568',
                                      'SizeBytes': 13, 'StreamViewType': 'OLD_IMAGE'},
                         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-08-04T14:47:51.609'},
                        {'eventID': '7d884471ae35f9774d45372f651d2707', 'eventName': 'REMOVE', 'eventVersion': '1.1', 'eventSource': 'aws:dynamodb', 'awsRegion': 'us-east-1',
                         'dynamodb': {'ApproximateCreationDateTime': 1691160630.0, 'Keys': {'id': {'S': '1485_delete'}},
                                      'OldImage': {'name': {'S': 'nome 1485_delete _delete'}, 'id': {'S': '1485_delete'}, 'time_to_exist': {'N': '1691160690'}},
                                      'SequenceNumber': '71729200000000014430921569', 'SizeBytes': 73, 'StreamViewType': 'OLD_IMAGE'},
                         'eventSourceARN': 'arn:aws:dynamodb:us-east-1:463684499885:table/laboratorio_eventos_stream_lambda/stream/2023-08-04T14:47:51.609'}]}


utils.RETRY_DELAY = 0


def test_send_events_keys_only_insert(events_keys_only_insert):
    # GIVEN
    event = events_keys_only_insert
    expected_batch_status = {"batchItemFailures": []}
    expected_send_call_count = len(event['Records'])
    context = {}
    producer = MagicMock()
    producer.send.return_value = True
    producer.flush.return_value = True
    producer.close.return_value = True

    with patch('send_to_kafka_function.create_kakfa_producer', return_value=producer):
        # WHEN
        batch_status_return = lambda_handler(event=event, context=context)

        # THEN
        assert batch_status_return == expected_batch_status
        assert producer.send.call_count == expected_send_call_count
        producer.send.assert_called()
        producer.flush.assert_called_once()
        producer.close.assert_called_once()


def test_send_events_keys_only_insert(events_new_and_old_images_insert):
    # GIVEN
    event = events_new_and_old_images_insert
    expected_batch_status = {"batchItemFailures": []}
    expected_send_call_count = len(event['Records'])
    context = {}
    producer = MagicMock()
    producer.send.return_value = True
    producer.flush.return_value = True
    producer.close.return_value = True

    with patch('send_to_kafka_function.create_kakfa_producer', return_value=producer):
        # WHEN
        batch_status_return = lambda_handler(event=event, context=context)

        # THEN
        assert batch_status_return == expected_batch_status
        assert producer.send.call_count == expected_send_call_count
        producer.send.assert_called()
        producer.flush.assert_called_once()
        producer.close.assert_called_once()


def test_events_new_and_old_images_insert_and_update(events_new_and_old_images_insert_and_update):
    # GIVEN
    event = events_new_and_old_images_insert_and_update
    expected_batch_status = {"batchItemFailures": []}
    expected_send_call_count = len(event['Records'])
    context = {}
    producer = MagicMock()
    producer.send.return_value = True
    producer.flush.return_value = True
    producer.close.return_value = True

    with patch('send_to_kafka_function.create_kakfa_producer', return_value=producer):
        # WHEN
        batch_status_return = lambda_handler(event=event, context=context)

        # THEN
        assert batch_status_return == expected_batch_status
        assert producer.send.call_count == expected_send_call_count
        producer.send.assert_called()
        producer.flush.assert_called_once()
        producer.close.assert_called_once()


def test_events_new_and_old_images_insert_and_update_ttl(events_new_and_old_images_insert_and_update_ttl):
    # GIVEN
    event = events_new_and_old_images_insert_and_update_ttl
    expected_batch_status = {"batchItemFailures": []}
    expected_send_call_count = len(event['Records'])
    context = {}
    producer = MagicMock()
    producer.send.return_value = True
    producer.flush.return_value = True
    producer.close.return_value = True

    with patch('send_to_kafka_function.create_kakfa_producer', return_value=producer):
        # WHEN
        batch_status_return = lambda_handler(event=event, context=context)

        # THEN
        assert batch_status_return == expected_batch_status
        assert producer.send.call_count == expected_send_call_count
        producer.send.assert_called()
        producer.flush.assert_called_once()
        producer.close.assert_called_once()


def test_events_new_and_old_images_delete(events_new_and_old_images_delete):
    # GIVEN
    event = events_new_and_old_images_delete
    expected_batch_status = {"batchItemFailures": []}
    expected_send_call_count = len(event['Records'])
    context = {}
    producer = MagicMock()
    producer.send.return_value = True
    producer.flush.return_value = True
    producer.close.return_value = True

    with patch('send_to_kafka_function.create_kakfa_producer', return_value=producer):
        # WHEN
        batch_status_return = lambda_handler(event=event, context=context)

        # THEN
        assert batch_status_return == expected_batch_status
        assert producer.send.call_count == expected_send_call_count
        producer.send.assert_called()
        producer.flush.assert_called_once()
        producer.close.assert_called_once()


def test_events_new_image_insert(events_new_image_insert):
    # GIVEN
    event = events_new_image_insert
    expected_batch_status = {"batchItemFailures": []}
    expected_send_call_count = len(event['Records'])
    context = {}
    producer = MagicMock()
    producer.send.return_value = True
    producer.flush.return_value = True
    producer.close.return_value = True

    with patch('send_to_kafka_function.create_kakfa_producer', return_value=producer):
        # WHEN
        batch_status_return = lambda_handler(event=event, context=context)

        # THEN
        assert batch_status_return == expected_batch_status
        assert producer.send.call_count == expected_send_call_count
        producer.send.assert_called()
        producer.flush.assert_called_once()
        producer.close.assert_called_once()


def test_events_new_image_insert_ttl(events_new_image_insert_ttl):
    # GIVEN
    event = events_new_image_insert_ttl
    expected_batch_status = {"batchItemFailures": []}
    expected_send_call_count = len(event['Records'])
    context = {}
    producer = MagicMock()
    producer.send.return_value = True
    producer.flush.return_value = True
    producer.close.return_value = True

    with patch('send_to_kafka_function.create_kakfa_producer', return_value=producer):
        # WHEN
        batch_status_return = lambda_handler(event=event, context=context)

        # THEN
        assert batch_status_return == expected_batch_status
        assert producer.send.call_count == expected_send_call_count
        producer.send.assert_called()
        producer.flush.assert_called_once()
        producer.close.assert_called_once()


def test_events_new_image_update(events_new_image_update):
    # GIVEN
    event = events_new_image_update
    expected_batch_status = {"batchItemFailures": []}
    expected_send_call_count = len(event['Records'])
    context = {}
    producer = MagicMock()
    producer.send.return_value = True
    producer.flush.return_value = True
    producer.close.return_value = True

    with patch('send_to_kafka_function.create_kakfa_producer', return_value=producer):
        # WHEN
        batch_status_return = lambda_handler(event=event, context=context)

        # THEN
        assert batch_status_return == expected_batch_status
        assert producer.send.call_count == expected_send_call_count
        producer.send.assert_called()
        producer.flush.assert_called_once()
        producer.close.assert_called_once()


def test_events_new_image_update_ttl(events_new_image_update_ttl):
    # GIVEN
    event = events_new_image_update_ttl
    expected_batch_status = {"batchItemFailures": []}
    expected_send_call_count = len(event['Records'])
    context = {}
    producer = MagicMock()
    producer.send.return_value = True
    producer.flush.return_value = True
    producer.close.return_value = True

    with patch('send_to_kafka_function.create_kakfa_producer', return_value=producer):
        # WHEN
        batch_status_return = lambda_handler(event=event, context=context)

        # THEN
        assert batch_status_return == expected_batch_status
        assert producer.send.call_count == expected_send_call_count
        producer.send.assert_called()
        producer.flush.assert_called_once()
        producer.close.assert_called_once()


def test_events_new_image_delete(events_new_image_delete):
    # GIVEN
    event = events_new_image_delete
    expected_batch_status = {"batchItemFailures": []}
    expected_send_call_count = len(event['Records'])
    context = {}
    producer = MagicMock()
    producer.send.return_value = True
    producer.flush.return_value = True
    producer.close.return_value = True

    with patch('send_to_kafka_function.create_kakfa_producer', return_value=producer):
        # WHEN
        batch_status_return = lambda_handler(event=event, context=context)

        # THEN
        assert batch_status_return == expected_batch_status
        assert producer.send.call_count == expected_send_call_count
        producer.send.assert_called()
        producer.flush.assert_called_once()
        producer.close.assert_called_once()


def test_events_old_image_insert(events_old_image_insert):
    # GIVEN
    event = events_old_image_insert
    expected_batch_status = {"batchItemFailures": []}
    expected_send_call_count = len(event['Records'])
    context = {}
    producer = MagicMock()
    producer.send.return_value = True
    producer.flush.return_value = True
    producer.close.return_value = True

    with patch('send_to_kafka_function.create_kakfa_producer', return_value=producer):
        # WHEN
        batch_status_return = lambda_handler(event=event, context=context)

        # THEN
        assert batch_status_return == expected_batch_status
        assert producer.send.call_count == expected_send_call_count
        producer.send.assert_called()
        producer.flush.assert_called_once()
        producer.close.assert_called_once()


def test_events_old_image_insert_ttl(events_old_image_insert_ttl):
    # GIVEN
    event = events_old_image_insert_ttl
    expected_batch_status = {"batchItemFailures": []}
    expected_send_call_count = len(event['Records'])
    context = {}
    producer = MagicMock()
    producer.send.return_value = True
    producer.flush.return_value = True
    producer.close.return_value = True

    with patch('send_to_kafka_function.create_kakfa_producer', return_value=producer):
        # WHEN
        batch_status_return = lambda_handler(event=event, context=context)

        # THEN
        assert batch_status_return == expected_batch_status
        assert producer.send.call_count == expected_send_call_count
        producer.send.assert_called()
        producer.flush.assert_called_once()
        producer.close.assert_called_once()


def test_events_old_image_update(events_old_image_update):
    # GIVEN
    event = events_old_image_update
    expected_batch_status = {"batchItemFailures": []}
    expected_send_call_count = len(event['Records'])
    context = {}
    producer = MagicMock()
    producer.send.return_value = True
    producer.flush.return_value = True
    producer.close.return_value = True

    with patch('send_to_kafka_function.create_kakfa_producer', return_value=producer):
        # WHEN
        batch_status_return = lambda_handler(event=event, context=context)

        # THEN
        assert batch_status_return == expected_batch_status
        assert producer.send.call_count == expected_send_call_count
        producer.send.assert_called()
        producer.flush.assert_called_once()
        producer.close.assert_called_once()


def test_events_old_image_update_ttl(events_old_image_update_ttl):
    # GIVEN
    event = events_old_image_update_ttl
    expected_batch_status = {"batchItemFailures": []}
    expected_send_call_count = len(event['Records'])
    context = {}
    producer = MagicMock()
    producer.send.return_value = True
    producer.flush.return_value = True
    producer.close.return_value = True

    with patch('send_to_kafka_function.create_kakfa_producer', return_value=producer):
        # WHEN
        batch_status_return = lambda_handler(event=event, context=context)

        # THEN
        assert batch_status_return == expected_batch_status
        assert producer.send.call_count == expected_send_call_count
        producer.send.assert_called()
        producer.flush.assert_called_once()
        producer.close.assert_called_once()


def test_events_old_image_delete(events_old_image_delete):
    # GIVEN
    event = events_old_image_delete
    expected_batch_status = {"batchItemFailures": []}
    expected_send_call_count = len(event['Records'])
    context = {}
    producer = MagicMock()
    producer.send.return_value = True
    producer.flush.return_value = True
    producer.close.return_value = True

    with patch('send_to_kafka_function.create_kakfa_producer', return_value=producer):
        # WHEN
        batch_status_return = lambda_handler(event=event, context=context)

        # THEN
        assert batch_status_return == expected_batch_status
        assert producer.send.call_count == expected_send_call_count
        producer.send.assert_called()
        producer.flush.assert_called_once()
        producer.close.assert_called_once()


def test_send_events_keys_only_insert_error(events_keys_only_insert):
    # GIVEN
    event = events_keys_only_insert
    expected_batch_status = {"batchItemFailures": [
        {'itemIdentifier': element['dynamodb']['SequenceNumber']} for element in event['Records']
    ]}
    expected_send_call_count = len(event['Records']) * utils.RETRY_TRIES
    context = {}
    producer = MagicMock()
    producer.send.side_effect = KafkaError()
    producer.flush.return_value = True
    producer.close.return_value = True

    with patch('send_to_kafka_function.create_kakfa_producer', return_value=producer):
        # WHEN
        batch_status_return = lambda_handler(event=event, context=context)

        # THEN
        assert batch_status_return == expected_batch_status
        assert producer.send.call_count == expected_send_call_count
        producer.send.assert_called()
        producer.flush.assert_called_once()
        producer.close.assert_called_once()


def test_send_events_keys_only_insert_error_on_send_and_closed(events_keys_only_insert):
    # GIVEN
    event = events_keys_only_insert
    expected_batch_status = {"batchItemFailures": [
        {'itemIdentifier': element['dynamodb']['SequenceNumber']} for element in event['Records']
    ]}
    expected_send_call_count = len(event['Records']) * utils.RETRY_TRIES
    context = {}
    producer = MagicMock()
    producer.send.side_effect = KafkaError()
    producer.flush.return_value = True
    producer.close.side_effect = KafkaError()

    with patch('send_to_kafka_function.create_kakfa_producer', return_value=producer):
        # WHEN
        batch_status_return = lambda_handler(event=event, context=context)

        # THEN
        assert batch_status_return == expected_batch_status
        assert producer.send.call_count == expected_send_call_count
        producer.send.assert_called()
        producer.flush.assert_called_once()
        producer.close.assert_called_once()


def test_send_events_keys_only_insert_error_on_send_and_flush(events_keys_only_insert):
    # GIVEN
    event = events_keys_only_insert
    expected_batch_status = {"batchItemFailures": [
        {'itemIdentifier': element['dynamodb']['SequenceNumber']} for element in event['Records']
    ]}
    expected_send_call_count = len(event['Records']) * utils.RETRY_TRIES
    context = {}
    producer = MagicMock()
    producer.send.side_effect = KafkaError()
    producer.flush.side_effect = KafkaError()
    producer.close.return_value = True

    with patch('send_to_kafka_function.create_kakfa_producer', return_value=producer):
        # WHEN
        batch_status_return = lambda_handler(event=event, context=context)

        # THEN
        assert batch_status_return == expected_batch_status
        assert producer.send.call_count == expected_send_call_count
        producer.send.assert_called()
        producer.flush.assert_called_once()
        producer.close.assert_not_called()
