import logging

from boto3.dynamodb.types import TypeDeserializer
from kafka.errors import KafkaError

from kafka_utils.utils import create_kakfa_producer, get_table_name, send_to_kafka, transform

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    result = {'batchItemFailures': []}
    logger.info('Event: %s', event)
    producer = create_kakfa_producer()
    table_name = get_table_name(event)
    deserializer = TypeDeserializer()
    for element in event['Records']:
        sequence_number = element['dynamodb']['SequenceNumber']
        element = transform(deserializer, element)
        try:
            send_to_kafka(
                producer=producer,
                topic=f'data-ingestion-pipeline-{table_name}',
                key=element['dynamodb']['Keys'],
                value=element
            )
        except KafkaError as e:
            logger.warning(f'Error sending message to kafka, {e}')
            result['batchItemFailures'].append({'itemIdentifier': sequence_number})
    try:
        producer.flush()
        producer.close()
    except KafkaError:
        logger.error('Error sending message to kafka', exc_info=True)
    finally:
        return result
