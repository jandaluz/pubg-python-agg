import os
import sys
import time
import logging
import datetime
import json
from google.cloud import storage, pubsub_v1
import requests
import landings

logger = logging.getLogger('pubg-agg')
logger.setLevel(logging.DEBUG)


def main(date=None):
    gcp_instance_name = get_instance_name()
    gcp_instance_zone = get_instance_zone()

    print('starting pubg-agg GCP')
    print(f'instance name: {gcp_instance_name}')
    logger.info('starting pubg-agg GCP')
    print(os.environ)
    bucket = os.environ["BUCKET"]
    prefix = os.environ["PREFIX"]

    print('get landings from samples')
    logger.info('get landings from samples')
    files = landings.main(date)
    print('files returned')
    logger.info('files returned')
    print(files)
    logger.info(files)
    if date is None:
        date = datetime.datetime.now().strftime("%Y-%m-%d")
    parquet_dest = f"{prefix}/parquet/landings_{date}.parquet"
    csv_dest = f"{prefix}/csv/landings_{date}.csv"

    print('prepare gcp write')
    logger.info('prepare gcp write')
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name=bucket)
    parquet_blob = bucket.blob(parquet_dest)
    csv_blob = bucket.blob(csv_dest)

    try:
        print('try upload parquet')
        logger.info('try upload parquet')
        parquet_blob.upload_from_filename(files["p"])
    except Exception as e:
        print('parquet upload failure')
        logger.error('parquet upload failure', exc_info=True)
        print(e)
    try:
        print('try uplaod csv')
        logger.info('try uplaod csv')
        csv_blob.upload_from_filename(files["c"])
    except Exception as e:
        print('csv upload failure')
        logger.error('csv upload failure', exc_info=True)
        print(e)

    send_instance_shutdown_message(gcp_instance_name, gcp_instance_zone)
    return 0


def get_instance_name():
    metadata_url = "http://metadata.google.internal/computeMetadata/v1/instance/name"
    headers = {"Metadata-Flavor": "Google"}
    r = requests.get(metadata_url, headers=headers)

    if(r.status_code == 200):
        return str(r.text)


def get_instance_zone():
    metadata_url = "http://metadata.google.internal/computeMetadata/v1/instance/zone"
    headers = {"Metadata-Flavor": "Google"}

    r = requests.get(metadata_url, headers=headers)

    if r.status_code == 200:
        full_zone = str(r.text)
        my_zone = full_zone[full_zone.rfind("/")+1:]
        return my_zone


def send_instance_shutdown_message(instance_name, zone):
    print('publish to shutdown pubsub')
    publisher = pubsub_v1.PublisherClient()
    topic_name = f"projects/pubg-hackathon/topics/stop-instance-event"
    input_dict = {
        "zone": zone,
        "instance": instance_name
    }
    json_bytes = json.dumps(input_dict).encode('utf-8')

    future = publisher.publish(topic_name, json_bytes)
    future.add_done_callback(lambda x: print(x.result()))

if __name__ == "__main__":
    formatted_date = None
    if len(sys.argv) > 1:
        input_date = sys.argv[1]
        formatted_date = f"{input_date}"

    main(formatted_date)
