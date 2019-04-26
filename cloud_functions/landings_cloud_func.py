import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
import logging
import tempfile
import gzip
from google.cloud import storage

LANDING_COLUMNS = ['match_id', 'map_name', 'game_mode', "x", "y", "zone", "player"]


def get_prefix_from_name(file_name):
    return file_name[:file_name.rindex("/")+1]

def get_last_n_published(client, bucket, prefix, n):
    print(bucket)
    b = client.bucket(bucket_name=bucket)
    print(b)
    blobs = b.list_blobs(prefix=prefix, delimiter="/")
    blob_list = list(blobs)
    blob_list.sort(key=map_blob_names, reverse=True)
    return blob_list[:n]

def parquets_to_df(client, bucket, blob_list):
    df = pd.DataFrame()
    for blob in blob_list:
        f = io.BytesIO()
        blob.download_to_file(f)
        _df = pq.ParquetFile(f).read().to_pandas()
        if not 'victim_x' in _df.columns and not 'item' in _df.columns:
            print(_df.shape)
            df = df.append(_df[LANDING_COLUMNS])
        elif 'event' in _df.columns:
            print(_df.shape)
            _df = _df[_df.event.str.contains("LAND")]
            df = df.append(_df[LANDING_COLUMNS])
        else:
            print(_df.shape)
            _df = _df[_df.item.isnull() & _df.weapon.isnull()]
            df = df.append(_df[LANDING_COLUMNS])
        print(f"new shape: {df.shape}")
        f.close()
    return df

def zip_and_upload_csv(dataframe, map_name, bucket):
    map_df = dataframe[dataframe.map_name == map_name]
    if map_df.shape[0] > 0:
        with tempfile.TemporaryDirectory() as tmp_dir:
            csv_string = map_df.to_csv()
            gzip_bytes = io.BytesIO(gzip.compress(csv_string.encode('utf-8')))
            blob = bucket.blob(f"landings/{map_name}.csv")
            blob.content_encoding="gzip"
            blob.upload_from_file(gzip_bytes)
            print("It is written!")  

def function_handler(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    storage_client = storage.Client()
    file = event
    print(f"Processing file: {file['bucket']}/{file['name']}.")
    bucket_name = file['bucket']
    name = file['name']
    prefix = get_prefix_from_name(name)
    blob_list = get_last_n_published(storage_client, bucket_name, prefix, 10)
    df = parquets_to_df(storage_client, bucket_name, blob_list)
    print(df.shape)
    publish_bucket = storage_client.bucket('pubg-hackathon-published')
    maps_df = df.groupby('map_name').match_id.nunique()
    for map in maps_df.index:
        try:
            zip_and_upload_csv(df, map, publish_bucket)
            print(f"Published map: {map}")
        except:
            print(f"error publishing {map}")



def map_blob_names(blobject):
    return blobject.name

if __name__ == "__main__":
    event = {
        "bucket": "pubg-hackathon",
        "name": "pubg/landings/parquet/landings_2019-04-24.parquet"
    }
    function_handler(event, None)