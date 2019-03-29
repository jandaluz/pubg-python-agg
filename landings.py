import os
import sys
import time
import pandas as pd
import numpy as np
import matplotlib.pyplot as plot
import matplotlib.image as image
import datetime
from pubg_python import PUBG, Shard
from pubg_python.domain.telemetry.events import LogParachuteLanding
import seaborn as sns
import threading
import math
import logging

logger = logging.getLogger('pubg-agg')
logger.setLevel(logging.DEBUG)
api_key = os.environ.get('API_KEY') 
api = PUBG(api_key=api_key, shard=Shard.PC_NA)
COLUMNS = ['match_id', 'map_name', 'game_mode', 'telemetry_url', "landing_x", "landing_y", "landing_zone", "player", "match_start"]
df = pd.DataFrame(columns=COLUMNS)
quant = 100

def main(date=None):
    #print('try api samples')
    ret = api.samples()
    
    #print(ret)
    params = {'filter[createdAt-start]': f"{date}T12:00:00Z"} if date is not None else None
    #print(params)
    samples = ret.get(params=params)
    i = 0
    thread_pool = []   
    print(f"sample matches returned: {len(samples.matches)}")
    while i < len(samples.matches):
        quant = math.ceil(len(samples.matches)/8)
        print(quant)
        sample_slice = samples.matches[i:i+quant]
        print(len(sample_slice))
        t = threading.Thread(target=batch_get_tels, args=(sample_slice, df))
        thread_pool.append(t)
        t.start()
        i += quant
    for t in thread_pool:
        t.join()
    print(df.shape)
    print('try to write parquet')
    os.makedirs(f"data/parquet/", mode=0o777, exist_ok=True )
    if date is None:
        date = datetime.datetime.now().strftime("%Y-%m-%d")
    df.to_parquet(f'data/parquet/telemetry_{date}.parquet') 
    print(f"Matches analyzed: {len(samples.matches)}")
    print(df.groupby('map_name').match_id.nunique())
    
def get_the_coordinates(match_telemetry):
    filtered = list(filter(lambda x: isinstance(x, LogParachuteLanding), match_telemetry.events))
    #return list(map(lambda c: {'player': c.character.account_id, "x":c.character.location.x, "y":c.character.location.y}, filtered))
    return list(map(lambda c: {"x":c.character.location.x, "y":c.character.location.y, 'zone':c.character.zone, 'player': c.character.name}, filtered))
    
def batch_get_tels(matches, root_df):
    global df
    print('inside batch_get_tels')
    try:
        for match_id in matches:
            print(f'match: {match_id}')
            match_info = api.matches().get(match_id)
            telemetry_url = match_info.assets[0].url
            map_coords = get_the_coordinates(api.telemetry(telemetry_url))
            print(f"{{id:{match_info.id}, map_name:{match_info.map_name}, 'game_mode': {match_info.game_mode} }}")
            print(len(map_coords))
            for coords in map_coords:
            #telemetry = {'match_id': match_info.id, 'map_name': match_info.map_name, 'game_mode': match_info.game_mode, 'telemetry_url': match_info.assets[0].url, "landings": map_coords}
                logging.info(coords)
                if len(coords['zone']) >= 1:
                    telemetry = {'match_id': match_info.id, 'match_start':match_info.created_at, 'map_name': match_info.map_name, 'game_mode': match_info.game_mode, 'telemetry_url': match_info.assets[0].url, "landing_x": coords['x'], "landing_y": coords['y'], "landing_zone": coords['zone'][0], "player": coords["player"]}            
                else:
                    logging.info('no zone')
                    telemetry = {'match_id': match_info.id, 'match_start': match_info.created_at, 'map_name': match_info.map_name, 'game_mode': match_info.game_mode, 'telemetry_url': match_info.assets[0].url, "landing_x": coords['x'], "landing_y": coords['y'], "landing_zone": np.nan, "player": coords["player"]}            
                logging.info(f"telemetry: {telemetry}")                
                _df = pd.DataFrame(telemetry, columns=COLUMNS, index=['match_id'])
                df = df.append(_df)
    except Exception as e:
        print("an exception occurred in thread", e)
        logger.exception("an exception occurred in thread", exc_info=True)

if __name__ == "__main__":
    formatted_date = None
    if len(sys.argv) > 1:
        input_date = sys.argv[1]
        formatted_date = f"{input_date}"
        
    main(formatted_date)