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
import telemetry_filters as filters
from telemetry_filters import COMMON_COLUMNS, ITEM_COLUMNS, KILL_KNOCK_COLUMNS

logger = logging.getLogger('pubg-agg')
logger.setLevel(logging.DEBUG)
api_key = os.environ.get('API_KEY') 
api = PUBG(api_key=api_key, shard=Shard.PC_NA)
#COLUMNS = ['match_id', 'map_name', 'game_mode', 'telemetry_url', "landing_x", "landing_y", "landing_zone", "player", "match_start"]
df = pd.DataFrame(columns=COMMON_COLUMNS + ITEM_COLUMNS + KILL_KNOCK_COLUMNS)
quant = 100

def main(date=None):
    ret = api.samples()
    
    #query string for samples call
    params = {'filter[createdAt-start]': f"{date}T12:00:00Z"} if date is not None else None

    samples = ret.get(params=params)
    i = 0
    thread_pool = []   
    print(f"sample matches returned: {len(samples.matches)}")
    while i < 2:
        quant = math.ceil(len(samples.matches)/8)
        print(quant)
        sample_slice = samples.matches[i:2]
        print(len(sample_slice))
        #grab a slice
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
    '''create a data dictionary for a player's landing
    
    Arguments:
        match_telemetry {list} -- list of a match's telemetry events
    
    Returns:
        list(dict) -- list of dictionaries containing player landing information
    '''

    #filter the envets for parachute landings
    filtered = list(filter(lambda x: isinstance(x, LogParachuteLanding), match_telemetry.events))
    #prepare the return list with coordinates, zone, and player info
    return list(map(lambda c: {"x":c.character.location.x, "y":c.character.location.y, 'zone':c.character.zone, 'player': c.character.name}, filtered))
    
def batch_get_tels(matches, root_df):
    '''telemetry to dataframe column logic to run in thread
    
    Arguments:
        matches {list} -- list of match information
        root_df {pandas.DataFrame} -- dataframe to append events to
    '''

    global df
    print('inside batch_get_tels')
    for match_id in matches:
        try:
            print(f'match: {match_id}')
            match_info = api.matches().get(match_id)
            telemetry_url = match_info.assets[0].url
            #prepare the coordinates for telemetry
            match_telemetry = api.telemetry(telemetry_url)
            map_coords = filters.filter_parachutes(match_telemetry)
            item_pickups = filters.filter_item_pickup(match_telemetry, match_info.created_at, 'Weapon')
            kills = filters.fitler_kill(match_telemetry, match_info.created_at)
            print(f"{{id:{match_info.id}, map_name:{match_info.map_name}, 'game_mode': {match_info.game_mode} }}")
            print(len(map_coords))
            common_match_info = common_match_info_dict(match_info)
            for coords in map_coords:
                telemetry = {"x": coords['x'],
                             "y": coords['y'],
                             "zone": coords['zone'],
                             "player": coords["player"]}            
                logging.info(f"telemetry: {telemetry}")
                chute_info = {**common_match_info, **telemetry}
                _df = pd.DataFrame(chute_info, columns=COMMON_COLUMNS, index=['match_id'])
                df = df.append(_df)
            for pickups in item_pickups:                
                telemetry = {"x": pickups['x'],
                             "y": pickups['y'], 
                             'player': pickups['player'],
                             'item': pickups['item'],
                             'item_category': pickups['category'],
                             "time_elapsed": pickups['time_elapsed']}
                pickup_info = {**common_match_info, **telemetry}
                _df = pd.DataFrame(pickup_info, columns=COMMON_COLUMNS + ITEM_COLUMNS, index=['match_id'])
                df = df.append(_df)
            for kill in kills:
                telemetry = {"x": kill["x"],
                             "y": kill["y"], 
                             "zone": kill["zone"],
                             "player": kill["player"],
                             "weapon": kill["weapon"],
                             "category": kill["category"],
                             "victim_x": kill["victim_x"],
                             "victim_y": kill["victim_y"],
                             "victim_name": kill["victim_name"],
                             "kill_distance": kill["kill_distance"],
                             "time_elapsed": kill["time_elapsed"],
                             "kill_type": kill["kill_type"]
                }
                kill_info = {**common_match_info, **telemetry}
                _df = pd.DataFrame(kill_info, columns=COMMON_COLUMNS + KILL_KNOCK_COLUMNS, index=['match_id'])
                df = df.append(_df)
        except Exception as e:
            print("an exception occurred in thread", e)
            logger.exception("an exception occurred in thread", exc_info=True)

def common_match_info_dict(match_info):
    return {"match_id": match_info.id,
            "match_start":match_info.created_at,
            "map_name": match_info.map_name,
            "game_mode": match_info.game_mode,
            "telemetry_url": match_info.assets[0].url}

if __name__ == "__main__":
    formatted_date = None
    if len(sys.argv) > 1:
        input_date = sys.argv[1]
        formatted_date = f"{input_date}"
        
    main(formatted_date)