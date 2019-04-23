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
# COLUMNS = ['match_id', 'map_name', 'game_mode', 'telemetry_url', "landing_x", "landing_y", "landing_zone", "player", "match_start"]
df = pd.DataFrame(columns=COMMON_COLUMNS + ITEM_COLUMNS + KILL_KNOCK_COLUMNS)
quant = 100

def main(date=None):
    start_time = datetime.datetime.now()
    print(f"script start time: {start_time}")
    ret = api.samples()
    
    #query string for samples call
    params = {'filter[createdAt-start]': f"{date}T12:00:00Z"} if date is not None else None

    samples = ret.get(params=params)
    i = 0
    thread_pool = []   
    print(f"sample matches returned: {len(samples.matches)}")
    quant = math.ceil(len(samples.matches)/4)
    print(quant)
    while i < len(samples.matches):
        sample_slice = samples.matches[i:i+quant]
        print(len(sample_slice))
        #grab a slice
        t = threading.Thread(target=tels_to_dataframe, args=[sample_slice])
        thread_pool.append(t)
        t.start()
        i += quant
    for t in thread_pool:
        t.join()
        thread_pool.remove(t)

    #tels_to_dataframe(samples.matches)
    print(df.shape)
    print('try to write parquet')
    os.makedirs(f"data/parquet/", mode=0o777, exist_ok=True )
    os.makedirs(f"data/csv/", mode=0o777, exist_ok=True)
    if date is None:
        date = datetime.datetime.now().strftime("%Y-%m-%d")
    df.to_parquet(f'data/parquet/telemetry_{date}.parquet') 
    print('try to write csv')
    df.to_csv(f'data/csv/landings_{date}.csv', header=True)
    print(f"Matches analyzed: {len(samples.matches)}")
    print(df.groupby('map_name').match_id.nunique())
    end_time = datetime.datetime.now()
    print(f"end time: {end_time}")
    print(f"run time: {(end_time - start_time).seconds}")
    return {
        "p": f"data/parquet/telemetry_{date}.parquet",
        "c": f"data/csv/landings_{date}.csv"
    }
    
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

def common_match_info_dict(match_info):
    return {"match_id": match_info.id,
            "match_start":match_info.created_at,
            "map_name": match_info.map_name,
            "game_mode": match_info.game_mode,
            "telemetry_url": match_info.assets[0].url}

def chutes_to_dataframe(chutes_list, common_match_info):
    global df
    print('enter chutes to dataframe')
    for coords in chutes_list:
        try:
            _df = pd.DataFrame(coords, columns=COMMON_COLUMNS, index=['match_id'])
            df = df.append(_df)
        except ValueError:
            print('unable to add info to dataframe')
            print(coords)
    print('exit chutes to dataframe')

def items_to_dataframe(items_list, common_match_info):
    print('enter items to dataframe')
    global df
    for pickups in items_list:                
        try:
            _df = pd.DataFrame(pickups, columns=COMMON_COLUMNS + ITEM_COLUMNS, index=['match_id'])
            df = df.append(_df)
        except ValueError:
            print('unable to add info to dataframe')
            print(pickups)
    print('exit items to dataframe')

def kills_to_dataframe(kills_list, common_match_info):
    global df
    print('enter kills to dataframe')
    for kill in kills_list:
        try:
            _df = pd.DataFrame(kill, columns=COMMON_COLUMNS + KILL_KNOCK_COLUMNS, index=['match_id'])
            df = df.append(_df)
        except ValueError:
            print('unable to add info to dataframe')
            print(kill)     
    print('exit kills to dataframe')

def tels_to_dataframe(matches):
    '''telemetry to dataframe column logic to run in thread
    
    Arguments:
        matches {list} -- list of match information
        root_df {pandas.DataFrame} -- dataframe to append events to
    '''
    start_time = datetime.datetime.now()
    print(f"start: {start_time}")
    print('inside tels_to_dataframe')
    thread_pool = []
    for match_id in matches:
        try:
            print(f'match: {match_id}')
            start_time = datetime.datetime.now()
            match_info = api.matches().get(match_id)
            #skip training range matches
            if(match_info.map_name == "Range_Main"):
                continue
            telemetry_url = match_info.assets[0].url
            # prepare the coordinates for telemetry
            match_telemetry = api.telemetry(telemetry_url)
            common_match_info = common_match_info_dict(match_info)
            map_coords = filters.filter_parachutes(match_telemetry, match_info)
            #item_pickups = filters.filter_item_pickup(match_telemetry, match_info, 'Weapon')
            kills = filters.fitler_kill(match_telemetry, match_info)
            print(f"{{id:{match_info.id}, map_name:{match_info.map_name}, 'game_mode': {match_info.game_mode} }}")
            print(len(map_coords))

            #grab a slice
            #chutes_thread = threading.Thread(target=batch_get_tels, args=(sample_slice, df))
            chutes_thread = threading.Thread(target=chutes_to_dataframe, args=(map_coords, common_match_info))
            chutes_thread.start()
            #items_thread = threading.Thread(target=items_to_dataframe, args=(item_pickups, common_match_info))
            #items_thread.start()
            kills_thread = threading.Thread(target=kills_to_dataframe, args=(kills, common_match_info))
            kills_thread.start()

            chutes_thread.join()
            #items_thread.join()
            kills_thread.join()

        except Exception as e:
            print("an exception occurred in thread", e)
            logger.exception("an exception occurred in thread", exc_info=True)
        for t in thread_pool:
            print(f"join thread {t}")
            t.join()
            thread_pool.remove(t)
        end_time = datetime.datetime.now()
        print(f"end: {end_time}")
        print(f"time taken: {(end_time - start_time).seconds}")

if __name__ == "__main__":
    formatted_date = None
    if len(sys.argv) > 1:
        input_date = sys.argv[1]
        formatted_date = f"{input_date}"
        
    main(formatted_date)