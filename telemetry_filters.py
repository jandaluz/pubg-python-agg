import os
import sys
import time
import pandas as pd
import numpy as np
import matplotlib.pyplot as plot
import matplotlib.image as image
import datetime
from pubg_python import PUBG, Shard
from pubg_python.domain.telemetry.events import LogParachuteLanding, LogItemPickup, LogPlayerKill, LogPlayerMakeGroggy, LogGameStatePeriodic
import seaborn as sns
import threading
import math
import logging

logger = logging.getLogger(__name__)    
def filter_parachutes(match_telemetry):
    '''create a data dictionary for a player's landing
    
    Arguments:
        match_telemetry {list} -- list of a match's telemetry events
    
    Returns:
        list(dict) -- list of dictionaries containing player landing information
    '''
    logger.debug("filter events for parachute landings")
    #filter the envets for parachute landings
    filtered = list(filter(lambda x: isinstance(x, LogParachuteLanding), match_telemetry.events))
    #prepare the return list with coordinates, zone, and player info
    return list(map(lambda c: {"x":c.character.location.x,
                               "y":c.character.location.y,
                               "zone":c.character.zone if len(c.character.zone) > 0 else None,
                               "player": c.character.name}, filtered))
    
def filter_item_pickup(match_telemetry, start_time, category):
    logger.debug("filter item pickups")
    start_pattern = "%Y-%m-%dT%H:%M:%SZ"
    dt_pattern = "%Y-%m-%dT%H:%M:%S.%fZ"
    #filter item pickups
    logger.debug("filter out parachute backpacks (start of game)")
    filtered = filter(lambda x: (isinstance(x, LogItemPickup) and x.item.item_id != "Item_Back_B_01_StartParachutePack_C" and x.item.category == 'Weapon'), match_telemetry.events)
    #prepare the return list with coordinates and player info
    return list(map(lambda c: {"x":c.character.location.x,
                               "y":c.character.location.y,
                               'zone':c.character.zone[0] if len(c.character.zone) > 0 else None,
                               'player': c.character.name,
                               'item': c.item.item_id,
                               'category': c.item.category,
                               "time_elapsed": calculate_event_time(start_time, c.timestamp)},
                               filtered))

def fitler_kill(match_telemetry, start_time):
    logger.debug('filter kills and knocks')
    #filter kills
    filtered_kills = filter(lambda x: isinstance(x, LogPlayerKill), match_telemetry.events) 
    kills =  list(map(lambda c: {
                "x": c.killer.location.x,
                "y": c.killer.location.y,
                "zone": c.killer.zone[0] if len(c.killer.zone) > 0 else None,
                "player": c.killer.name,
                "weapon": c.damage_causer_name,
                "category": c.damage_type_category,
                "victim_x": c.victim.location.x,
                "victim_y": c.victim.location.y,
                "victim_name": c.victim.name,
                "kill_distance": c.distance,
                "time_elapsed": calculate_event_time(start_time, c.timestamp),
                "kill_type": "KILL"
    },
    filtered_kills))
    filtered_knocks = filter(lambda x: isinstance(x, LogPlayerMakeGroggy), match_telemetry.events)
    #prepare the return list
    knocks = list(map(lambda c: {
        "x": c.attacker.location.x,
        "y": c.attacker.location.y,
        "zone": c.attacker.zone[0] if len(c.attacker.zone) > 0 else None,
        "player": c.attacker.name,
        "weapon": c.damage_causer_name,
        "category": c.damage_type_category,
        "victim_x": c.victim.location.x,
        "victim_y": c.victim.location.y,
        "victim_name": c.victim.name,
        "kill_distance": c.distance,
        "time_elapsed": calculate_event_time(start_time, c.timestamp),
        "kill_type": "KNOCK"
    },
    filtered_knocks))

    return knocks + kills

def calculate_event_time(match_start_timestamp, event_timestamp):
    start_pattern = "%Y-%m-%dT%H:%M:%SZ"
    dt_pattern = "%Y-%m-%dT%H:%M:%S.%fZ"
    event_datetime = datetime.datetime.strptime(event_timestamp, dt_pattern)
    match_start_datetime = datetime.datetime.strptime(match_start_timestamp, start_pattern)

    delta = event_datetime - match_start_datetime
    return delta.seconds

COMMON_COLUMNS = ['match_id', 'map_name', 'game_mode', 'telemetry_url', "match_start", "x", "y", "zone", "player"]
ITEM_COLUMNS = ['item', 'category', 'time_elapsed']
KILL_KNOCK_COLUMNS = ["weapon", "victim_x", "victim_y", "victim_name", "kill_distance", "kill_type"]