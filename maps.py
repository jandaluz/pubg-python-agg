import requests as r
import json
import matplotlib.image as mpimg
import io

maps_json = r.get('https://raw.githubusercontent.com/pubg/api-assets/master/dictionaries/telemetry/mapName.json').content.decode('utf-8')
maps = json.loads(maps_json)
maps_reverse = dict(map(reversed, maps.items()))

MAPS = maps_reverse

DIMENSIONS = {
    'Sanhok': (408000, 408000),
    'Erangel': (816000, 816000),
    'Miramar': (816000, 816000),
    'Vikendi': (612000, 612000)
}

IMAGE_URLS = {
    'Sanhok': {'low': 'https://raw.githubusercontent.com/pubg/api-assets/master/Assets/Maps/Sanhok_Main_Low_Res.jpg'},
    'Erangel': {'low': 'https://raw.githubusercontent.com/pubg/api-assets/master/Assets/Maps/Erangel_Main_Low_Res.jpg'},
    'Miramar': {'low': 'https://raw.githubusercontent.com/pubg/api-assets/master/Assets/Maps/Miramar_Main_Low_Res.jpg'},
    'Vikendi': {'low': 'https://raw.githubusercontent.com/pubg/api-assets/master/Assets/Maps/Vikendi_Main_Low_Res.png'},
}

def get_map_plot_image(map_name):
    url = IMAGE_URLS[map_name]['low']
    resp = r.get(url)
    body = resp.content
    img_obj = io.BytesIO(body)
    ext = url[url.rindex('.')+1:]
    return mpimg.imread(img_obj, ext)

