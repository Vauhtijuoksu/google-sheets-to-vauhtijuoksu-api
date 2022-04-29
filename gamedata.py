from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

import configparser
import pytz

import lib.gsheets as gs
import lib.vjapi as vj

if __name__ == "__main__":
    configReader = configparser.ConfigParser()
    configReader.read('config.ini')

    config = configReader['GAMEDATA']
    if not config:
        print('Please give config')
        quit()

    gsheets = gs.Gsheets(config['SECRET_JSON_FILE_PATH'])
    gamedataSheet = gsheets.fetchfromsheets(config['SHEET_NAME'])
    print('Sheet fetched')

    vjapi = vj.VauhtijuoksuApi(config['VAUHTIJUOKSU_API_URL'], config['BASIC_AUTH_USER'], config['BASIC_AUTH_PW'])
    gamedataAll = vjapi.getGamedataAll()
    print('Games from vauhtijuoksu-api fetched')


    # delete old games
    ids_old = []
    for gamedata in gamedataAll:
        ids_old.append(gamedata['id'])

    with ThreadPoolExecutor(max_workers=50) as pool:
        pool.map(vjapi.deleteGamedata, ids_old)
    print(f'{len(ids_old)} old games deleted')

    # add new games
    new_games = []
    for gamedata in gamedataSheet:
        gamedata['start_time'] = pytz.timezone('Europe/Helsinki').localize(datetime.strptime(gamedata['start_time'], '%d/%m/%Y %H:%M:%S')).isoformat()
        gamedata['end_time'] = pytz.timezone('Europe/Helsinki').localize(datetime.strptime(gamedata['end_time'], '%d/%m/%Y %H:%M:%S')).isoformat()
        new_games.append(gamedata)

    with ThreadPoolExecutor(max_workers=50) as pool:
        pool.map(vjapi.postGamedata, new_games)
    print(f'{len(new_games)} new games added')

    gamedata = vjapi.getGamedataAll()
    vjapi.patchStreamMetadata({'current_game_id': gamedata[0]['id']})

