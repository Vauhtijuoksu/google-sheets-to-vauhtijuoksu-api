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
    gamedataSheet = gsheets.fetchfromsheets(config['GAMEDATA_SHEET_NAME'])
    PlayersSheet = gsheets.fetchfromsheets(config['PLAYERS_SHEET_NAME'])
    print('Sheets fetched')

    vjapi = vj.VauhtijuoksuApi(config['VAUHTIJUOKSU_API_URL'], config['BASIC_AUTH_USER'], config['BASIC_AUTH_PW'])
    gamedataAll = vjapi.getGamedataAll()
    print('Games from vauhtijuoksu-api fetched')

    playersAll = vjapi.getPlayersAll()
    print('Players from vauhtijuoksu-api fetched')


    # delete old games
    ids_old = []
    for gamedata in gamedataAll:
        ids_old.append(gamedata['id'])

    with ThreadPoolExecutor(max_workers=50) as pool:
        pool.map(vjapi.deleteGamedata, ids_old)
    print(f'{len(ids_old)} old games deleted')

    # delete old players
    ids_old = []
    for player in playersAll:
        ids_old.append(player['id'])

    with ThreadPoolExecutor(max_workers=50) as pool:
        pool.map(vjapi.deletePlayer, ids_old)
    print(f'{len(ids_old)} old players deleted')

    # add new players
    new_players = []
    for player in PlayersSheet:
        new_players.append({
            "display_name": player["display_name"],
            "social_medias": [
                {
                    "platform": "TWITCH",
                    "username": player.get("twitch_channel", "")
                },
                {
                    "platform": "DISCORD",
                    "username": player.get("discord_nick", "")
                }
            ]
        })

    with ThreadPoolExecutor(max_workers=50) as pool:
        pool.map(vjapi.postPlayer, new_players)
    print(f'{len(new_players)} new players added')

    players = vjapi.getPlayersAll()
    # add new games
    new_games = []
    for gamedata in gamedataSheet:
        gamedata['start_time'] = pytz.timezone('Europe/Helsinki').localize(datetime.strptime(gamedata['start_time'], '%Y-%m-%d %H:%M:%S')).isoformat()
        gamedata['end_time'] = pytz.timezone('Europe/Helsinki').localize(datetime.strptime(gamedata['end_time'], '%Y-%m-%d %H:%M:%S')).isoformat()
        players_parsed_from_gamedata = gamedata['players'].split(',')
        gamedata['players'] = []
        for player in players_parsed_from_gamedata:
            player = player.strip()
            player_id = next(item for item in players if item["display_name"] == player)['id']
            gamedata['players'].append(player_id)

        new_games.append(gamedata)

    with ThreadPoolExecutor(max_workers=50) as pool:
        pool.map(vjapi.postGamedata, new_games)
    print(f'{len(new_games)} new games added')

    gamedata = vjapi.getGamedataAll()
    vjapi.patchStreamMetadata({'current_game_id': gamedata[0]['id']})

