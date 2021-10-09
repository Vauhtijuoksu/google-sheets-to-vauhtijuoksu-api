import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, time
from concurrent.futures import ThreadPoolExecutor

import requests
import json

SECRET_JSON_FILE_PATH=''
VAUHTIJUOKSU_API_URL=''
GAMEDATA_SHEET_NAME=''

class Gsheets:
    def __init__(self, secret_path):
        self.client = self._connect(secret_path)

    def _connect(self, secret_path):
        scope = ['https://spreadsheets.google.com/feeds',
                'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(secret_path, scope)
        client = gspread.authorize(creds)
        return client

    def fetchfromsheets(self, sheetname, worksheetname='', worksheets=[]):
        sheet = self.client.open(sheetname)
        result = sheet.sheet1.get_all_records()

        return result

class VauhtijuoksuApi:
    def __init__(self, url):
        self.url = url

    def getGamedataAll(self):
        r = requests.get(f'{self.url}/gamedata')
        return json.loads(r.content)

    def deleteGamedata(self, id):
        r = requests.delete(f'{self.url}/gamedata/{id}')
        return r.status_code

    def postGamedata(self, value):
        r = requests.post(f'{self.url}/gamedata/', json=value)
        if r.status_code == 200:
            return json.loads(r.content)
        else:
            print(r)


       
if __name__ == "__main__":
    gsheets = Gsheets(SECRET_JSON_FILE_PATH)
    gamedataSheet = gsheets.fetchfromsheets(GAMEDATA_SHEET_NAME)
    print('Sheet fetched')

    vjapi = VauhtijuoksuApi(VAUHTIJUOKSU_API_URL)
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
        gamedata['start_time'] = datetime.strptime(gamedata['start_time'], '%d/%m/%Y %H:%M:%S').isoformat()
        gamedata['end_time'] = datetime.strptime(gamedata['end_time'], '%d/%m/%Y %H:%M:%S').isoformat()
        new_games.append(gamedata)

    with ThreadPoolExecutor(max_workers=50) as pool:
        pool.map(vjapi.postGamedata, new_games)
    print(f'{len(new_games)} new games added')

