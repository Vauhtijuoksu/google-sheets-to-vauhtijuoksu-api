import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, time
from concurrent.futures import ThreadPoolExecutor

import configparser
import requests
import json

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
    def __init__(self, url, basic_auth_user, basic_auth_pw):
        self.url = url
        self.user = basic_auth_user
        self.pw = basic_auth_pw

    def getGamedataAll(self):
        r = requests.get(f'{self.url}/gamedata')
        return json.loads(r.content)

    def deleteGamedata(self, id):
        r = requests.delete(f'{self.url}/gamedata/{id}', auth=(self.user, self.pw))
        if r.status_code == 204:
            return r.status_code
        else:
            print(r.status_code)

    def postGamedata(self, value):
        r = requests.post(f'{self.url}/gamedata/', json=value, auth=(self.user, self.pw))
        if r.status_code == 201:
            return json.loads(r.content)
        else:
            print(r.status_code)


       
if __name__ == "__main__":
    configReader = configparser.ConfigParser()
    configReader.read('config.ini')

    config = configReader['DEFAULT']
    if not config:
        print('Please give config')
        quit()

    gsheets = Gsheets(config['SECRET_JSON_FILE_PATH'])
    gamedataSheet = gsheets.fetchfromsheets(config['GAMEDATA_SHEET_NAME'])
    print('Sheet fetched')

    vjapi = VauhtijuoksuApi(config['VAUHTIJUOKSU_API_URL'], config['BASIC_AUTH_USER'], config['BASIC_AUTH_PW'])
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

