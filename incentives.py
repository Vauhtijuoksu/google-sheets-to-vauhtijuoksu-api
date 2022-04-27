from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

import configparser
import pytz

import lib.gsheets as gs
import lib.vjapi as vj

if __name__ == "__main__":
    configReader = configparser.ConfigParser()
    configReader.read('config.ini')

    config = configReader['INCENTIVES']
    if not config:
        print('Please give config')
        quit()

    gsheets = gs.Gsheets(config['SECRET_JSON_FILE_PATH'])
    incentiveSheet = gsheets.fetchfromsheets(config['SHEET_NAME'])
    print('Sheet fetched')

    vjapi = vj.VauhtijuoksuApi(config['VAUHTIJUOKSU_API_URL'], config['BASIC_AUTH_USER'], config['BASIC_AUTH_PW'])
    incentivesAll = vjapi.getIncentivesAll()
    print('Incentives from vauhtijuoksu-api fetched')


    # delete old incentives
    ids_old = []
    for incentive in incentivesAll:
        ids_old.append(incentive['id'])

    with ThreadPoolExecutor(max_workers=50) as pool:
        pool.map(vjapi.deleteIncentive, ids_old)
    print(f'{len(ids_old)} old incentives deleted')

    gamedata = vjapi.getGamedataAll()

    # add new incentives
    new_incentives = []
    for incentive in incentiveSheet:

        for game in gamedata:
            if game['game'] == incentive['game']:
                incentive['game_id'] = game['id']
                continue
        incentive.pop('game')

        incentive['end_time'] = pytz.timezone('Europe/Helsinki').localize(datetime.strptime(incentive['end_time'], '%d/%m/%Y %H:%M:%S')).isoformat()

        incentiveType = incentive['type']
        if incentiveType == 'open':
            incentive['open_char_limit'] = incentive['parameters']
        elif incentiveType == 'milestone':
            if isinstance(incentive['parameters'], int):
                pass
                incentive['milestones'] = [incentive['parameters']]
            else:
                parameters_list = incentive['parameters'].split('/')
                incentive['milestones'] = parameters_list
        elif incentiveType == 'option':
            parameters_list = incentive['parameters'].split('/')
            incentive['option_parameters'] = parameters_list

        incentive.pop('parameters')

        new_incentives.append(incentive)

    with ThreadPoolExecutor(max_workers=50) as pool:
        pool.map(vjapi.postIncentive, new_incentives)
    print(f'{len(new_incentives)} new incentives added')

