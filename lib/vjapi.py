import requests
import json

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

    def getIncentivesAll(self):
        r = requests.get(f'{self.url}/incentives')
        return json.loads(r.content)

    def deleteIncentive(self, id):
        r = requests.delete(f'{self.url}/incentives/{id}', auth=(self.user, self.pw))
        if r.status_code == 204:
            return r.status_code
        else:
            print(r.status_code)

    def postIncentive(self, value):
        r = requests.post(f'{self.url}/incentives/', json=value, auth=(self.user, self.pw))
        if r.status_code == 201:
            return json.loads(r.content)
        else:
            print(r.content)