import requests
import json, sys


# portalURL = "https://{}:6443/arcgis/admin/" #generateToken

##TODO check with UNIX about GIS3 credentials

class Token(object):
    '''Class to retrieve token from servers.
        This is for any development that may require going to the individual server'''

    def __init__(self, username, password, url):

        self.params = {'username': username,
                           'password': password,
                           'client': 'requestip',
                           'expiration': 180,
                            'f': 'json'}

        self.headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
        self.url = url

    # def get_Token(self):
        try:
            resp = requests.post(self.url + "generateToken", data=self.params, headers=self.headers, verify=False)
            json_resp = resp.json()


            tok = json_resp
            print(tok)
            self.token_id = tok['token']
            # print(token_id)
        except:

            if self.token_id == "":
                print("Awww. Didn't get any tokens. It's ok. Maybe next time.")
                sys.exit()

    def stop_serv(self, fold):

        self.params = {'token': self.token_id,
                   'f': 'pjson'}
        url = self.url + fold + '/stop'
        try:
            resp = requests.post(url, data=self.params, headers=self.headers, verify=False)
            print(resp.status_code)
        except:
            print('Could not stop/start service')
            sys.exit()
    def start_serv(self, fold):

        self.params = {'token': self.token_id,
                   'f': 'pjson'}
        url = self.url + fold + '/start'
        try:
            resp = requests.post(url, data=self.params, headers=self.headers, verify=False)
            print(resp.status_code)
        except:
            print('Could not stop/start service')
            sys.exit()
#