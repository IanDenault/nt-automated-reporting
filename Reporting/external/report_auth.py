import requests, httplib2,os,pickle, json
from urllib import parse


class auth():
    def __init__(self, conf_path):
        #read config json file 
        self.conf = self.read_config_file(f'{conf_path}/conf.json')
        self.google_path = f'{conf_path}/Drive_Creds.json'
        self.pickle_path = f'{conf_path}/token.pickle'

    def read_config_file(self,path):
        with open(path) as file:
            return json.load(file)

    def getSession(self, provider):
        if provider == 'openai':
            return self.conf["API"]["OPENAI"]["API_TOKEN"]
        if provider == 'salesforce':
            return self.salesforce()
        elif provider == 'beeswax':
            return self.beeswax()
        else:
            return None

    def beeswax(self):
        api_secrets = self.conf["API"]["BEESWAX"]

        beeswax_session = requests.Session()
        data = {"email": api_secrets['API_USER'],"password": api_secrets['API_USER_PASSWORD'],"account_id" : "2","keep_logged_in" : "True"}
        response = beeswax_session.request("POST", f"{api_secrets['API_BASE_URL']}v2/authenticate", data=data)
        _ = requests.utils.dict_from_cookiejar(response.cookies)

        return beeswax_session

    def salesforce(self):
        api_secrets = self.conf["API"]["SALES_FORCE"]

        # salesforce auth
        url = f"{api_secrets['API_BASE_URL']}/services/oauth2/token"
        body = ({"grant_type":"password",
                "client_id": api_secrets['API_CLIENT_ID'],
                "client_secret": api_secrets['API_CLIENT_SECRET'],
                "username": api_secrets['API_USER'],
                "password": api_secrets['API_USER_PASSWORD']})

        sf_session = httplib2.Http()

        _ , data = sf_session.request(url, "POST",
                                    headers={'Content-Type': 'application/x-www-form-urlencoded'},
                                    body=parse.urlencode(body))

        splt = data.decode("utf-8").split('"')
        token = splt[3]
        return token

    def authenticate(self,perams):
        sessions = []
        for service in perams:
            service['session'] = self.getSession(service['source'])
            print(service)
            sessions.append(service)
        return sessions
