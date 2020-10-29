# The following script accesses TDameritrades api for streaming services offered by the brokerage
# It calls on objects from previous scripts to handle authentication and configuration
import datetime
from td_auth import *
import urllib
import dateutil.parser
import json
from urllib import parse
from var import *
import websockets
import asyncio
import pyodbc
import nest_asyncio


TDClient = td_auth(client_id, account_num, password)
TDClient.authenticate()

access_token = TDClient.access_token
print(access_token)


def unix_time_millis(dt):
    # converts date time to miliseconds
    epoch = datetime.datetime.utcfromtimestamp(0)
    return (dt - epoch).total_seconds() * 1000.0


# Define endpoints
endpoint = 'https://api.tdameritrade.com/v1/userprincipals'
headers = {'Authorization': 'Bearer {}'.format(access_token)}
print(headers)
# define parameters
params = {'fields': 'streamerSubscriptionKeys,streamerConnectionInfo'}
# make requests
content = requests.get(url=endpoint, params=params, headers=headers)
print('userprincipals:', content.json())
userprincipalsresponse = content.json()
# grab time stamp
tokenTimeStamp = userprincipalsresponse['streamerInfo']['tokenTimestamp']
date = dateutil.parser.parse(tokenTimeStamp, ignoretz=True)
tokenTimeStampAsMs = unix_time_millis(date)
# define items we need to request login
creds = {'userid': userprincipalsresponse['accounts'][0]['accountId'],
         'token': userprincipalsresponse['streamerInfo']['token'],
         'company': userprincipalsresponse['accounts'][0]['company'],
         'segment': userprincipalsresponse['accounts'][0]['segment'],
         'cddomain': userprincipalsresponse['accounts'][0]['accountCdDomainId'],
         'usergroup': userprincipalsresponse['streamerInfo']['userGroup'],
         'accesslevel': userprincipalsresponse['streamerInfo']['accessLevel'],
         'authorized': 'Y',
         'timestamp': int(tokenTimeStampAsMs),
         'appid': userprincipalsresponse['streamerInfo']['appId'],
         'acl': userprincipalsresponse['streamerInfo']['acl']}
# define login request
login_request = {"requests": [{"service": "ADMIN",
                               "requestid": 0,
                               "command": "LOGIN",
                               "account": userprincipalsresponse['accounts'][0]['accountId'],
                               "source": userprincipalsresponse['streamerInfo']['appId'],
                               "parameters": {"credential": urllib.parse.urlencode(creds),
                                              "token": userprincipalsresponse['streamerInfo']['token'],
                                              "version": "1.0"}}]}

# define a data request
data_request = {"requests": [{"service": "ACTIVES_NASDAQ",
                              "requestid": '1',
                              "command": "SUBS",
                              "account": userprincipalsresponse['accounts'][0]['accountId'],
                              "source": userprincipalsresponse['streamerInfo']['appId'],
                              "parameters": {"keys": "NASDAQ-60",
                                             "fields": "0,1"}},
                             {'service': 'NASDAQ_BOOK',
                              'requestid': '2',
                              'command': 'SUBS',
                              'account': userprincipalsresponse['accounts'][0]['accountId'],
                              'source': userprincipalsresponse['streamerInfo']['appId'],
                              'parameters': {'keys': 'MSFT',
                                             'fields': '0,1,2,3,4'}}]}
# turn request into json string
login_encoded = json.dumps(login_request)
data_encoded = json.dumps(data_request)
print(login_encoded)
print(data_encoded)


class WebSocketClient(object):

    def __init__(self):
        self.cnxn = None
        self.crsr = None
        self.connection = None

    def database_connect(self):
        print('initializing...')
        server = 'DESKTOP-TFAK9M9\SQLEXPRESS'
        database = 'Stock_data'
        sql_driver = '{ODBC Driver 17 for SQL Server}'
        # define our connection to the data base
        self.cnxn = pyodbc.connect(driver=sql_driver,
                                   server=server,
                                   database=database,
                                   trusted_connection='yes')
        self.crsr = self.cnxn.cursor()

    def database_insert(self, query, data_tuple):

        self.crsr.execute(query, data_tuple)
        self.cnxn.commit()
        self.cnxn.close()
        print('data inserted')

    async def connect(self):
        print('connect initiated...')
        uri = 'wss://' + userprincipalsresponse['streamerInfo']['streamerSocketUrl'] + '/ws'
        print(uri)
        # awaited because client must connect before other functions are carried out
        self.connection = await websockets.client.connect(uri)

        if self.connection.open:
            print('Connection established')
            return self.connection

    async def sendMessage(self, message):
        await self.connection.send(message)

    async def recieveMessage(self, connection):
        while True:
            try:
                message = await connection.recv()
                message_decoded = json.loads(message)
                query = 'INSERT INTO Stream_1 (service, timestamp, command) VALUES (?,?,?);'
                self.database_connect()
                if 'data' in message_decoded.keys():
                    data = message_decoded['data'][0]
                    data_tuple = (data['service'], str(data['timestamp']), data['command'])
                    # insert data
                    self.database_insert(query, data_tuple)
                print('-' * 20)
                print('revieved meesage from server' + str(message))
            except websockets.exceptions.ConnectionClosed:
                print('Connection closed')
                break


    async def heartbeat(self, connection):
        while True:
            try:
                await connection.send('ping')
                await asyncio.sleep(5)
            except websockets.exceptions.ConnectionClosed:
                print('Connection with server closed')
                break


async def main():
    # create clinet object
    client = WebSocketClient()
    nest_asyncio.apply()
    # define event loop
    loop = asyncio.get_event_loop()
    # start a connection to websocket
    connection = loop.run_until_complete(client.connect())
    # define desired tasks
    tasks = [asyncio.ensure_future(client.recieveMessage(connection)),
             asyncio.ensure_future(client.sendMessage(login_encoded)),
             asyncio.ensure_future(client.recieveMessage(connection)),
             asyncio.ensure_future(client.sendMessage(data_encoded)),
             asyncio.ensure_future(client.recieveMessage(connection))]
    loop.run_until_complete(asyncio.wait(tasks))

if __name__ == '__main__':

    asyncio.run(main(), debug=True)
