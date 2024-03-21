import os, sys, math, time, pathlib, json, random, string, socketio, threading

global authCode
authCode = os.environ.get('API_KEY')

source = pathlib.Path(__file__).parent.absolute()
collection = str(source) + '/collection'
sys.path.insert(0, str(source) + '/modules')

validChars = 'qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM-_1234567890'
validLetters = 'qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM'

# Data Management

def randomHash(length):
    pool = validLetters + string.digits
    return ''.join(random.choice(pool) for i in range(length))

# Databases

def createDatabase(databaseName):
    for letter in databaseName:
        if not letter in validChars:
            return 'Invalid Database Name'
    try:
        with open(str(collection) + '/' + databaseName + '.json', 'x') as creationFile:
            with open(str(collection) + '/' + databaseName + '.json', 'w') as creationWrite:
                creationWrite.writelines('{}')
                creationWrite.close()
                creationFile.close()
                return 'Database Created'
    except Exception:
        return 'Database Name In Use'
    
def deleteDatabase(databaseName):
    for letter in databaseName:
        if not letter in validChars:
            return 'Invalid Database Name'
    try:
        if databaseName == 'dataStream':
            return 'Unable To Delete Database'
        if os.path.exists(str(collection) + '/' + str(databaseName) + '.json'):
            os.remove(str(collection) + '/' + str(databaseName) + '.json')
            return 'Deleted Database'
        else:
            return 'Database Does Not Exist'
    except Exception as e:
        print(e)
        return 'Failed To Delete Database'
    
# Tables
    
def createTable(databaseName, tableName):
    for letter in tableName:
        if not letter in validChars:
            return 'Invalid Table Name'
    try:
        hash = randomHash(35)
        with open(str(collection) + '/' + databaseName + '.json', 'r') as databaseFileRead:
            databaseData = json.load(databaseFileRead)
            try:
                if databaseData[tableName]:
                    databaseFileRead.close()
                    return 'Table Name Taken'
            except Exception:
                None
            databaseData[tableName] = {'tableId': hash, 'tableData': {}}
            with open(str(collection) + '/' + databaseName + '.json', 'w') as databaseFileWrite:
                databaseFileWrite.write(json.dumps(databaseData))
                databaseFileWrite.close()
                databaseFileRead.close()
                return 'Table Created'
    except Exception:
        return 'Failed To Create Table'
    
def deleteTable(databaseName, tableName):
    for letter in databaseName:
        if not letter in validChars:
            return 'Invalid Database Name'
    for letter in tableName:
        if not letter in validChars:
            return 'Invalid Table Name'
    try:
        with open(str(collection) + '/' + databaseName + '.json', 'r') as databaseFileRead:
            databaseData = json.load(databaseFileRead)
            databaseData.pop(tableName)
            with open(str(collection) + '/' + databaseName + '.json', 'w') as databaseFileWrite:
                databaseFileWrite.write(json.dumps(databaseData))
                databaseFileWrite.close()
                databaseFileRead.close()
                return 'Table Deleted'
    except Exception:
        return 'Failed To Delete Table'
    
# Elements
    
def createElement(databaseName, tableName, elementName, elementData):
    for letter in databaseName:
        if not letter in validChars:
            return 'Invalid Database Name'
    for letter in tableName:
        if not letter in validChars:
            return 'Invalid Table Name'
    try:
        hash = randomHash(25)
        elementDataType = str(type(elementData))
        if elementDataType == "<class 'str'>" or elementDataType == "<class 'int'>" or elementDataType == "<class 'dict'>":
            with open(str(collection) + '/' + databaseName + '.json', 'r') as databaseFileRead:
                databaseData = json.load(databaseFileRead)
                try:
                    if databaseData[tableName]['tableData'][elementName]:
                        databaseFileRead.close()
                        return 'Element Name Taken'
                except Exception:
                    None
                databaseData[tableName]['tableData'][elementName] = {'elementName': elementName,'elementId': hash, 'elementData': elementData}
                with open(str(collection) + '/' + databaseName + '.json', 'w') as databaseFileWrite:
                    databaseFileWrite.write(json.dumps(databaseData))
                    databaseFileWrite.close()
                    databaseFileRead.close()
                    return 'Elemnent Created'
        else: 
            return 'Invalid Data Type'
    except Exception:
        return 'Failed To Create Element'
    
def deleteElement(databaseName, tableName, elementName):
    for letter in databaseName:
        if not letter in validChars:
            return 'Invalid Database Name'
    for letter in tableName:
        if not letter in validChars:
            return 'Invalid Table Name'
    try:
        with open(str(collection) + '/' + databaseName + '.json', 'r') as databaseFileRead:
            databaseData = json.load(databaseFileRead)
            try:
                if databaseData[tableName]['tableData'][elementName]:
                    None
            except Exception:
                databaseFileRead.close()
                return 'Element Not Found'
            databaseData[tableName]['tableData'].pop(elementName)
            with open(str(collection) + '/' + databaseName + '.json', 'w') as databaseFileWrite:
                databaseFileWrite.write(json.dumps(databaseData))
                databaseFileWrite.close()
                databaseFileRead.close()
                return 'Elemnent Deleted'
    except Exception:
        return 'Failed To Delete Element'
    
def editElement(databaseName, tableName, elementName, elementData):
    for letter in databaseName:
        if not letter in validChars:
            return 'Invalid Database Name'
    for letter in tableName:
        if not letter in validChars:
            return 'Invalid Table Name'
    try:
        elementDataType = str(type(elementData))
        if elementDataType == "<class 'str'>" or elementDataType == "<class 'int'>" or elementDataType == "<class 'dict'>":
            with open(str(collection) + '/' + databaseName + '.json', 'r') as databaseFileRead:
                databaseData = json.load(databaseFileRead)
                try:
                    if not databaseData[tableName][elementName]:
                        databaseFileRead.close()
                        return 'Element Not Found'
                except Exception:
                    None
                databaseData[tableName]['tableData'][elementName]['elementData'] = elementData
                with open(str(collection) + '/' + databaseName + '.json', 'w') as databaseFileWrite:
                    databaseFileWrite.write(json.dumps(databaseData))
                    databaseFileWrite.close()
                    databaseFileRead.close()
                    return 'Elemnent Edited'
        else: 
            return 'Invalid Data Type'
    except Exception:
        return 'Failed To Create Element'
    
def getElement(databaseName, tableName, elementName):
    for letter in databaseName:
        if not letter in validChars:
            return 'Invalid Database Name'
    for letter in tableName:
        if not letter in validChars:
            return 'Invalid Table Name'
    try:
        with open(str(collection) + '/' + databaseName + '.json', 'r') as databaseFileRead:
            databaseData = json.load(databaseFileRead)
            try:
                if databaseData[tableName]['tableData'][elementName]:
                    None
            except Exception:
                databaseFileRead.close()
                return 'Element Not Found'
            databaseFileRead.close()
            return databaseData[tableName]['tableData'][elementName]
    except Exception:
        return 'Failed To Delete Element'

def getAllElements(databaseName, tableName):
    for letter in databaseName:
        if not letter in validChars:
            return 'Invalid Database Name'
    for letter in tableName:
        if not letter in validChars:
            return 'Invalid Table Name'
    try:
        with open(str(collection) + '/' + databaseName + '.json', 'r') as databaseFileRead:
            databaseData = json.load(databaseFileRead)
            try:
                if databaseData[tableName]:
                    None
            except Exception:
                databaseFileRead.close()
                return 'Table Not Found'
            databaseFileRead.close()
            elementsList = []
            for item in databaseData[tableName]['tableData']:
                elementsList.append(databaseData[tableName]['tableData'][item])
            return elementsList
    except Exception:
        return 'Failed To Get Elements'

def getAllElementsThat(databaseName, tableName, identifier):
    for letter in databaseName:
        if not letter in validChars:
            return 'Invalid Database Name'
    for letter in tableName:
        if not letter in validChars:
            return 'Invalid Table Name'
    try:
        with open(str(collection) + '/' + databaseName + '.json', 'r') as databaseFileRead:
            databaseData = json.load(databaseFileRead)
            try:
                if databaseData[tableName]:
                    None
            except Exception:
                databaseFileRead.close()
                return 'Table Not Found'
            databaseFileRead.close()
            elementsList = []
            for item in databaseData[tableName]['tableData']:
                if identifier in databaseData[tableName]['tableData'][item]['elementName']:
                    elementsList.append(databaseData[tableName]['tableData'][item])
            return elementsList
    except Exception:
        return 'Failed To Get Elements'

# Execution

def executeData(data):
    global authCode
    functionType = data['function']
    apiKey = data['apiKey']
    functionData = data['functionData']

    if apiKey == authCode:
        print(functionType, functionData)
        if functionType == 'CREATE':
            if functionData['createType'] == 'DB':
                return createDatabase(functionData['DB'])
            elif functionData['createType'] == 'T':
                return createTable(functionData['DB'], functionData['T'])
            elif functionData['createType'] == 'E':
                return createElement(functionData['DB'], functionData['T'], functionData['E'], functionData['D'])
        elif functionType == 'DELETE':
            if functionData['deleteType'] == 'DB':
                return deleteDatabase(functionData['DB'])
            elif functionData['deleteType'] == 'T':
                return deleteTable(functionData['DB'], functionData['T'])
            elif functionData['deleteType'] == 'E':
                return deleteElement(functionData['DB'], functionData['T'], functionData['E'])
        elif functionType == 'EDIT':
            if functionData['editType'] == 'E':
                return editElement(functionData['DB'], functionData['T'], functionData['E'], functionData['D'])
        elif functionType == 'GET':
            if functionData['getType'] == 'E':
                return getElement(functionData['DB'], functionData['T'], functionData['E'])
            elif functionData['getType'] == 'EAll':
                return getAllElements(functionData['DB'], functionData['T'])
            elif functionData['getType'] == 'EQuery':
                return getAllElementsThat(functionData['DB'], functionData['T'], functionData['Q'])

def keepAliveThread():
    while True:
        None

class returningThread(threading.Thread):
  def __init__(self, *args, **kwargs):
      super().__init__(*args, **kwargs)
      self.result = None

  def run(self):
      if self._target is None:
          return
      try:
          self.result = self._target(*self._args, **self._kwargs)
      except Exception as exc:
          print(f'{type(exc).__name__}: {exc}', file=sys.stderr) 

  def join(self, *args, **kwargs):
      super().join(*args, **kwargs)
      return self.result

def startUp():
    sio = socketio.Client(handle_sigint=True,reconnection=True)

    @sio.event
    def connect():
        print('Connection Established')

    @sio.event
    def disconnect():
        print('Client Dissconnected')

    @sio.on('dataRequest')
    def dataRequest(data):
        returner = executeData(json.loads(data))
        sio.emit('sendingBack', returner)

    thread = returningThread(target=keepAliveThread, args=())
    thread.start()

    sio.connect('https://cc3f3bc3-eb68-4b95-bc77-0d6a691f3a5f-00-12prjays5uigr.worf.replit.dev/', namespaces=['/'], transports=['websocket'])

if __name__ == '__main__':
    startUp()