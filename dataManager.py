import os, sys, math, time, pathlib, json, random, string

source = pathlib.Path(__file__).parent.absolute()
collection = str(source) + r'\collection'
sys.path.insert(0, str(source) + '/modules')

validChars = 'qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM-_1234567890'
validLetters = 'qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM'

def startUp():
    print('Database Started')

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
        if pathlib.Path.exists(str(collection) + '/' + databaseName + '.json'):
            os.remove(str(collection) + '/' + databaseName + '.json')
            return 'Deleted Database'
        else:
            return 'Database Does Not Exist'
    except Exception:
        return 'Failed To Delete Database'
    
# Tables
    
def createTable(databaseName, tableName):
    for letter in tableName:
        if not letter in validChars:
            return 'Invalid Table Name'
    #try:
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
    #except Exception as e:
        print(e)
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
                databaseData[tableName]['tableData'][elementName] = {'elementId': hash, 'elementData': elementData}
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

if __name__ == '__main__':
    deleteTable('dataStream', 'hello-world')
    startUp()