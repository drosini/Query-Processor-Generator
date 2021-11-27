# -*- coding: utf-8 -*-
"""

CS-562 Database Management Systems II

Final Project

Goal: 
    - A program that reads a SQL query
    - Produces another program 
    - Produced program can evaluate the given Query
    
"""
import psycopg2 as p
import pandas as pd
import re


class queryProcessor:

    def __init__(self, use_default_login = False):

        self.name = None
        self.setName()
        
        self.path = r'C:/Users/drosi/Desktop/School/DBMS II/query_{}.py'.format(self.name)
        self.file = open(self.path, 'a')

        # Prompting user to login into PostGres
        login = loginCreds()
        login.promptLogin(use_default_login)
        creds = login.get_creds()

        self.creds = creds
        self.conn = establishConnection(self.creds)

        self.query = "SELECT * FROM sales"
        self.salesTable = queryTable(self.conn, self.query)
        
        self.arguements = None
        
        self.dictPath = None
        

    def setName(self):
        name = input("Please Enter a Unique Name for the Qeury: \n")
        self.name = name
        
        
    def passArguements(self, file = None):

        varClass = queryVariables()

        if file != None:
            varClass.entryFromFile(file)

        else:
            varClass.promptVariableEntry()

        self.arguements = varClass.getVariables()
        
    def buildQuery(self, compareSQL = False):
        
        self.writeUtilities()
        
        self.writeGroupBy()
        
        self.writeFileScan()
        
        self.writeTableMaker()
        
        if compareSQL == True:
            if (self.arguements['query'] == None):
                print('\nNO SQL QUERY PASSED, CANNOT COMPILE COMPARE SQL\n')
            else:
                self.writeSQL()
            
        
        self.file.close()
        
    def writeUtilities(self):
       libraries = ['import psycopg2 as p', '\n'
                    'import pandas as pd', '\n\n']
       self.file.writelines(libraries)
       
       
       login = ["host = '{}'".format(self.creds['host']), '\n',
                "database = '{}'".format(self.creds['database']), '\n',
                "user = '{}'".format(self.creds['user']), '\n',
                "password = '{}'".format(self.creds['password']), '\n\n']
       self.file.writelines(login)
       
       psycopg2 = ["conn = p.connect(host=host, database=database, user=user, password=password)", '\n',
                   "cursor = conn.cursor()", '\n',
                   "cursor.execute('{}')".format(self.query), '\n\n',
                   "columns = []", '\n',
                   "for col in range(len(cursor.description)):", '\n',
                   "     columns.append(cursor.description[col][0])", '\n\n',
                   "rows = cursor.fetchall()", '\n',
                   "cursor.close()", '\n\n',
                   "table = pd.DataFrame(rows, columns = columns)",'\n\n']
       self.file.writelines(psycopg2)
       return
    
    def writeGroupBy(self):

        numGA = len(self.arguements['V'])
        indent = ''
        grouping = []
        
        self.dictPath = ''
        
        
        for i in range(1, numGA + 1):
            grouping.append("g{} = table['{}'].unique()\n".format(i, self.arguements['V'][i -1]))
            
        grouping.append("groupBy = {}\n")
        
        for i in range(1, numGA + 1):
            self.dictPath = self.dictPath + "[{}]".format(self.arguements['V'][i -1])
            
            grouping.append(indent + 'for {} in g{}:\n'.format(self.arguements['V'][i -1], i))
            
            indent = indent + '     '
            
            if i != numGA:    
                grouping.append(indent + "groupBy{} = {}\n".format(self.dictPath, '{}'))
                
            else:
                grouping.append(indent + "groupBy{} = {}".format(self.dictPath, '{'))
                
                for ind in range(len(self.arguements['F'])):
                    grouping.append("'{}': [None, None]".format(self.arguements['F'][ind]))
                    
                    if ind != (len(self.arguements['F']) - 1):
                        grouping.append(", ")
                        
                    else:
                        grouping.append("}\n\n")
        
        self.file.writelines(grouping)
        return
    
    def writeFileScan(self):
        
        scan = []
        indent = ''
        
        scan.append("for index in range(len(table)):\n")
        indent = indent + '     '
        scan.append(indent + "row = table.iloc[index]\n\n")

        for arg in self.arguements['V']:
            scan.append(indent + "{} = row['{}']\n".format(arg, arg))
        
        for i in range(1, self.arguements['n'] + 1):
            
            args = self.getGroupConditions(i)
            scan.append(indent + 'if {}:\n'.format(args))
           
            aggregates = self.getGroupAggregates(indent, i)
            scan.append(aggregates + '\n')
            
            
            
        self.file.writelines(scan)
        return
        
    def writeTableMaker(self):
        
        numGA = len(self.arguements['V'])
        indent= ''
        maker = []
        maker.append('output = []\n')

        for i in range(1, numGA + 1):
 
            maker.append(indent + "for {} in g{}:\n".format(self.arguements['V'][i - 1], i))
            
            indent = indent + '     '
            
            if i == numGA:
                
                var, arg = self.processHavingArguement()
                
                for v in var:
                    maker.append(indent + "_{} = groupBy{}['{}'][0]\n".format(v, self.dictPath, v))
                    
                maker.append('\n' + indent + "if {}:\n\n".format(arg))
                indent += '     '
                maker.append(indent + 'row = []\n')
                
                for proj in self.arguements['S']:
                    if '_' in proj:
                        maker.append(indent + "row.append(groupBy{}['{}'][0])\n".format(self.dictPath, proj))
                    else:
                        maker.append(indent + "row.append({})\n".format(proj))
                        
                maker.append(indent + "output.append(row)\n\n")
         
        maker.append("S = {}\n".format(self.arguements['S']))
        maker.append("print('RESULTS FROM GENERATED QUERY PROGRAM:') \n")
        maker.append("queryResult = pd.DataFrame(output, columns = S)\n")
        maker.append("print(queryResult)\n\n")

        self.file.writelines(maker)
        return
    
    def writeSQL(self):

       compares = ['query = "{}"\n'.format(self.arguements['query']),
                   "cursor = conn.cursor()", '\n',
                   "cursor.execute(query)", '\n\n',
                   "columns = []", '\n',
                   "for col in range(len(cursor.description)):", '\n',
                   "     columns.append(cursor.description[col][0])", '\n\n',
                   "rows = cursor.fetchall()", '\n',
                   "cursor.close()", '\n\n',
                   "sqlResult = pd.DataFrame(rows, columns = columns)",'\n\n',
                   "print('RESULTS FROM EQUIVALENT SQL QUERY:')", '\n',
                   "print(sqlResult)"]
       self.file.writelines(compares)
       
       return
    
    
    def getGroupConditions(self, groupNumber):
        
        operatorDict = {'=': '==',
                '<': '<',
                '>': '>',
                '!': '!='}
        
        conditions = ''
        
        for condition in self.arguements['sig']:
            
            group = int(re.sub('[.]', ' ', condition).split()[0])
            
            if group == groupNumber:
                
                if len(conditions) != 0:
                    conditions += ' and'
                    
                statement = re.sub('[.]', ' ', condition).split()[1]
               
                atribute = re.sub('[=<>!]', ' ', statement).split()[0]
                value = re.sub('[=<>!]', ' ', statement).split()[1]
                operator = re.search('[=<>!]', statement)[0]
                
                conditions += " row['{}'] {} {}".format(atribute, operatorDict[operator], value)
                
        return conditions
    
    
    def getGroupAggregates(self, indent, groupNumber):
        
        indent = indent + '     '
        text = ''
        
        argsDict = {}
        for func in self.arguements['F']:
            
            statement = re.sub('[_]', ' ', func).split()
        
            group = int(statement[0])
            if group == groupNumber:
                
                atribute = statement[2]
                operation = statement[1]
                
                if atribute not in argsDict:
                    argsDict [atribute] = []
                
                argsDict[atribute].append(operation)
                
        atributes = list(argsDict.keys())
        for atribute in atributes:
            
            text += (indent + "{} = row['{}']".format(atribute, atribute) + '\n\n')
            
            for op in argsDict[atribute]:                
                arg = str(groupNumber) + '_' + op + '_' + atribute
                
                text += (indent + "data = groupBy{}['{}']\n".format(self.dictPath, arg))
                text += self.agregateCalculationText(indent, atribute, op)
                text += (indent+ "groupBy{}['{}'] = data".format(self.dictPath, arg) + '\n\n')
        
        return text
    
    def agregateCalculationText(self, indent, atribute, op):
        
        text = ''
        
        if op == 'min':
            text += (indent + "if (data[0] == None):\n")
            text += (indent + '     ' + 'data[0] = {}\n'.format(atribute))
            text += (indent + "elif ({} < data[0]):\n".format(atribute))
            text += (indent + '     ' + 'data[0] = {}\n'.format(atribute))
            return text
        
        if op == 'max':
            text += (indent + "if (data[0] == None):\n")
            text += (indent + '     ' + 'data[0] = {}\n'.format(atribute))
            text += (indent + "elif ({} > data[0]):\n".format(atribute))
            text += (indent + '     ' + 'data[0] = {}\n'.format(atribute))
            return text

        if op == 'count':
            text += (indent + "if (data[0] == None):\n")
            text += (indent + '     ' + 'data[0] = 1\n')
            text += (indent + "else:\n")
            text += (indent + '     ' + 'data[0] += 1\n')
            return text
        
        if op == 'sum':
            text += (indent + "if (data[0] == None):\n")
            text += (indent + '     ' + 'data[0] = {}\n'.format(atribute))
            text += (indent + "else:\n")
            text += (indent + '     ' + 'data[0] += {}\n'.format(atribute))           
            return text
        
        if op == 'avg':
            text += (indent + "if (data[0] == None):\n")
            text += (indent + '     ' + 'data[0] = {}\n'.format(atribute))
            text += (indent + '     ' + 'data[1] = 1\n')
            text += (indent + "else:\n")
            text += (indent + '     ' + 'avg = data[0]\n')
            text += (indent + '     ' + 'count = data[1]\n')
            text += (indent + '     ' + 'sum = avg * count\n')
            text += (indent + '     ' + 'sum += {}\n'.format(atribute))
            text += (indent + '     ' + 'count += 1\n')
            text += (indent + '     ' + 'avg = sum / count\n')
            text += (indent + '     ' + 'data[0] = avg\n')
            text += (indent + '     ' + 'data[1] = count\n')

            return text
        
    def processHavingArguement(self):
        
        g = self.arguements['G']
        var = []
        arg = ''
        
        for val in g:
            
            if '_' in val:
                if val not in var:
                    var.append(val)   
                arg += '_{} '.format(val)
                
            else:
                arg += '{} '.format(val)
        
        
        return var, arg
        


class loginCreds:

    def __init__(self):
        self.host = "localhost"
        self.database = "postgres"
        self.user = "postgres"
        self.password = "2424"

    def promptLogin(self, useDefault=False):

        if useDefault == False:
            host = input("Enter Host Name (DEFAULT - 'localhost'): \n")
            self.host = host
            self.database = input("Enter Database Name (DEFAULT - 'postgres'): \n")
            self.user = input("Enter Username (DEFAULT - 'postgres'): \n")
            self.password = input("Enter Password: \n")

    def get_creds(self):

        creds = {
            "host": self.host,
            "database": self.database,
            "user": self.user,
            "password": self.password
        }

        return creds


class queryVariables:

    def __init__(self):

        self.S = None
        self.n = None
        self.V = None
        self.F = None
        self.sig = None
        self.G = None
        self.query = None

    def promptVariableEntry(self):

        print("Please Enter All Query Variables as Per the Follwoing Prompts.")
        print("Note: For any lists, seperate entries with a <COMMA> and/or <SPACE> \n")
        print("***DO NOT USE SPACES FOR ANYTHING BESIDES SEPERATING ENTRIES***")

        S = input("S --> List of Projected Attributes for Query Output: \n")
        self.S = self.tokenize(S)

        n = input("n --> Number of Grouping Variables: \n")
        self.n = int(n.split()[0])

        V = input("V --> List of Grouping Variables: \n")
        self.V = self.tokenize(V)

        F = input("F --> List of Sets of Aggregate Functions: \n")
        self.F = self.tokenize(F)
        
        sig = input("sigma -> List of Predicate to Define Ranges for Grouping Variables: \n")
        self.sig = self.tokenize(sig)
     
        G = input("G --> Predicate for Having Clasue: \n")
        self.G = self.tokenize(G)
        
        self.printVariables()

    def entryFromFile(self, file):

        f = open(file, 'r').readlines()
        
        self.S = self.tokenize(f[0])

        n = f[1].replace('\n', '')      
        self.n = int(n)
        
        self.V = self.tokenize(f[2])
        self.F = self.tokenize(f[3])
        self.sig = self.tokenize(f[4])
        self.G = self.tokenize(f[5])
        
        if len(f) == 7:
            self.query = f[6].replace('\n', '')
        
        else: 
            self.query = None
        
        self.printVariables()
        
    def tokenize(self, string):
        
        s = string.replace('\n', '')
        s = s.replace(',', '')
        
        tokens = s.split()
        
        return tokens

    def getVariables(self):

        variables = {
            "S": self.S,
            "n": self.n,
            "V": self.V,
            "F": self.F,
            "sig": self.sig,
            "G": self.G,
            "query": self.query
        }

        return variables
    
    def printVariables(self):
        print('S --> {}'.format(self.S))
        print('n --> {}'.format(self.n))
        print('V --> {}'.format(self.V))
        print('F --> {}'.format(self.F))
        print('sig --> {}'.format(self.sig))
        print('G --> {}'.format(self.G))
        print('Query --> {}\n'.format(self.query))
        return

# Function to Establish PostGres Connection
# Pass "loginCreds" Dictionary
def establishConnection(creds):

    conn = p.connect(host=creds['host'],
                     database=creds['database'],
                     user=creds['user'],
                     password=creds['password'])

    return conn

# Returns a Dataframe for a Given Query
# Used to get Tables from PostGres Database
def queryTable(conn, query):

    # Querying PostGres for a Given Table
    cursor = conn.cursor()
    cursor.execute(query)

    # Getting the Column Names from the Cursor Object
    columns = []
    for col in range(len(cursor.description)):
        columns.append(cursor.description[col][0])

    # Getting all the Rows from the Cursor Object for Given Query
    rows = cursor.fetchall()

    # Building DataFrame for the Given Data
    table = pd.DataFrame(rows, columns=columns)

    cursor.close()

    return table




