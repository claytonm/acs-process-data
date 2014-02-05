import MySQLdb
conn = MySQLdb.connect(host="localhost", user="root", passwd="password", db="acs_test")
c = conn.cursor()
c.execute("SET sql_mode = ''")
varDict = 'varTableDict.txt'
seqTXT_E = 'seqTXTDict_E.txt'
seqTXT_M = 'seqTXTDict_M.txt'
pathTXT = "Massachusetts_All_Geographies_Not_Tracts_Block_Groups/"

def varTableDict(filename):
    """
    Returns a dictionary mapping variables to sequence tables for both
    estimate (_E) and margin of error (_M) tables.
    """
    # Reads ACS variableName -> seqTable list and creates dictionary
    inputFile = open(filename)
    subjectDict_E = {}
    subjectDict_M = {}
    for line in inputFile:
        lst = line.split(',')
        subjectDict_E[lst[0]] = lst[2]
        subjectDict_M[lst[0]] = lst[3].strip('\n')
    return subjectDict_E, subjectDict_M

vDict_E, vDict_M = varTableDict(varDict)

def seqTXTDict(filename1, filename2):
    """
    Returns a dictionary mapping SeqTable names to e20105mi style names.
    """
    inputFile1 = open(filename1)
    inputFile2 = open(filename2)
    seqTXTDict_E = {}
    seqTXTDict_M = {}
    for line in inputFile1:
        lst = line.split(',')
        seqTXTDict_E[lst[0]] = lst[1].strip('\n')
    for line in inputFile2:
        lst = line.split(',')
        seqTXTDict_M[lst[0]] = lst[1].strip('\n')
    return seqTXTDict_E, seqTXTDict_M

seqTXTDict_E, seqTXTDict_M = seqTXTDict(seqTXT_E, seqTXT_M)

def tableVarList(filename):
    """
    Returns a list of (tableName, variableName) pairs.
    Length is number of variables.
    """
    inputFile = open(filename)
    subjectList_E = []
    subjectList_M = []
    for line in inputFile:
        lst = line.split(',')
        subjectList_E.append((lst[2], lst[0]))
        subjectList_M.append((lst[3].strip('\n'), lst[0]))
    return subjectList_E, subjectList_M

subjectList_E, subjectList_M = tableVarList(varDict)


def tableListUnique(subjectList):
    """
    Accepts a list of (tableName, variableName) pairs.
    Returns a list of unique tableNames.
    """
    tableList = []
    for i in range(0, len(subjectList)-1):
        if not subjectList[i][0] in tableList:
            tableList.append(subjectList[i][0])
    return tableList

tableList_E = tableListUnique(subjectList_E)
tableList_M = tableListUnique(subjectList_M)


def tableVarList(tableName, subjectList):
    """
    Accepts a table name and a list of (tableName, subject) pairs.
    Returns a comma separated string of subjects and DOUBLE. e.g.
    "var1 DOUBLE, var2 DOUBLE, " etc. String ends with ", "
    """
    varStr = ""
    for i in range(0, len(subjectList)-1):
        if subjectList[i][0]==tableName:
            varStr = varStr + subjectList[i][1] + " DOUBLE, "
    return varStr

def tableVarListDict(tableList, subjectList):
    """
    Accepts a list of unique table names and a list of
    (tableName, variableName) pairs. Returns a dictionary
    tableNames -> comma separated string of variableNames
    in the table. This will be used to auto-generate CREATE
    TABLE statements.
    """
    tableVarListDict = {}
    for table in tableList:
        tableVarListDict[table] = tableVarList(table, subjectList)
    return tableVarListDict

tableVarListDict_E = tableVarListDict(tableList_E, subjectList_E)
tableVarListDict_M = tableVarListDict(tableList_M, subjectList_M)

def createTableACS(tableVarListDict):
    """
    Accepts tableName -> comma-separated varList string dictionary,
    and executes a CREATE TABLE SQL query.
    """
    for table in tableVarListDict.keys():
        varList = tableVarListDict[table]
        sql = ("CREATE TABLE " + table + \
               " (FILEID varchar(20), FILETYPE varchar(20), STUSAB varchar(20), CHARITER varchar(20), SEQUENCE varchar(20), LOGRECNO varchar(60), " \
               + varList + " PRIMARY KEY(LOGRECNO))")
        c.execute(sql)

createTableACS(tableVarListDict_E)
createTableACS(tableVarListDict_M)

def fillTablesACS(seqTXTDict_E, seqTXTDict_M):
    """
    Populates ACS tables with data.
    """
    for table in seqTXTDict_E.keys():
        tableTXT = seqTXTDict_E[table] + ".txt"
        sqlE = "LOAD DATA INFILE " + "'" + \
               pathTXT + tableTXT + \
               "'" + " INTO TABLE " + table + " FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'"
        c.execute(sqlE)
    for table in seqTXTDict_M.keys():
        tableTXT_M = seqTXTDict_M[table] + ".txt"
        sqlM = "LOAD DATA INFILE " + "'" + \
               pathTXT + tableTXT_M + \
               "'" + " INTO TABLE " + table + " FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'"
        c.execute(sqlM)

fillTablesACS(seqTXTDict_E, seqTXTDict_M)
conn.commit()       
    

    
    

    

    
    




        
    

















     

