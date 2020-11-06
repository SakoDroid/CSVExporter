# -*- coding: utf-8 -*-

import csv
import pymysql as db


csv_file = input("Hi! Please give me the name of the csv file (if in the same dir) or the complete address of the file (if in other dirs) : \n")
csv_file = csv_file.replace(".csv","") + ".csv"
print("----------------------------------------------")
print("Now let's configure the database (only for mysql and mysql based databases such as mariadb) : \n")
usr = input("Please give me user name : \n")
pss = input("Ok now the password for " + usr + " : \n")
dbname = input("Nice! Now give me the database name  : \n")
tbname = input("Just 1 more step! Give me the name of the table you want to export data into : \n")
print("Allright! Let's go")
print("----------------------------------------------")
dbs = db.connect(host="localhost",user=usr,password=pss,db=dbname)
csr = dbs.cursor()
csr.execute("SHOW TABLES")
tables = csr.fetchall()

def getCSVHead(filename):
    head = []
    with open(filename,"r") as csv_file:
        ind = 0
        rows = csv.reader(csv_file)
        for row in rows:
            if ind == 0 :
                head = row
            ind += 1
    return head

def getTableHeads(tablename):
    head = []
    csr.execute("DESCRIBE " + tablename)
    titles = csr.fetchall()
    for title in titles :
        head.append(title[0])
    return head

def createTable(tablename,head):
    main_query = "CREATE TABLE IF NOT EXISTS " + tablename + " ("
    ln = len(head)
    ind = 1
    for title in head:
        main_query += title + " text"
        if ind != ln :
            main_query += ", "
        ind += 1
    main_query += ")"
    csr.execute(main_query)
    dbs.commit()
    
def createQuery(heads,tablename):
    query = "INSERT INTO " + tablename + " ("
    ln = len(heads)
    ind = 1
    for hd in heads:
        query += hd
        if ind != ln :
            query += ","
        ind += 1
    query += ") VALUES ("
    return query

def import_csv(filename,heads):
    num = 0
    with open(filename,"r") as csvFile :
        csv_data = csv.DictReader(csvFile)
        for row in csv_data:
            ind = 1
            if num == 0 :
                num += 1
                continue
            query = createQuery(heads,tbname)
            for i in row :
                query += '"' + row[i].replace('"','').replace("'","") + '"'
                if ind != len(row) :
                    query += ','
                ind += 1
            query += ")"
            csr.execute(query)
    dbs.commit()
def tableExists(tableName,tablesTuple):
    temp = False
    for table in tablesTuple:
        if (tableName in table):
            temp = True
    return temp

if (tables == None):
    hd = getCSVHead(csv_file)
    createTable(tbname,hd)
    import_csv(csv_file,hd)
else :
    if (tableExists(tbname,tables)):
        hd = getTableHeads(tbname)
        import_csv(csv_file,hd)
    else :
        hd = getCSVHead(csv_file)
        createTable(tbname,hd)
        import_csv(csv_file,hd)
print("all done!")
dbs.commit()
dbs.close()
