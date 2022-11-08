from lib2to3.pytree import convert
import pyodbc
import tkinter as tk
from tkinter import filedialog
import pandas as pd
import numpy as np
import csv
import configparser

class operator:
    operatorName: str
    newItemCount: int
    totalCodes: int
    successRate: float

    def __init__(self, operatorName, newItemCount, totalCodes):
        self.operatorName = operatorName
        self.newItemCount = newItemCount
        self.totalCodes = totalCodes
        self.successRate = totalCodes / newItemCount

root = tk.Tk()
root.withdraw()

#clean up SMS data in Google Sheet called 'New Item Report Source'

file_path = filedialog.askopenfilename()
dataFrame = pd.read_csv(file_path, converters={"UPC" :  str})

noRemoval = dataFrame[(dataFrame['Type'] == 'ADD')]

uniquePairs = list(noRemoval.groupby(['Oper.Name','UPC']).groups)
#print(dataFrame)
uniqueUPC = pd.unique(noRemoval['UPC'].values)
uniqueOperator = pd.unique(noRemoval['Oper.Name'].values)

config = configparser.ConfigParser()
config.read('script_configs.ini')
username = config['DEFAULT']['user']
password = config['DEFAULT']['password']
server = config['DEFAULT']['server']
port = config['DEFAULT']['port']
database = config['DEFAULT']['database']
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';PORT='+port+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()

upcDict = {}

for upc in uniqueUPC:
    para = str((13 - len(str(upc)))*'0'+str(upc))
    #print(para)
    upcDict.update({upc : 'TRUE' == cursor.execute("SELECT CASE WHEN EXISTS (SELECT * FROM STORESQL.dbo.OBJ_TAB OBJ WHERE OBJ.F01 = ? AND OBJ.F180 <> '') THEN 'TRUE' ELSE 'FALSE' END", para).fetchone()[0]})
#print(upcDict)

operatorData = []
k = 0

for u in uniqueOperator:
    newItemCount = sum(1 for elem in uniquePairs if(elem[0] == u))
    totalCodes = 0
    for elem in uniquePairs:
        if(elem[0] == u):
            if(upcDict[elem[1]]):
                totalCodes += 1
    operatorData.append(operator(u, newItemCount, totalCodes))

with open('item-test.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    for p in operatorData:
        writer.writerow([p.operatorName, p.newItemCount, p.totalCodes, p.successRate])
