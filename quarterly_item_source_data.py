import openpyxl
import tkinter as tk
from tkinter import filedialog
from pathlib import Path
import datetime
import pyodbc
import pandas as pd
import numpy as np
import configparser

config = configparser.ConfigParser()
config.read('script_configs.ini')
username = config['DEFAULT']['user']
password = config['DEFAULT']['password']
server = config['DEFAULT']['server']
port = config['DEFAULT']['port']
database = config['DEFAULT']['database']
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';PORT='+port+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()

query="""
select obj.F01 as 'UPC', obj.F155 as 'Brand', obj.F29 as 'Desc', obj.F22 as 'Size', rpc.F1024 as 'Dept', sdp.F1022 as 'Sub dept', prc.F30 as 'Price', cos.F1140 as 'Unit Cost'
from STORESQL.dbo.OBJ_TAB obj
left join STORESQL.dbo.POS_TAB pos on OBJ.F01 = pos.F01
inner join STORESQL.dbo.RPC_TAB rpc on obj.F18 = rpc.F18
left join STORESQL.dbo.sdp_tab sdp on pos.F04 = sdp.F04
inner join STORESQL.dbo.PRICE_TAB prc on obj.F01 = prc.F01
left join STORESQL.dbo.COST_TAB cos on OBJ.F01 = cos.f01
"""

df = pd.read_sql(query, cnxn)

for dept in df['Dept'].unique().tolist():
    if(dept != "" or dept != "Apples and Bananas" or dept != "SMS" or dept != "JUNK"):
        file_name = 'Quarterly Inventory - %s Q42021.xlsx' % dept
        file_path = Path('//bfc-hv-01/SWAP/New Reports/%s' % file_name)
        file_path.touch(exist_ok=True)
        writer = pd.ExcelWriter(file_path, engine='openpyxl')
        writing_frame = df[df['Dept']==dept]
        writing_frame.to_excel(writer, sheet_name='Data')
        writer.save()