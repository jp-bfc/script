from socket import setdefaulttimeout, socket
import pyodbc
import pandas as pd
import os
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
#from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import time
import datetime
import numpy as np
import configparser

setdefaulttimeout(360)

SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
sheet_creds = service_account.Credentials.from_service_account_file(
                    'sheet_credentials.json', scopes=SCOPES)
drive_creds = service_account.Credentials.from_service_account_file(
    'drive_credentials.json', scopes=SCOPES)

#https://googleapis.github.io/google-api-python-client/docs/dyn/drive_v3.html
drive = build('drive', 'v3', credentials=drive_creds)
#https://googleapis.github.io/google-api-python-client/docs/dyn/sheets_v4.html
sheets = build('sheets', 'v4', credentials=sheet_creds)



config = configparser.ConfigParser()
config.read('script_configs.ini')
username = config['DEFAULT']['user']
password = config['DEFAULT']['password']
server = config['DEFAULT']['server']
port = config['DEFAULT']['port']
database = config['DEFAULT']['database']
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';PORT='+port+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()

master_folder_id = config['DEFAULT']['master_item_folder']
workbook_id = config['DEFAULT']['sms_dump_sheet']

query = """
SELECT
OBJ.F01 as 'ITEM UPC',
OBJ.F155 as 'Brand',
OBJ.F29 as 'Description',
OBJ.F22 as 'Size',          
rpc.F1024 as 'Department',
sdp.F1022 as 'Sub dept',
fam.F1040 as 'Local',
cat.F1023 as 'Organic',
pos.F02 as 'POS Desc',
pos.F05 as 'Bottle Link',
btl.F50 as 'Bottle Deposit',
OBJ.F1744 as 'SellSize',
OBJ.F23 as 'UoM',
OBJ.F1957 as 'BASICS',
OBJ.F270 as 'Weight Qty',
pos.F383 as 'Promo Code',
pos.F81 as 'VT Tax',
pos.F96 as 'Meals Tax',
pos.F97 as 'Local',
pos.F98 as 'Org/Local',
pos.F99 as 'FT',
pos.F100 as 'Coop',
pos.F101 as 'Tax',
pos.F123 as 'PLU',
pos.F178 as 'WIC',
OBJ.F180 as 'Full Code',
SUBSTRING(F180, CHARINDEX('V', F180), 1) AS 'V',
SUBSTRING(F180, CHARINDEX('6', F180), 1) AS '6',
SUBSTRING(F180, CHARINDEX('M', F180), 1) AS 'M',
SUBSTRING(F180, CHARINDEX('F', F180), 1) AS 'F',
SUBSTRING(F180, CHARINDEX('C', F180), 1) AS 'C',
SUBSTRING(F180, CHARINDEX('O', F180), 1) AS 'O',
SUBSTRING(F180, CHARINDEX('W', F180), 1) AS 'W',
SUBSTRING(F180, CHARINDEX('B', F180), 1) AS 'B',
SUBSTRING(F180, CHARINDEX('N', F180), 1) AS 'N',
SUBSTRING(F180, CHARINDEX('G', F180), 1) AS 'G',
SUBSTRING(F180, CHARINDEX('A', F180), 1) AS 'A',
SUBSTRING(F180, CHARINDEX('E', F180), 1) AS 'E',
CASE WHEN pos.F82 = 1 THEN 'scaled'
WHEN sdp.F82 = 1 THEN 'scaled'
ELSE ''
END AS 'Scaled?',
cos.F27 as 'Vnd ID',
cos.F26 as 'Vnd Code',		
cos.F19 as 'Units Per Case',
cos.F38 as 'Case Cost',
cos.F1140 as 'Unit Cost',
prc.F30 as 'Price',
prc.F136 as 'Sale Price',
prc.F137 as 'Sale Start',
prc.F138 as 'Sale End'
from STORESQL.dbo.OBJ_TAB OBJ
left join STORESQL.dbo.POS_TAB pos on OBJ.F01 = pos.F01
left join STORESQL.dbo.RPC_TAB rpc on OBJ.F18 = rpc.F18
left join STORESQL.dbo.COST_TAB cos on OBJ.F01 = cos.f01
left join STORESQL.dbo.FAM_TAB fam on OBJ.F16 = fam.F16
left join STORESQL.dbo.CAT_TAB cat on OBJ.F17 = cat.F17
left join STORESQL.dbo.sdp_tab sdp on pos.F04 = sdp.F04
left join STORESQL.dbo.PRICE_TAB prc on OBJ.F01 = prc.F01
left join STORESQL.dbo.BTL_TAB btl on pos.F05 = btl.F05
Order by OBJ.F01
"""

df = pd.read_sql(query, cnxn)

df = df.fillna('')
ts = datetime.datetime.now().strftime("%m/%d/%y %I-%M %p")

df = df.astype({"Sale Start" : str})
df = df.astype({"Sale End" : str})
df['Sale Start'] = df['Sale Start'].apply(lambda x : "" if x == "NaT" else x)
df['Sale End'] = df['Sale End'].apply(lambda x : "" if x == "NaT" else x)

sheet_title = [x for x in sheets.spreadsheets().get(spreadsheetId=workbook_id).execute().get('sheets', '')
                            if x.get('properties', '').get('sheetId', '') == 0][0].get('properties', '').get('title', '')
sheetId = 0
sheets.spreadsheets().values().batchClear(spreadsheetId=workbook_id, body = {'ranges': sheet_title + "!A1:AU"}).execute()


result = sheets.spreadsheets().values().append(
    spreadsheetId=workbook_id,
    range= sheet_title + "!A1:B1",
    body={ "majorDimension" : "ROWS", "values" : [df.columns.tolist()] + df.to_numpy().tolist()},
    valueInputOption="RAW"
        ).execute()

response = sheets.spreadsheets().batchUpdate(
                    spreadsheetId = workbook_id,
                    body = {
                        "requests": [
                            {
                                "updateSheetProperties" : {
                                    "properties" : {
                                        "sheetId" : sheetId,
                                        "title" : "Updated %s" % ts
                                    },
                                    "fields" : "title"
                                }
                            },
                            #FOR FIRST TIME SHEET SET UP, UNNECESSARY FOR UPDATING
                            # {"repeatCell" :
                            # #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/other#GridRange
                            #     {"range" :
                            #         {"sheetId": sheetId,
                            #         "startRowIndex": 0,
                            #         "endRowIndex" : 1,
                            #         "startColumnIndex": 0,
                            #         "endColumnIndex": df.shape[1]+1},
                            #     #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/cells#CellData
                            #     "cell": {
                            #         "userEnteredFormat":{
                            #             "borders":{
                            #                 "top": {"style" : "SOLID_THICK"},
                            #                 "bottom":{"style" : "SOLID_THICK"},
                            #                 "left":{"style" : "SOLID_THICK"},
                            #                 "right":{"style" : "SOLID_THICK"}
                            #                 },
                            #             "textFormat": { "fontFamily" : "Arial", "fontSize" : 14, "bold" : True },
                            #             #https://rgbacolorpicker.com/
                            #             "backgroundColor" : {"red":39/255, "green":175/255, "blue":245/255, "alpha": 0.35},
                            #             "horizontalAlignment" : "CENTER",
                            #             "verticalAlignment" : "MIDDLE"
                            #             },
                            #         },
                            #         #https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#google.protobuf.FieldMask
                            #         #https://cloud.google.com/blog/products/application-development/formatting-cells-with-the-google-sheets-api
                            #         "fields" : """userEnteredFormat.textFormat.bold,
                            #                     userEnteredFormat.backgroundColor.red,
                            #                     userEnteredFormat.backgroundColor.green,
                            #                     userEnteredFormat.backgroundColor.blue,
                            #                     userEnteredFormat.backgroundColor.alpha,
                            #                     userEnteredFormat.textFormat.fontFamily,
                            #                     userEnteredFormat.textFormat.fontSize,
                            #                     userEnteredFormat.borders.top,
                            #                     userEnteredFormat.borders.bottom,
                            #                     userEnteredFormat.borders.left,
                            #                     userEnteredFormat.borders.right,
                            #                     userEnteredFormat.horizontalAlignment,
                            #                     userEnteredFormat.verticalAlignment"""
                            #    }}
                            {
                            'setBasicFilter': {
                                'filter': {
                                    'range': {
                                        "sheetId" : sheetId,
                                        "startRowIndex" : 0,
                                        "endRowIndex" : df.shape[0]+3,
                                        "startColumnIndex" : 0,
                                        "endColumnIndex" : df.shape[1]
                                    }
                                }
                            }},
                            {"repeatCell" :
                            #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/other#GridRange
                                {"range" :
                                    {"sheetId": sheetId,
                                    "startRowIndex": 1,
                                    "endRowIndex" : df.shape[0]+4,
                                    "startColumnIndex": 0,
                                    "endColumnIndex": df.shape[1]},
                                #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/cells#CellData
                                "cell": {
                                    "userEnteredFormat":{
                                        "borders":{
                                            "top": {"style" : "SOLID"},
                                            "bottom":{"style" : "SOLID"},
                                            "left":{"style" : "SOLID"},
                                            "right":{"style" : "SOLID"}
                                            },
                                        "textFormat": { "fontFamily" : "Arial", "fontSize" : 12}
                                        },
                                    },
                                    #https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#google.protobuf.FieldMask
                                    #https://cloud.google.com/blog/products/application-development/formatting-cells-with-the-google-sheets-api
                                    "fields" : """userEnteredFormat.textFormat.fontFamily,
                                                userEnteredFormat.textFormat.fontSize,
                                                userEnteredFormat.borders.top,
                                                userEnteredFormat.borders.bottom,
                                                userEnteredFormat.borders.left,
                                                userEnteredFormat.borders.right"""
                                }},
                                {"repeatCell" :
                                #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/other#GridRange
                                    {"range" :
                                        {"sheetId": sheetId,
                                        "startRowIndex": 1,
                                        "endRowIndex" : df.shape[0]+1,
                                        "startColumnIndex": df.columns.get_loc("Case Cost"),
                                        "endColumnIndex": df.columns.get_loc("Sale Price")+1},
                                    #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/cells#CellData
                                    "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "CURRENCY", "pattern" : "$#,##0.00"}}},
                                                            "fields" : """userEnteredFormat.numberFormat.type,
                                                                            userEnteredFormat.numberFormat.pattern"""
                                    }
                                    },
                                ]
                    }).execute()

