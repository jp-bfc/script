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
import numpy as np
import datetime
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

query = """
select obj.F01 as 'UPC', obj.F155 as 'Brand', obj.F29 as 'Desc', rpc.F1024 as 'Dept', prc.F30 as 'Reg Price', cs.F1140 as 'Unit Cost'
from
(select * from STORESQL.dbo.OBJ_TAB) obj
inner join (select * from STORESQL.dbo.PRICE_TAB) prc on obj.F01 = prc.F01
left join (select * from STORESQL.dbo.COST_TAB) cs on obj.F01 = cs.F01
inner join (select * from STORESQL.dbo.RPC_TAB) rpc on obj.F18 = rpc.F18
where obj.F18 not in (14, 21, 23, 97, 98, 99) 
"""

report = pd.read_sql(query, cnxn)
report.fillna('', inplace=True)


SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
quick_folder_id = config['DEFAULT']['quick_folder']
sheet_creds = service_account.Credentials.from_service_account_file(
                    'sheet_credentials.json', scopes=SCOPES)
drive_creds = service_account.Credentials.from_service_account_file(
    'drive_credentials.json', scopes=SCOPES)


#https://googleapis.github.io/google-api-python-client/docs/dyn/drive_v3.html
drive = build('drive', 'v3', credentials=drive_creds)
#https://googleapis.github.io/google-api-python-client/docs/dyn/sheets_v4.html
sheets = build('sheets', 'v4', credentials=sheet_creds)

dept_list = report['Dept'].unique().tolist()

output_workbook = drive.files().create(
            body={
            'name' :  "Conventional Items",
            'parents' : ['%s' % quick_folder_id],
            'mimeType' : 'application/vnd.google-apps.spreadsheet'
            },
            fields='id').execute()

workbookId = output_workbook.get('id', '')
report = report.sort_values(['Brand', 'UPC'], ascending=[True, True])


for dept in dept_list:
    sheet_name = dept.split()[0]
    data = report[report['Dept'] == dept]
    data = data.drop(['Dept'], axis=1)
    headers = data.columns.tolist()
    headers.append("Conventional")
    response = sheets.spreadsheets().batchUpdate(
            spreadsheetId = workbookId,
            body = {
                "requests":{
                    "addSheet":{
                                
                            "properties" : {
                                "title": sheet_name,
                                "gridProperties" : {
                                    "columnCount" : 6,
                                    "frozenRowCount" : 1,
                                }
                            },
                        }
                    }
                }).execute()
    sheetId = [x for x in sheets.spreadsheets().get(spreadsheetId=workbookId).execute().get('sheets', '')
                    if x.get('properties', '').get('title', '') == sheet_name][0].get('properties', '').get('sheetId', '')
    result = sheets.spreadsheets().values().append(
        spreadsheetId=workbookId,
        range=sheet_name+"!A1:B1",
        body={ "majorDimension" : "ROWS", "values" : [headers]},
        valueInputOption="RAW"
            ).execute()
    #values
    result = sheets.spreadsheets().values().append(
    spreadsheetId=workbookId,
    range=sheet_name+"!A1:B1",
    body={ "majorDimension" : "ROWS", "values" : data.values.tolist()},
    valueInputOption="RAW"
        ).execute()
    
    response = sheets.spreadsheets().batchUpdate(
        spreadsheetId = workbookId,
        body = { "requests" : [
        
        {"repeatCell" :
    #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/other#GridRange
        {"range" :
            {"sheetId": sheetId,
            "startRowIndex": 1,
            "endRowIndex" : data.shape[0]+1,
            "startColumnIndex": data.columns.get_loc("Reg Price"),
            "endColumnIndex": data.columns.get_loc("Unit Cost")+1},
        #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/cells#CellData
        "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "NUMBER", "pattern" : "0.00"}}},
        "fields" : """userEnteredFormat.numberFormat.type,
                    userEnteredFormat.numberFormat.pattern"""
        }},
        {"repeatCell" :
    #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/other#GridRange
        {"range" :
            {"sheetId": sheetId,
            "startRowIndex": 0,
            "endRowIndex" : 1,
            "startColumnIndex": 0,
            "endColumnIndex": data.shape[1]+2},
        #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/cells#CellData
        "cell": {
            "userEnteredFormat":{
                "borders":{
                    "top": {"style" : "SOLID_THICK"},
                    "bottom":{"style" : "SOLID_THICK"},
                    "left":{"style" : "SOLID_THICK"},
                    "right":{"style" : "SOLID_THICK"}
                    },
                "textFormat": { "fontFamily" : "Arial", "fontSize" : 14, "bold" : True },
                #https://rgbacolorpicker.com/
                "backgroundColor" : {"red":39/255, "green":175/255, "blue":245/255, "alpha": 0.35}
                },
            },
            #https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#google.protobuf.FieldMask
            #https://cloud.google.com/blog/products/application-development/formatting-cells-with-the-google-sheets-api
            "fields" : """userEnteredFormat.textFormat.bold,
                        userEnteredFormat.backgroundColor.red,
                        userEnteredFormat.backgroundColor.green,
                        userEnteredFormat.backgroundColor.blue,
                        userEnteredFormat.backgroundColor.alpha,
                        userEnteredFormat.textFormat.fontFamily,
                        userEnteredFormat.textFormat.fontSize,
                        userEnteredFormat.borders.top,
                        userEnteredFormat.borders.bottom,
                        userEnteredFormat.borders.left,
                        userEnteredFormat.borders.right"""
        }
        },
        {"repeatCell" :
    #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/other#GridRange
        {"range" :
            {"sheetId": sheetId,
            "startRowIndex": 1,
            "endRowIndex" : data.shape[0]+1,
            "startColumnIndex": 0,
            "endColumnIndex": data.shape[1]+2},
        #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/cells#CellData
        "cell": {
            "userEnteredFormat":{
                "borders":{
                    "top": {"style" : "SOLID"},
                    "bottom":{"style" : "SOLID"},
                    "left":{"style" : "SOLID"},
                    "right":{"style" : "SOLID"}
                    },
                "textFormat": { "fontFamily" : "Arial", "fontSize" : 12,}
                },
            },
            #https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#google.protobuf.FieldMask
            #https://cloud.google.com/blog/products/application-development/formatting-cells-with-the-google-sheets-api
            "fields" : """userEnteredFormat.textFormat.bold,
                        userEnteredFormat.textFormat.fontFamily,
                        userEnteredFormat.textFormat.fontSize,
                        userEnteredFormat.borders.top,
                        userEnteredFormat.borders.bottom,
                        userEnteredFormat.borders.left,
                        userEnteredFormat.borders.right"""
        }},
        {
        "autoResizeDimensions":{
            "dimensions" : {
                "sheetId" : sheetId,
                "dimension" : "COLUMNS",
                "startIndex" : 0,
                "endIndex" : data.shape[0]+2
                }
            }
        }
        ]}).execute()
    time.sleep(7)

response = sheets.spreadsheets().batchUpdate(spreadsheetId = workbookId, body = {"requests":{"deleteSheet":{"sheetId" : 0}}}).execute()
