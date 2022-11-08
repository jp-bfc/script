import pyodbc
import pandas as pd
import os
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
#from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
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
select obj.F01, obj.F155 as 'Brand', obj.F29 as 'Description', obj.F22 as 'Size', rpc.F1024 as 'Department', sdp.F1022 as 'Sub dept', prc.F30 as 'Price', cost.F1140 as 'Cost'
from STORESQL.dbo.OBJ_TAB obj
inner join STORESQL.dbo.RPC_TAB rpc on obj.F18 = rpc.F18
inner join STORESQL.dbo.POS_TAB pos on obj.F01 = pos.F01
inner join STORESQL.dbo.SDP_TAB sdp on pos.F04 = sdp.F04
inner join STORESQL.dbo.COST_TAB cost on obj.F01 = cost.F01 
inner join STORESQL.dbo.PRICE_TAB prc on obj.F01 = prc.F01
where obj.F18 in (6,11,16,19)"""

items = pd.read_sql(query, cnxn)

SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
wellness_sales_upc_lookup_workbook_ID = config['DEFAULT']['wellness_update_sheet']
sheet_creds = service_account.Credentials.from_service_account_file(
                    'sheet_credentials.json', scopes=SCOPES)
drive_creds = service_account.Credentials.from_service_account_file(
    'drive_credentials.json', scopes=SCOPES)

#https://googleapis.github.io/google-api-python-client/docs/dyn/drive_v3.html
drive = build('drive', 'v3', credentials=drive_creds)
#https://googleapis.github.io/google-api-python-client/docs/dyn/sheets_v4.html
sheets = build('sheets', 'v4', credentials=sheet_creds)

sheetId = [x for x in sheets.spreadsheets().get(spreadsheetId=wellness_sales_upc_lookup_workbook_ID).execute().get('sheets', '') if x.get('properties', '').get('title', '') == "Wellness SMS Data"][0].get('properties', '').get('sheetId', '')

update_time = datetime.datetime.now().strftime('%A %m/%d %H:%M')

result = sheets.spreadsheets().batchUpdate(spreadsheetId=wellness_sales_upc_lookup_workbook_ID,
            body={ "requests" : [
                {
                    "deleteRange" : {
                        "range" : {
                            "sheetId" : sheetId,
                            "startRowIndex" : 1
                        },
                        "shiftDimension" : "ROWS"
                    }
                },
                {
                    "updateCells" :{
                        "range" : {
                            "sheetId" : sheetId,
                            "startRowIndex" : 0,
                            "endRowIndex" : 1,
                            "startColumnIndex" : items.shape[1],
                            "startColumnIndex" : items.shape[1]+1
                        },
                        "rows" : [
                            {"values" : {"userEnteredValue" : {"stringValue" : "Updated: %s" % update_time}}}
                        ],
                        "fields" :
                        """
                        userEnteredValue.stringValue
                        """
                    }
                },
                ]}).execute()

result = sheets.spreadsheets().values().append(spreadsheetId=wellness_sales_upc_lookup_workbook_ID, range="Wellness SMS Data!A2:B2", body={ "majorDimension" : "ROWS", "values" : items.values.tolist()}, valueInputOption="RAW").execute()
result = sheets.spreadsheets().batchUpdate(spreadsheetId=wellness_sales_upc_lookup_workbook_ID,
            body={ "requests" : 
                    [{"setBasicFilter" : 
                    { "filter": {
                        "range" : {
                            "sheetId" : sheetId,
                            "startRowIndex" : 0,
                            "endRowIndex" : items.shape[0]+1,
                            "startColumnIndex" : 0,
                            "endColumnIndex" : items.shape[1]
                        }
                    }}},
                    {"autoResizeDimensions" : {"dimensions": {"sheetId" : sheetId, "dimension" : "COLUMNS"}}}
                    ]}).execute()