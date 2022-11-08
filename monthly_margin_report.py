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


average_query = """select rpt.F01 as 'UPC', obj.F29 as 'Description', obj.F155 as 'Brand',
rpc.F1024 as 'Dept', sdp.F1022 as 'Sub-Dept', round(avg(rpt.F64), 2) as 'Avg Vol', round(avg(rpt.F65), 2) as 'Avg Rev',
round(avg(rpt.F1301), 2) as 'Avg Cost',
CASE WHEN avg(rpt.F65) > 0 then round((avg(rpt.F65) - avg(rpt.F1301))/avg(rpt.F65), 4) else 0 END as 'Avg Margin'
from (select * from STORESQL.dbo.RPT_ITM_M
where F254 < '2022-10-31 00:00:00:000' and F254 > (GETDATE() - 150) and F1034 = 3) rpt
inner join STORESQL.dbo.OBJ_TAB obj on obj.F01 = rpt.F01
inner join STORESQL.dbo.POS_TAB pos on obj.F01 = pos.F01
inner join STORESQL.dbo.RPC_TAB rpc on obj.F18 = rpc.F18
inner join STORESQL.dbo.sdp_tab sdp on pos.F04 = sdp.F04
where rpc.F18 not in (21, 23, 97)
group by rpt.F01, obj.F29, obj.F155, rpc.F1024, sdp.F1022
order by rpc.F1024, round(avg(rpt.F65), 2) desc
"""

last_month_query = """select rpt.F01 as 'UPC', obj.F29 as 'Description', obj.F155 as 'Brand',
rpc.F1024 as 'Dept', sdp.F1022 as 'Sub-Dept', round(rpt.F64, 2) as 'Volume', round(rpt.F65, 2) as 'Revenue',
round(rpt.F1301, 2) as 'Cost',
CASE WHEN rpt.F65 > 0 then round((rpt.F65 - rpt.F1301)/rpt.F65, 4) else 0 END as 'Margin'
from (select * from STORESQL.dbo.RPT_ITM_M
where F254 = '2022-10-31 00:00:00:000' and F1034 = 3) rpt
inner join STORESQL.dbo.OBJ_TAB obj on obj.F01 = rpt.F01
inner join STORESQL.dbo.POS_TAB pos on obj.F01 = pos.F01
inner join STORESQL.dbo.RPC_TAB rpc on obj.F18 = rpc.F18
inner join STORESQL.dbo.sdp_tab sdp on pos.F04 = sdp.F04
where rpc.F18 not in (21, 23, 97)
order by rpc.F1024, rpt.F65 desc
"""

average = pd.read_sql(average_query, cnxn)
last_week = pd.read_sql(last_month_query, cnxn)


average.drop(['Description', 'Brand', 'Dept', 'Sub-Dept'], axis=1, inplace=True)

report = last_week.merge(average, how='inner', on='UPC', suffixes=('_lw', '_avg'))
report.fillna('')

#print(report)
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
weekly_margins_folder_id = config['DEFAULT']['margins_folder']
sheet_creds = service_account.Credentials.from_service_account_file(
                    'sheet_credentials.json', scopes=SCOPES)
drive_creds = service_account.Credentials.from_service_account_file(
    'drive_credentials.json', scopes=SCOPES)

#https://googleapis.github.io/google-api-python-client/docs/dyn/drive_v3.html
drive = build('drive', 'v3', credentials=drive_creds)
#https://googleapis.github.io/google-api-python-client/docs/dyn/sheets_v4.html
sheets = build('sheets', 'v4', credentials=sheet_creds)

folder_files = drive.files().list(q="parents in '%s'" % weekly_margins_folder_id,
                                spaces='drive',
                                fields='nextPageToken, files(id, name)',
                                pageToken=None).execute()


dept_list = report['Dept'].unique().tolist()
dept_list.sort()

title = "BFC Total Store"
# output_workbook = drive.files().create(
#             body={
#             'name' : title + ' Last Month\'s Margins' ,
#             'parents' : ['%s' % monthly_margins_folder_id],
#             'mimeType' : 'application/vnd.google-apps.spreadsheet'
#             },
#             fields='id').execute()

workbookId = [x for x in folder_files['files'] if x['name'] == 'BFC Total Store Last Month\'s Margins'][0]['id']
sheet_name = 'Monthly Margins - October 2022'
#sheet_name = 'monthly Margins %s' % datetime.date.today().strftime('%m/%d')
dept_data = report
grand_monthly_vol = dept_data['Volume'].sum()
grand_monthly_rev = dept_data['Revenue'].sum()
grand_monthly_cost = dept_data['Cost'].sum()
grand_monthly_margin = (grand_monthly_rev - grand_monthly_cost)/grand_monthly_rev

grand_avg_monthly_vol = dept_data['Avg Vol'].sum()
grand_avg_monthly_rev = dept_data['Avg Rev'].sum()
grand_avg_monthly_cost = dept_data['Avg Cost'].sum()
grand_avg_monthly_margin = (grand_avg_monthly_rev - grand_avg_monthly_cost)/grand_avg_monthly_rev

headers = dept_data.columns.tolist()

response = sheets.spreadsheets().batchUpdate(
        spreadsheetId = workbookId,
        body = {
            "requests": {
                "addSheet":{
                    "properties": {
                    "title": sheet_name,
                    "gridProperties" : { "frozenRowCount" : 4 }
                        }
                    }
        }}).execute()
sheetId = [x for x in sheets.spreadsheets().get(spreadsheetId=workbookId).execute().get('sheets', '')
                if x.get('properties', '').get('title', '') == sheet_name][0].get('properties', '').get('sheetId', '')

grand_total_row = [["","","","","","Volume","Revenue","Cost","Margin","Avg Vol","Avg Rev","Avg Cost","Avg Margin"],
["","","","","Total Month", grand_monthly_vol, grand_monthly_rev, grand_monthly_cost, grand_monthly_margin, grand_avg_monthly_vol,grand_avg_monthly_rev, grand_avg_monthly_cost, grand_avg_monthly_margin]]

result = sheets.spreadsheets().values().append(
        spreadsheetId=workbookId,
        range=sheet_name+"!A1:B1",
        body={ "majorDimension" : "ROWS", "values" : grand_total_row},
        valueInputOption="RAW"
            ).execute()

result = sheets.spreadsheets().values().append(
        spreadsheetId=workbookId,
        range=sheet_name+"!A4:B4",
        body={ "majorDimension" : "ROWS", "values" : [headers]},
        valueInputOption="RAW"
            ).execute()
    #values
result = sheets.spreadsheets().values().append(
    spreadsheetId=workbookId,
    range=sheet_name+"!A5:B5",
    body={ "majorDimension" : "ROWS", "values" : dept_data.values.tolist()},
    valueInputOption="RAW"
        ).execute()
response = sheets.spreadsheets().batchUpdate(
        spreadsheetId = workbookId,
        body = {"requests" : [{"repeatCell" :
    #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/other#GridRange
        {"range" :
            {"sheetId": sheetId,
            "startRowIndex": 3,
            "endRowIndex" : 4,
            "startColumnIndex": 0,
            "endColumnIndex": dept_data.shape[1]},
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
        }},
        {"repeatCell" :
    #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/other#GridRange
        {"range" :
            {"sheetId": sheetId,
            "startRowIndex": 0,
            "endRowIndex" : 1,
            "startColumnIndex": 5,
            "endColumnIndex": dept_data.shape[1]},
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
        }},
        {"repeatCell" :
    #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/other#GridRange
        {"range" :
            {"sheetId": sheetId,
            "startRowIndex": 1,
            "endRowIndex" : 2,
            "startColumnIndex": 4,
            "endColumnIndex": 5},
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
                "backgroundColor" : {"red":39/255, "green":175/255, "blue":245/255, "alpha": 0.35},
                "horizontalAlignment" : "CENTER",
                "verticalAlignment" : "MIDDLE"
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
                        userEnteredFormat.borders.right,
                        userEnteredFormat.horizontalAlignment,
                        userEnteredFormat.verticalAlignment"""
        }},
        {"repeatCell" :
    #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/other#GridRange
        {"range" :
            {"sheetId": sheetId,
            "startRowIndex": 1,
            "endRowIndex" : 2,
            "startColumnIndex": 5,
            "endColumnIndex": dept_data.shape[1]},
        #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/cells#CellData
        "cell": {
            "userEnteredFormat":{
                "borders":{
                    "top": {"style" : "SOLID"},
                    "bottom":{"style" : "SOLID"},
                    "left":{"style" : "SOLID"},
                    "right":{"style" : "SOLID"}
                    },
                "textFormat": { "fontFamily" : "Arial", "fontSize" : 12, "bold" : True }
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
        {"repeatCell" :
    #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/other#GridRange
        {"range" :
            {"sheetId": sheetId,
            "startRowIndex": 3,
            "endRowIndex" : 4,
            "startColumnIndex": 0,
            "endColumnIndex": dept_data.shape[1]},
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
        }},
        {"repeatCell" :
    #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/other#GridRange
        {"range" :
            {"sheetId": sheetId,
            "startRowIndex": 4,
            "endRowIndex" : dept_data.shape[0]+5,
            "startColumnIndex": 0,
            "endColumnIndex": dept_data.shape[1]},
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
            "fields" : """
                        userEnteredFormat.textFormat.fontFamily,
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
            "startRowIndex": 4,
            "endRowIndex" : dept_data.shape[0]+5,
            "startColumnIndex": dept_data.columns.get_loc("Avg Margin"),
            "endColumnIndex": dept_data.columns.get_loc("Avg Margin")+1},
        #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/cells#CellData
        "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "PERCENT", "pattern" : "0.00%"}}},
                                "fields" : """userEnteredFormat.numberFormat.type,
                                                userEnteredFormat.numberFormat.pattern"""
        }
        },
        {"repeatCell" :
    #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/other#GridRange
        {"range" :
            {"sheetId": sheetId,
            "startRowIndex": 4,
            "endRowIndex" : dept_data.shape[0]+5,
            "startColumnIndex": dept_data.columns.get_loc("Margin"),
            "endColumnIndex": dept_data.columns.get_loc("Margin")+1},
        #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/cells#CellData
        "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "PERCENT", "pattern" : "0.00%"}}},
                                "fields" : """userEnteredFormat.numberFormat.type,
                                                userEnteredFormat.numberFormat.pattern"""
        }
        },
        {"repeatCell" :
    #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/other#GridRange
        {"range" :
            {"sheetId": sheetId,
            "startRowIndex": 4,
            "endRowIndex" : dept_data.shape[0]+5,
            "startColumnIndex": dept_data.columns.get_loc("Avg Rev"),
            "endColumnIndex": dept_data.columns.get_loc("Avg Rev")+1},
        #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/cells#CellData
        "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "CURRENCY", "pattern" : "$#,##0.00"}}},
                                "fields" : """userEnteredFormat.numberFormat.type,
                                                userEnteredFormat.numberFormat.pattern"""
        }
        },
        {"repeatCell" :
    #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/other#GridRange
        {"range" :
            {"sheetId": sheetId,
            "startRowIndex": 4,
            "endRowIndex" : dept_data.shape[0]+5,
            "startColumnIndex": dept_data.columns.get_loc("Revenue"),
            "endColumnIndex": dept_data.columns.get_loc("Revenue")+1},
        #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/cells#CellData
        "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "CURRENCY", "pattern" : "$#,##0.00"}}},
                                "fields" : """userEnteredFormat.numberFormat.type,
                                                userEnteredFormat.numberFormat.pattern"""
        }
        },
        {"repeatCell" :
    #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/other#GridRange
        {"range" :
            {"sheetId": sheetId,
            "startRowIndex": 4,
            "endRowIndex" : dept_data.shape[0]+5,
            "startColumnIndex": dept_data.columns.get_loc("Avg Cost"),
            "endColumnIndex": dept_data.columns.get_loc("Avg Cost")+1},
        #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/cells#CellData
        "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "CURRENCY", "pattern" : "$#,##0.00"}}},
                                "fields" : """userEnteredFormat.numberFormat.type,
                                                userEnteredFormat.numberFormat.pattern"""
        }
        },
        {"repeatCell" :
    #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/other#GridRange
        {"range" :
            {"sheetId": sheetId,
            "startRowIndex": 4,
            "endRowIndex" : dept_data.shape[0]+5,
            "startColumnIndex": dept_data.columns.get_loc("Cost"),
            "endColumnIndex": dept_data.columns.get_loc("Cost")+1},
        #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/cells#CellData
        "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "CURRENCY", "pattern" : "$#,##0.00"}}},
                                "fields" : """userEnteredFormat.numberFormat.type,
                                                userEnteredFormat.numberFormat.pattern"""
        }
        },
        {"repeatCell" :
    #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/other#GridRange
        {"range" :
            {"sheetId": sheetId,
            "startRowIndex": 1,
            "endRowIndex" : 2,
            "startColumnIndex": dept_data.columns.get_loc("Avg Margin"),
            "endColumnIndex": dept_data.columns.get_loc("Avg Margin")+1},
        #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/cells#CellData
        "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "PERCENT", "pattern" : "0.00%"}}},
                                "fields" : """userEnteredFormat.numberFormat.type,
                                                userEnteredFormat.numberFormat.pattern"""
        }
        },
        {"repeatCell" :
    #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/other#GridRange
        {"range" :
            {"sheetId": sheetId,
            "startRowIndex": 1,
            "endRowIndex" : 2,
            "startColumnIndex": dept_data.columns.get_loc("Margin"),
            "endColumnIndex": dept_data.columns.get_loc("Margin")+1},
        #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/cells#CellData
        "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "PERCENT", "pattern" : "0.00%"}}},
                                "fields" : """userEnteredFormat.numberFormat.type,
                                                userEnteredFormat.numberFormat.pattern"""
        }
        },
        {"repeatCell" :
    #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/other#GridRange
        {"range" :
            {"sheetId": sheetId,
            "startRowIndex": 1,
            "endRowIndex" : 2,
            "startColumnIndex": dept_data.columns.get_loc("Avg Rev"),
            "endColumnIndex": dept_data.columns.get_loc("Avg Rev")+2},
        #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/cells#CellData
        "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "CURRENCY", "pattern" : "$#,##0.00"}}},
                                "fields" : """userEnteredFormat.numberFormat.type,
                                                userEnteredFormat.numberFormat.pattern"""
        }
        },
        {"repeatCell" :
    #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/other#GridRange
        {"range" :
            {"sheetId": sheetId,
            "startRowIndex": 1,
            "endRowIndex" : 2,
            "startColumnIndex": dept_data.columns.get_loc("Revenue"),
            "endColumnIndex": dept_data.columns.get_loc("Revenue")+2},
        #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/cells#CellData
        "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "CURRENCY", "pattern" : "$#,##0.00"}}},
                                "fields" : """userEnteredFormat.numberFormat.type,
                                                userEnteredFormat.numberFormat.pattern"""
        }
        },
        {
            "addConditionalFormatRule":{
                "rule":{
                    "ranges" :[{
                        "sheetId" : sheetId,
                        "startRowIndex": 4,
                        "endRowIndex" : dept_data.shape[0]+5,
                        "startColumnIndex": dept_data.columns.get_loc("Avg Vol"),
                        "endColumnIndex": dept_data.columns.get_loc("Avg Vol")+1
                        }],
                    "booleanRule" : {
                        "condition" : { "type" : "CUSTOM_FORMULA", "values" : [{"userEnteredValue" : "=F5>J5"}]},
                        "format" : {"backgroundColor" : {"red":50/255, "green":248/255, "blue":40/255, "alpha": 0.05}}
                        }
                    },
                    "index" : 0
                }
        },
        {
            "addConditionalFormatRule":{
                "rule":{
                    "ranges" :[{
                        "sheetId" : sheetId,
                        "startRowIndex": 4,
                        "endRowIndex" : dept_data.shape[0]+5,
                        "startColumnIndex": dept_data.columns.get_loc("Avg Vol"),
                        "endColumnIndex": dept_data.columns.get_loc("Avg Vol")+1
                        }],
                    "booleanRule" : {
                        "condition" : { "type" : "CUSTOM_FORMULA", "values" : [{"userEnteredValue" : "=F5<J5"}]},
                        "format" : {"backgroundColor" : {"red":225/255, "green":40/255, "blue":40/255, "alpha": 0.05}}
                        }
                    },
                    "index" : 0
                }
        },
        {
            "addConditionalFormatRule":{
                "rule":{
                    "ranges" :[{
                        "sheetId" : sheetId,
                        "startRowIndex": 4,
                        "endRowIndex" : dept_data.shape[0]+5,
                        "startColumnIndex": dept_data.columns.get_loc("Avg Rev"),
                        "endColumnIndex": dept_data.columns.get_loc("Avg Rev")+1
                        }],
                    "booleanRule" : {
                        "condition" : { "type" : "CUSTOM_FORMULA", "values" : [{"userEnteredValue" : "=G5>K5"}]},
                        "format" : {"backgroundColor" : {"red":50/255, "green":248/255, "blue":40/255, "alpha": 0.05}}
                        }
                    },
                    "index" : 0
                }
        },
        {
            "addConditionalFormatRule":{
                "rule":{
                    "ranges" :[{
                        "sheetId" : sheetId,
                        "startRowIndex": 4,
                        "endRowIndex" : dept_data.shape[0]+5,
                        "startColumnIndex": dept_data.columns.get_loc("Avg Rev"),
                        "endColumnIndex": dept_data.columns.get_loc("Avg Rev")+1
                        }],
                    "booleanRule" : {
                        "condition" : { "type" : "CUSTOM_FORMULA", "values" : [{"userEnteredValue" : "=G5<K5"}]},
                        "format" : {"backgroundColor" : {"red":225/255, "green":40/255, "blue":40/255, "alpha": 0.05}}
                        }
                    },
                    "index" : 0
                }
        },
        {
            "addConditionalFormatRule":{
                "rule":{
                    "ranges" :[{
                        "sheetId" : sheetId,
                        "startRowIndex": 4,
                        "endRowIndex" : dept_data.shape[0]+5,
                        "startColumnIndex": dept_data.columns.get_loc("Avg Cost"),
                        "endColumnIndex": dept_data.columns.get_loc("Avg Cost")+1
                        }],
                    "booleanRule" : {
                        "condition" : { "type" : "CUSTOM_FORMULA", "values" : [{"userEnteredValue" : "=H5>L5"}]},
                        "format" : {"backgroundColor" : {"red":50/255, "green":248/255, "blue":40/255, "alpha": 0.05}}
                        }
                    },
                    "index" : 0
                }
        },
        {
            "addConditionalFormatRule":{
                "rule":{
                    "ranges" :[{
                        "sheetId" : sheetId,
                        "startRowIndex": 4,
                        "endRowIndex" : dept_data.shape[0]+5,
                        "startColumnIndex": dept_data.columns.get_loc("Avg Cost"),
                        "endColumnIndex": dept_data.columns.get_loc("Avg Cost")+1
                        }],
                    "booleanRule" : {
                        "condition" : { "type" : "CUSTOM_FORMULA", "values" : [{"userEnteredValue" : "=H5<L5"}]},
                        "format" : {"backgroundColor" : {"red":225/255, "green":40/255, "blue":40/255, "alpha": 0.05}}
                        }
                    },
                    "index" : 0
                }
        },
        {
            "addConditionalFormatRule":{
                "rule":{
                    "ranges" :[{
                        "sheetId" : sheetId,
                        "startRowIndex": 4,
                        "endRowIndex" : dept_data.shape[0]+5,
                        "startColumnIndex": dept_data.columns.get_loc("Avg Margin"),
                        "endColumnIndex": dept_data.columns.get_loc("Avg Margin")+1
                        }],
                    "booleanRule" : {
                        "condition" : { "type" : "CUSTOM_FORMULA", "values" : [{"userEnteredValue" : "=I5>M5"}]},
                        "format" : {"backgroundColor" : {"red":50/255, "green":248/255, "blue":40/255, "alpha": 0.05}}
                        }
                    },
                    "index" : 0
                }
        },
        {
            "addConditionalFormatRule":{
                "rule":{
                    "ranges" :[{
                        "sheetId" : sheetId,
                        "startRowIndex": 4,
                        "endRowIndex" : dept_data.shape[0]+5,
                        "startColumnIndex": dept_data.columns.get_loc("Avg Margin"),
                        "endColumnIndex": dept_data.columns.get_loc("Avg Margin")+1
                        }],
                    "booleanRule" : {
                        "condition" : { "type" : "CUSTOM_FORMULA", "values" : [{"userEnteredValue" : "=I5<M5"}]},
                        "format" : {"backgroundColor" : {"red":225/255, "green":40/255, "blue":40/255, "alpha": 0.05}}
                        }
                    },
                    "index" : 0
                }
        },
        {
            "addConditionalFormatRule":{
                "rule":{
                    "ranges" :[{
                        "sheetId" : sheetId,
                        "startRowIndex": 1,
                        "endRowIndex" : 2,
                        "startColumnIndex": dept_data.columns.get_loc("Avg Vol"),
                        "endColumnIndex": dept_data.columns.get_loc("Avg Vol")+1
                        }],
                    "booleanRule" : {
                        "condition" : { "type" : "CUSTOM_FORMULA", "values" : [{"userEnteredValue" : "=F2>J2"}]},
                        "format" : {"backgroundColor" : {"red":50/255, "green":248/255, "blue":40/255, "alpha": 0.05}}
                        }
                    },
                    "index" : 0
                }
        },
        {
            "addConditionalFormatRule":{
                "rule":{
                    "ranges" :[{
                        "sheetId" : sheetId,
                        "startRowIndex": 1,
                        "endRowIndex" : 2,
                        "startColumnIndex": dept_data.columns.get_loc("Avg Vol"),
                        "endColumnIndex": dept_data.columns.get_loc("Avg Vol")+1
                        }],
                    "booleanRule" : {
                        "condition" : { "type" : "CUSTOM_FORMULA", "values" : [{"userEnteredValue" : "=F2<J2"}]},
                        "format" : {"backgroundColor" : {"red":225/255, "green":40/255, "blue":40/255, "alpha": 0.05}}
                        }
                    },
                    "index" : 0
                }
        },
        {
            "addConditionalFormatRule":{
                "rule":{
                    "ranges" :[{
                        "sheetId" : sheetId,
                        "startRowIndex": 1,
                        "endRowIndex" : 2,
                        "startColumnIndex": dept_data.columns.get_loc("Avg Rev"),
                        "endColumnIndex": dept_data.columns.get_loc("Avg Rev")+1
                        }],
                    "booleanRule" : {
                        "condition" : { "type" : "CUSTOM_FORMULA", "values" : [{"userEnteredValue" : "=G2>K2"}]},
                        "format" : {"backgroundColor" : {"red":50/255, "green":248/255, "blue":40/255, "alpha": 0.05}}
                        }
                    },
                    "index" : 0
                }
        },
        {
            "addConditionalFormatRule":{
                "rule":{
                    "ranges" :[{
                        "sheetId" : sheetId,
                        "startRowIndex": 1,
                        "endRowIndex" : 2,
                        "startColumnIndex": dept_data.columns.get_loc("Avg Rev"),
                        "endColumnIndex": dept_data.columns.get_loc("Avg Rev")+1
                        }],
                    "booleanRule" : {
                        "condition" : { "type" : "CUSTOM_FORMULA", "values" : [{"userEnteredValue" : "=G2<K2"}]},
                        "format" : {"backgroundColor" : {"red":225/255, "green":40/255, "blue":40/255, "alpha": 0.05}}
                        }
                    },
                    "index" : 0
                }
        },
        {
            "addConditionalFormatRule":{
                "rule":{
                    "ranges" :[{
                        "sheetId" : sheetId,
                        "startRowIndex": 1,
                        "endRowIndex" : 2,
                        "startColumnIndex": dept_data.columns.get_loc("Avg Cost"),
                        "endColumnIndex": dept_data.columns.get_loc("Avg Cost")+1
                        }],
                    "booleanRule" : {
                        "condition" : { "type" : "CUSTOM_FORMULA", "values" : [{"userEnteredValue" : "=H2>L2"}]},
                        "format" : {"backgroundColor" : {"red":50/255, "green":248/255, "blue":40/255, "alpha": 0.05}}
                        }
                    },
                    "index" : 0
                }
        },
        {
            "addConditionalFormatRule":{
                "rule":{
                    "ranges" :[{
                        "sheetId" : sheetId,
                        "startRowIndex": 1,
                        "endRowIndex" : 2,
                        "startColumnIndex": dept_data.columns.get_loc("Avg Cost"),
                        "endColumnIndex": dept_data.columns.get_loc("Avg Cost")+1
                        }],
                    "booleanRule" : {
                        "condition" : { "type" : "CUSTOM_FORMULA", "values" : [{"userEnteredValue" : "=H2<L2"}]},
                        "format" : {"backgroundColor" : {"red":225/255, "green":40/255, "blue":40/255, "alpha": 0.05}}
                        }
                    },
                    "index" : 0
                }
        },
        {
            "addConditionalFormatRule":{
                "rule":{
                    "ranges" :[{
                        "sheetId" : sheetId,
                        "startRowIndex": 1,
                        "endRowIndex" : 2,
                        "startColumnIndex": dept_data.columns.get_loc("Avg Margin"),
                        "endColumnIndex": dept_data.columns.get_loc("Avg Margin")+1
                        }],
                    "booleanRule" : {
                        "condition" : { "type" : "CUSTOM_FORMULA", "values" : [{"userEnteredValue" : "=I2>M2"}]},
                        "format" : {"backgroundColor" : {"red":50/255, "green":248/255, "blue":40/255, "alpha": 0.05}}
                        }
                    },
                    "index" : 0
                }
        },
        {
            "addConditionalFormatRule":{
                "rule":{
                    "ranges" :[{
                        "sheetId" : sheetId,
                        "startRowIndex": 1,
                        "endRowIndex" : 2,
                        "startColumnIndex": dept_data.columns.get_loc("Avg Margin"),
                        "endColumnIndex": dept_data.columns.get_loc("Avg Margin")+1
                        }],
                    "booleanRule" : {
                        "condition" : { "type" : "CUSTOM_FORMULA", "values" : [{"userEnteredValue" : "=I2<M2"}]},
                        "format" : {"backgroundColor" : {"red":225/255, "green":40/255, "blue":40/255, "alpha": 0.05}}
                        }
                    },
                    "index" : 0
                }
        },
        {
        "autoResizeDimensions":{
            "dimensions" : {
                "sheetId" : sheetId,
                "dimension" : "COLUMNS",
                "startIndex" : 0,
                "endIndex" : dept_data.shape[0]+1
                }
            }
        }]
        }).execute()
response = sheets.spreadsheets().batchUpdate(spreadsheetId = workbookId, body = {"requests": {"updateSheetProperties":{"properties":{"sheetId" : sheetId, "index":0}, "fields":"index"}}}).execute()
#response = sheets.spreadsheets().batchUpdate(spreadsheetId = workbookId, body = {"requests":{"deleteSheet":{"sheetId" : 0}}}).execute()
time.sleep(7)

for dept in dept_list:
    title = dept.split(' ')[0] if dept != '' else 'No Dept'
    # output_workbook = drive.files().create(
    #         body={
    #         'name' : title + ' Last Month\'s Margins' ,
    #         'parents' : ['%s' % monthly_margins_folder_id],
    #         'mimeType' : 'application/vnd.google-apps.spreadsheet'
    #         },
    #         fields='id').execute()

    workbookId = [x for x in folder_files['files'] if x['name'] == title + ' Last Month\'s Margins'][0]['id']
    sheet_name = 'Monthly Margins - October 2022'
    #sheet_name = 'monthly Margins %s' % datetime.date.today().strftime('%m/%d')
    dept_data = report[report['Dept'] == dept]
    grand_monthly_vol = dept_data['Volume'].sum()
    grand_monthly_rev = dept_data['Revenue'].sum()
    grand_monthly_cost = dept_data['Cost'].sum()
    grand_monthly_margin = (grand_monthly_rev - grand_monthly_cost)/grand_monthly_rev

    grand_avg_monthly_vol = dept_data['Avg Vol'].sum()
    grand_avg_monthly_rev = dept_data['Avg Rev'].sum()
    grand_avg_monthly_cost = dept_data['Avg Cost'].sum()
    grand_avg_monthly_margin = (grand_avg_monthly_rev - grand_avg_monthly_cost)/grand_avg_monthly_rev


    dept_data = dept_data.drop(['Dept'], axis=1)
    headers = dept_data.columns.tolist()

    response = sheets.spreadsheets().batchUpdate(
            spreadsheetId = workbookId,
            body = {
                "requests": {
                    "addSheet":{
                        "properties": {
                        "title": sheet_name,
                        "gridProperties" : { "frozenRowCount" : 4 }
                            }
                        }
            }}).execute()
    sheetId = [x for x in sheets.spreadsheets().get(spreadsheetId=workbookId).execute().get('sheets', '')
                    if x.get('properties', '').get('title', '') == sheet_name][0].get('properties', '').get('sheetId', '')

    grand_total_row = [["","","","","Volume","Revenue","Cost","Margin","Avg Vol","Avg Rev","Avg Cost","Avg Margin"],
    ["","","","Total Month", grand_monthly_vol, grand_monthly_rev, grand_monthly_cost, grand_monthly_margin, grand_avg_monthly_vol,grand_avg_monthly_rev, grand_avg_monthly_cost, grand_avg_monthly_margin]]

    result = sheets.spreadsheets().values().append(
            spreadsheetId=workbookId,
            range=sheet_name+"!A1:B1",
            body={ "majorDimension" : "ROWS", "values" : grand_total_row},
            valueInputOption="RAW"
                ).execute()

    result = sheets.spreadsheets().values().append(
            spreadsheetId=workbookId,
            range=sheet_name+"!A4:B4",
            body={ "majorDimension" : "ROWS", "values" : [headers]},
            valueInputOption="RAW"
                ).execute()
        #values
    result = sheets.spreadsheets().values().append(
        spreadsheetId=workbookId,
        range=sheet_name+"!A5:B5",
        body={ "majorDimension" : "ROWS", "values" : dept_data.values.tolist()},
        valueInputOption="RAW"
            ).execute()
    response = sheets.spreadsheets().batchUpdate(
            spreadsheetId = workbookId,
            body = {"requests" : [{"repeatCell" :
        #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/other#GridRange
            {"range" :
                {"sheetId": sheetId,
                "startRowIndex": 3,
                "endRowIndex" : 4,
                "startColumnIndex": 0,
                "endColumnIndex": dept_data.shape[1]},
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
            }},
            {"repeatCell" :
        #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/other#GridRange
            {"range" :
                {"sheetId": sheetId,
                "startRowIndex": 0,
                "endRowIndex" : 1,
                "startColumnIndex": 4,
                "endColumnIndex": dept_data.shape[1]},
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
            }},
            {"repeatCell" :
        #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/other#GridRange
            {"range" :
                {"sheetId": sheetId,
                "startRowIndex": 1,
                "endRowIndex" : 2,
                "startColumnIndex": 3,
                "endColumnIndex": 4},
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
                    "backgroundColor" : {"red":39/255, "green":175/255, "blue":245/255, "alpha": 0.35},
                    "horizontalAlignment" : "CENTER",
                    "verticalAlignment" : "MIDDLE"
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
                            userEnteredFormat.borders.right,
                            userEnteredFormat.horizontalAlignment,
                            userEnteredFormat.verticalAlignment"""
            }},
            {"repeatCell" :
        #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/other#GridRange
            {"range" :
                {"sheetId": sheetId,
                "startRowIndex": 1,
                "endRowIndex" : 2,
                "startColumnIndex": 4,
                "endColumnIndex": dept_data.shape[1]},
            #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/cells#CellData
            "cell": {
                "userEnteredFormat":{
                    "borders":{
                        "top": {"style" : "SOLID"},
                        "bottom":{"style" : "SOLID"},
                        "left":{"style" : "SOLID"},
                        "right":{"style" : "SOLID"}
                        },
                    "textFormat": { "fontFamily" : "Arial", "fontSize" : 12, "bold" : True }
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
            {"repeatCell" :
        #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/other#GridRange
            {"range" :
                {"sheetId": sheetId,
                "startRowIndex": 3,
                "endRowIndex" : 4,
                "startColumnIndex": 0,
                "endColumnIndex": dept_data.shape[1]},
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
            }},
            {"repeatCell" :
        #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/other#GridRange
            {"range" :
                {"sheetId": sheetId,
                "startRowIndex": 4,
                "endRowIndex" : dept_data.shape[0]+5,
                "startColumnIndex": 0,
                "endColumnIndex": dept_data.shape[1]},
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
                "fields" : """
                            userEnteredFormat.textFormat.fontFamily,
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
                "startRowIndex": 4,
                "endRowIndex" : dept_data.shape[0]+5,
                "startColumnIndex": dept_data.columns.get_loc("Avg Margin"),
                "endColumnIndex": dept_data.columns.get_loc("Avg Margin")+1},
            #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/cells#CellData
            "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "PERCENT", "pattern" : "0.00%"}}},
                                    "fields" : """userEnteredFormat.numberFormat.type,
                                                    userEnteredFormat.numberFormat.pattern"""
            }
            },
            {"repeatCell" :
        #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/other#GridRange
            {"range" :
                {"sheetId": sheetId,
                "startRowIndex": 4,
                "endRowIndex" : dept_data.shape[0]+5,
                "startColumnIndex": dept_data.columns.get_loc("Margin"),
                "endColumnIndex": dept_data.columns.get_loc("Margin")+1},
            #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/cells#CellData
            "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "PERCENT", "pattern" : "0.00%"}}},
                                    "fields" : """userEnteredFormat.numberFormat.type,
                                                    userEnteredFormat.numberFormat.pattern"""
            }
            },
            {"repeatCell" :
        #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/other#GridRange
            {"range" :
                {"sheetId": sheetId,
                "startRowIndex": 4,
                "endRowIndex" : dept_data.shape[0]+5,
                "startColumnIndex": dept_data.columns.get_loc("Avg Rev"),
                "endColumnIndex": dept_data.columns.get_loc("Avg Rev")+1},
            #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/cells#CellData
            "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "CURRENCY", "pattern" : "$#,##0.00"}}},
                                    "fields" : """userEnteredFormat.numberFormat.type,
                                                    userEnteredFormat.numberFormat.pattern"""
            }
            },
            {"repeatCell" :
        #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/other#GridRange
            {"range" :
                {"sheetId": sheetId,
                "startRowIndex": 4,
                "endRowIndex" : dept_data.shape[0]+5,
                "startColumnIndex": dept_data.columns.get_loc("Revenue"),
                "endColumnIndex": dept_data.columns.get_loc("Revenue")+1},
            #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/cells#CellData
            "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "CURRENCY", "pattern" : "$#,##0.00"}}},
                                    "fields" : """userEnteredFormat.numberFormat.type,
                                                    userEnteredFormat.numberFormat.pattern"""
            }
            },
            {"repeatCell" :
        #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/other#GridRange
            {"range" :
                {"sheetId": sheetId,
                "startRowIndex": 4,
                "endRowIndex" : dept_data.shape[0]+5,
                "startColumnIndex": dept_data.columns.get_loc("Avg Cost"),
                "endColumnIndex": dept_data.columns.get_loc("Avg Cost")+1},
            #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/cells#CellData
            "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "CURRENCY", "pattern" : "$#,##0.00"}}},
                                    "fields" : """userEnteredFormat.numberFormat.type,
                                                    userEnteredFormat.numberFormat.pattern"""
            }
            },
            {"repeatCell" :
        #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/other#GridRange
            {"range" :
                {"sheetId": sheetId,
                "startRowIndex": 4,
                "endRowIndex" : dept_data.shape[0]+5,
                "startColumnIndex": dept_data.columns.get_loc("Cost"),
                "endColumnIndex": dept_data.columns.get_loc("Cost")+1},
            #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/cells#CellData
            "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "CURRENCY", "pattern" : "$#,##0.00"}}},
                                    "fields" : """userEnteredFormat.numberFormat.type,
                                                    userEnteredFormat.numberFormat.pattern"""
            }
            },
            {"repeatCell" :
        #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/other#GridRange
            {"range" :
                {"sheetId": sheetId,
                "startRowIndex": 1,
                "endRowIndex" : 2,
                "startColumnIndex": dept_data.columns.get_loc("Avg Margin"),
                "endColumnIndex": dept_data.columns.get_loc("Avg Margin")+1},
            #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/cells#CellData
            "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "PERCENT", "pattern" : "0.00%"}}},
                                    "fields" : """userEnteredFormat.numberFormat.type,
                                                    userEnteredFormat.numberFormat.pattern"""
            }
            },
            {"repeatCell" :
        #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/other#GridRange
            {"range" :
                {"sheetId": sheetId,
                "startRowIndex": 1,
                "endRowIndex" : 2,
                "startColumnIndex": dept_data.columns.get_loc("Margin"),
                "endColumnIndex": dept_data.columns.get_loc("Margin")+1},
            #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/cells#CellData
            "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "PERCENT", "pattern" : "0.00%"}}},
                                    "fields" : """userEnteredFormat.numberFormat.type,
                                                    userEnteredFormat.numberFormat.pattern"""
            }
            },
            {"repeatCell" :
        #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/other#GridRange
            {"range" :
                {"sheetId": sheetId,
                "startRowIndex": 1,
                "endRowIndex" : 2,
                "startColumnIndex": dept_data.columns.get_loc("Avg Rev"),
                "endColumnIndex": dept_data.columns.get_loc("Avg Rev")+2},
            #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/cells#CellData
            "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "CURRENCY", "pattern" : "$#,##0.00"}}},
                                    "fields" : """userEnteredFormat.numberFormat.type,
                                                    userEnteredFormat.numberFormat.pattern"""
            }
            },
            {"repeatCell" :
        #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/other#GridRange
            {"range" :
                {"sheetId": sheetId,
                "startRowIndex": 1,
                "endRowIndex" : 2,
                "startColumnIndex": dept_data.columns.get_loc("Revenue"),
                "endColumnIndex": dept_data.columns.get_loc("Revenue")+2},
            #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/cells#CellData
            "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "CURRENCY", "pattern" : "$#,##0.00"}}},
                                    "fields" : """userEnteredFormat.numberFormat.type,
                                                    userEnteredFormat.numberFormat.pattern"""
            }
            },
            {
                "addConditionalFormatRule":{
                    "rule":{
                        "ranges" :[{
                            "sheetId" : sheetId,
                            "startRowIndex": 4,
                            "endRowIndex" : dept_data.shape[0]+5,
                            "startColumnIndex": dept_data.columns.get_loc("Avg Vol"),
                            "endColumnIndex": dept_data.columns.get_loc("Avg Vol")+1
                            }],
                        "booleanRule" : {
                            "condition" : { "type" : "CUSTOM_FORMULA", "values" : [{"userEnteredValue" : "=E5>I5"}]},
                            "format" : {"backgroundColor" : {"red":50/255, "green":248/255, "blue":40/255, "alpha": 0.05}}
                            }
                        },
                        "index" : 0
                    }
            },
            {
                "addConditionalFormatRule":{
                    "rule":{
                        "ranges" :[{
                            "sheetId" : sheetId,
                            "startRowIndex": 4,
                            "endRowIndex" : dept_data.shape[0]+5,
                            "startColumnIndex": dept_data.columns.get_loc("Avg Vol"),
                            "endColumnIndex": dept_data.columns.get_loc("Avg Vol")+1
                            }],
                        "booleanRule" : {
                            "condition" : { "type" : "CUSTOM_FORMULA", "values" : [{"userEnteredValue" : "=E5<I5"}]},
                            "format" : {"backgroundColor" : {"red":225/255, "green":40/255, "blue":40/255, "alpha": 0.05}}
                            }
                        },
                        "index" : 0
                    }
            },
            {
                "addConditionalFormatRule":{
                    "rule":{
                        "ranges" :[{
                            "sheetId" : sheetId,
                            "startRowIndex": 4,
                            "endRowIndex" : dept_data.shape[0]+5,
                            "startColumnIndex": dept_data.columns.get_loc("Avg Rev"),
                            "endColumnIndex": dept_data.columns.get_loc("Avg Rev")+1
                            }],
                        "booleanRule" : {
                            "condition" : { "type" : "CUSTOM_FORMULA", "values" : [{"userEnteredValue" : "=F5>J5"}]},
                            "format" : {"backgroundColor" : {"red":50/255, "green":248/255, "blue":40/255, "alpha": 0.05}}
                            }
                        },
                        "index" : 0
                    }
            },
            {
                "addConditionalFormatRule":{
                    "rule":{
                        "ranges" :[{
                            "sheetId" : sheetId,
                            "startRowIndex": 4,
                            "endRowIndex" : dept_data.shape[0]+5,
                            "startColumnIndex": dept_data.columns.get_loc("Avg Rev"),
                            "endColumnIndex": dept_data.columns.get_loc("Avg Rev")+1
                            }],
                        "booleanRule" : {
                            "condition" : { "type" : "CUSTOM_FORMULA", "values" : [{"userEnteredValue" : "=F5<J5"}]},
                            "format" : {"backgroundColor" : {"red":225/255, "green":40/255, "blue":40/255, "alpha": 0.05}}
                            }
                        },
                        "index" : 0
                    }
            },
            {
                "addConditionalFormatRule":{
                    "rule":{
                        "ranges" :[{
                            "sheetId" : sheetId,
                            "startRowIndex": 4,
                            "endRowIndex" : dept_data.shape[0]+5,
                            "startColumnIndex": dept_data.columns.get_loc("Avg Cost"),
                            "endColumnIndex": dept_data.columns.get_loc("Avg Cost")+1
                            }],
                        "booleanRule" : {
                            "condition" : { "type" : "CUSTOM_FORMULA", "values" : [{"userEnteredValue" : "=G5>K5"}]},
                            "format" : {"backgroundColor" : {"red":50/255, "green":248/255, "blue":40/255, "alpha": 0.05}}
                            }
                        },
                        "index" : 0
                    }
            },
            {
                "addConditionalFormatRule":{
                    "rule":{
                        "ranges" :[{
                            "sheetId" : sheetId,
                            "startRowIndex": 4,
                            "endRowIndex" : dept_data.shape[0]+5,
                            "startColumnIndex": dept_data.columns.get_loc("Avg Cost"),
                            "endColumnIndex": dept_data.columns.get_loc("Avg Cost")+1
                            }],
                        "booleanRule" : {
                            "condition" : { "type" : "CUSTOM_FORMULA", "values" : [{"userEnteredValue" : "=G5<K5"}]},
                            "format" : {"backgroundColor" : {"red":225/255, "green":40/255, "blue":40/255, "alpha": 0.05}}
                            }
                        },
                        "index" : 0
                    }
            },
            {
                "addConditionalFormatRule":{
                    "rule":{
                        "ranges" :[{
                            "sheetId" : sheetId,
                            "startRowIndex": 4,
                            "endRowIndex" : dept_data.shape[0]+5,
                            "startColumnIndex": dept_data.columns.get_loc("Avg Margin"),
                            "endColumnIndex": dept_data.columns.get_loc("Avg Margin")+1
                            }],
                        "booleanRule" : {
                            "condition" : { "type" : "CUSTOM_FORMULA", "values" : [{"userEnteredValue" : "=H5>L5"}]},
                            "format" : {"backgroundColor" : {"red":50/255, "green":248/255, "blue":40/255, "alpha": 0.05}}
                            }
                        },
                        "index" : 0
                    }
            },
            {
                "addConditionalFormatRule":{
                    "rule":{
                        "ranges" :[{
                            "sheetId" : sheetId,
                            "startRowIndex": 4,
                            "endRowIndex" : dept_data.shape[0]+5,
                            "startColumnIndex": dept_data.columns.get_loc("Avg Margin"),
                            "endColumnIndex": dept_data.columns.get_loc("Avg Margin")+1
                            }],
                        "booleanRule" : {
                            "condition" : { "type" : "CUSTOM_FORMULA", "values" : [{"userEnteredValue" : "=H5<L5"}]},
                            "format" : {"backgroundColor" : {"red":225/255, "green":40/255, "blue":40/255, "alpha": 0.05}}
                            }
                        },
                        "index" : 0
                    }
            },
            {
                "addConditionalFormatRule":{
                    "rule":{
                        "ranges" :[{
                            "sheetId" : sheetId,
                            "startRowIndex": 1,
                            "endRowIndex" : 2,
                            "startColumnIndex": dept_data.columns.get_loc("Avg Vol"),
                            "endColumnIndex": dept_data.columns.get_loc("Avg Vol")+1
                            }],
                        "booleanRule" : {
                            "condition" : { "type" : "CUSTOM_FORMULA", "values" : [{"userEnteredValue" : "=E2>I2"}]},
                            "format" : {"backgroundColor" : {"red":50/255, "green":248/255, "blue":40/255, "alpha": 0.05}}
                            }
                        },
                        "index" : 0
                    }
            },
            {
                "addConditionalFormatRule":{
                    "rule":{
                        "ranges" :[{
                            "sheetId" : sheetId,
                            "startRowIndex": 1,
                            "endRowIndex" : 2,
                            "startColumnIndex": dept_data.columns.get_loc("Avg Vol"),
                            "endColumnIndex": dept_data.columns.get_loc("Avg Vol")+1
                            }],
                        "booleanRule" : {
                            "condition" : { "type" : "CUSTOM_FORMULA", "values" : [{"userEnteredValue" : "=E2<I2"}]},
                            "format" : {"backgroundColor" : {"red":225/255, "green":40/255, "blue":40/255, "alpha": 0.05}}
                            }
                        },
                        "index" : 0
                    }
            },
            {
                "addConditionalFormatRule":{
                    "rule":{
                        "ranges" :[{
                            "sheetId" : sheetId,
                            "startRowIndex": 1,
                            "endRowIndex" : 2,
                            "startColumnIndex": dept_data.columns.get_loc("Avg Rev"),
                            "endColumnIndex": dept_data.columns.get_loc("Avg Rev")+1
                            }],
                        "booleanRule" : {
                            "condition" : { "type" : "CUSTOM_FORMULA", "values" : [{"userEnteredValue" : "=F2>J2"}]},
                            "format" : {"backgroundColor" : {"red":50/255, "green":248/255, "blue":40/255, "alpha": 0.05}}
                            }
                        },
                        "index" : 0
                    }
            },
            {
                "addConditionalFormatRule":{
                    "rule":{
                        "ranges" :[{
                            "sheetId" : sheetId,
                            "startRowIndex": 1,
                            "endRowIndex" : 2,
                            "startColumnIndex": dept_data.columns.get_loc("Avg Rev"),
                            "endColumnIndex": dept_data.columns.get_loc("Avg Rev")+1
                            }],
                        "booleanRule" : {
                            "condition" : { "type" : "CUSTOM_FORMULA", "values" : [{"userEnteredValue" : "=F2<J2"}]},
                            "format" : {"backgroundColor" : {"red":225/255, "green":40/255, "blue":40/255, "alpha": 0.05}}
                            }
                        },
                        "index" : 0
                    }
            },
            {
                "addConditionalFormatRule":{
                    "rule":{
                        "ranges" :[{
                            "sheetId" : sheetId,
                            "startRowIndex": 1,
                            "endRowIndex" : 2,
                            "startColumnIndex": dept_data.columns.get_loc("Avg Cost"),
                            "endColumnIndex": dept_data.columns.get_loc("Avg Cost")+1
                            }],
                        "booleanRule" : {
                            "condition" : { "type" : "CUSTOM_FORMULA", "values" : [{"userEnteredValue" : "=G2>K2"}]},
                            "format" : {"backgroundColor" : {"red":50/255, "green":248/255, "blue":40/255, "alpha": 0.05}}
                            }
                        },
                        "index" : 0
                    }
            },
            {
                "addConditionalFormatRule":{
                    "rule":{
                        "ranges" :[{
                            "sheetId" : sheetId,
                            "startRowIndex": 1,
                            "endRowIndex" : 2,
                            "startColumnIndex": dept_data.columns.get_loc("Avg Cost"),
                            "endColumnIndex": dept_data.columns.get_loc("Avg Cost")+1
                            }],
                        "booleanRule" : {
                            "condition" : { "type" : "CUSTOM_FORMULA", "values" : [{"userEnteredValue" : "=G2<K2"}]},
                            "format" : {"backgroundColor" : {"red":225/255, "green":40/255, "blue":40/255, "alpha": 0.05}}
                            }
                        },
                        "index" : 0
                    }
            },
            {
                "addConditionalFormatRule":{
                    "rule":{
                        "ranges" :[{
                            "sheetId" : sheetId,
                            "startRowIndex": 1,
                            "endRowIndex" : 2,
                            "startColumnIndex": dept_data.columns.get_loc("Avg Margin"),
                            "endColumnIndex": dept_data.columns.get_loc("Avg Margin")+1
                            }],
                        "booleanRule" : {
                            "condition" : { "type" : "CUSTOM_FORMULA", "values" : [{"userEnteredValue" : "=H2>L2"}]},
                            "format" : {"backgroundColor" : {"red":50/255, "green":248/255, "blue":40/255, "alpha": 0.05}}
                            }
                        },
                        "index" : 0
                    }
            },
            {
                "addConditionalFormatRule":{
                    "rule":{
                        "ranges" :[{
                            "sheetId" : sheetId,
                            "startRowIndex": 1,
                            "endRowIndex" : 2,
                            "startColumnIndex": dept_data.columns.get_loc("Avg Margin"),
                            "endColumnIndex": dept_data.columns.get_loc("Avg Margin")+1
                            }],
                        "booleanRule" : {
                            "condition" : { "type" : "CUSTOM_FORMULA", "values" : [{"userEnteredValue" : "=H2<L2"}]},
                            "format" : {"backgroundColor" : {"red":225/255, "green":40/255, "blue":40/255, "alpha": 0.05}}
                            }
                        },
                        "index" : 0
                    }
            },
            {
            "autoResizeDimensions":{
                "dimensions" : {
                    "sheetId" : sheetId,
                    "dimension" : "COLUMNS",
                    "startIndex" : 0,
                    "endIndex" : dept_data.shape[0]+1
                    }
                }
            }]
            }).execute()
    response = sheets.spreadsheets().batchUpdate(spreadsheetId = workbookId, body = {"requests": {"updateSheetProperties":{"properties":{"sheetId" : sheetId, "index":0}, "fields":"index"}}}).execute()
    #response = sheets.spreadsheets().batchUpdate(spreadsheetId = workbookId, body = {"requests":{"deleteSheet":{"sheetId" : 0}}}).execute()
    time.sleep(7)