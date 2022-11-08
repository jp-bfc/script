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


# after_sale_start = """
# select prc.F01 as 'UPC', obj.F29 as 'Description', obj.F155 as 'Brand', rpc.F1024 as 'Dept', sdp.F1022 as 'Sub-Dept',
# prc.F30 as 'Regular Price', prc.F136 as 'Sale Price', prc.F137 as 'Sale Start', prc.F138 as 'Sale End', round(avg(d.F64), 2) as 'AverageAfterVolume', round(avg(d.F65), 2) as 'AverageAfterRevenue'
# from (select * from STORESQL.dbo.PRICE_TAB
# where F138 < '2022-08-04 00:00:00:000') prc
# inner join STORESQL.dbo.OBJ_TAB obj on obj.F01 = prc.F01
# inner join STORESQL.dbo.POS_TAB pos on obj.F01 = pos.F01
# inner join STORESQL.dbo.RPC_TAB rpc on obj.F18 = rpc.F18
# inner join STORESQL.dbo.sdp_tab sdp on pos.F04 = sdp.F04
# left join (select * from STORESQL.dbo.RPT_ITM_D where F1034 = 3) d on prc.F01 = d.F01
# where rpc.F18 not in (21, 23) and d.F254 > prc.F137
# group by prc.F01, obj.F29, obj.F155, rpc.F1024, sdp.F1022, prc.F30, prc.F136, prc.F137, prc.F138
# order by rpc.F1024, obj.F155, prc.F01
# """

# before_sale_start = """
# select prc.F01 as 'UPC', obj.F29 as 'Description', obj.F155 as 'Brand', rpc.F1024 as 'Dept', sdp.F1022 as 'Sub-Dept',
# prc.F30 as 'Regular Price', prc.F136 as 'Sale Price', prc.F137 as 'Sale Start', round(avg(d.F64), 2) as 'AverageBeforeVolume', round(avg(d.F65), 2) as 'AverageBeforeRevenue'
# from (select * from STORESQL.dbo.PRICE_TAB
# where F138 < '2022-08-04 00:00:00:000') prc
# inner join STORESQL.dbo.OBJ_TAB obj on obj.F01 = prc.F01
# inner join STORESQL.dbo.POS_TAB pos on obj.F01 = pos.F01
# inner join STORESQL.dbo.RPC_TAB rpc on obj.F18 = rpc.F18
# inner join STORESQL.dbo.sdp_tab sdp on pos.F04 = sdp.F04
# left join (select * from STORESQL.dbo.RPT_ITM_D where F1034 = 3) d on prc.F01 = d.F01
# where rpc.F18 not in (21, 23) and d.F254 < prc.F137 and d.F254 > (prc.F137 - 90)
# group by prc.F01, obj.F29, obj.F155, rpc.F1024, sdp.F1022, prc.F30, prc.F136, prc.F137
# order by rpc.F1024, obj.F155, prc.F01
# """

query = """
select
t.UPC,
AVG(before_sale.F65) as 'Before Revenue',
AVG(before_sale.F64) as 'Before Volume',
AVG(before_sale.F1301) as 'Before COGS',
AVG(before_sale.F65)-AVG(before_sale.F1301) as 'Before Profit',
CASE WHEN sum(before_sale.F65) > 0 THEN (AVG(before_sale.F65)-AVG(before_sale.F1301))/AVG(before_sale.F65) ELSE 0 END as 'Before Margin',
t.[Sale Revenue], t.[Sale Volume], t.[Sale COGS], t.[Sale Profit], t.[Sale Margin],
t.[Dept], t.[Sub-Dept], t.[Desc], t.Brand, t.[Sale End Date], t.Price, t.[Type of Price],
t.[Sale Price], t.[Sale Start], t.[Sale End],
t.[Date Start], t.[Active Price], t.[Next Price Source], t.[Next Price]
from
(select
prc.F01 as 'UPC',
AVG(rpt_d.F65) as 'Sale Revenue',
AVG(rpt_d.F64) as 'Sale Volume',
AVG(rpt_d.F1301) as 'Sale COGS',
AVG(rpt_d.F65)-AVG(rpt_d.F1301) as 'Sale Profit',
CASE WHEN sum(rpt_d.F65) > 0 THEN (AVG(rpt_d.F65)-AVG(rpt_d.F1301))/AVG(rpt_d.F65) ELSE 0 END as 'Sale Margin', 
rpc.F1024 as 'Dept', sdp.F1022 as 'Sub-Dept', obj.F29 as 'Desc', obj.F155 as 'Brand', prc.F1014 as 'Sale End Date', prc.F30 as 'Price', prc.F113 as 'Type of Price',
prc.F136 as 'Sale Price', prc.F137 as 'Sale Start', prc.F138 as 'Sale End',
prc.F253 as 'Date Start', prc.F1007 as 'Active Price', prc.F1011 as 'Next Price Source', prc.F1013 as 'Next Price' 
from STORESQL.dbo.PRICE_TAB prc
inner join STORESQL.dbo.OBJ_TAB obj on prc.F01 = obj.F01
inner join STORESQL.dbo.POS_TAB pos on obj.F01 = pos.F01
inner join STORESQL.dbo.RPC_TAB rpc on obj.F18 = rpc.F18
inner join STORESQL.dbo.sdp_tab sdp on pos.F04 = sdp.F04
left join (select * from STORESQL.dbo.RPT_ITM_D where F1034 = 3) rpt_d on (prc.F01 = rpt_d.F01 and rpt_d.F254 >= prc.F137 and rpt_d.F254 < getdate())
where prc.F113 = 'SALE' and obj.F18 not in (21, 23) and prc.F1014 < '2022-11-04 00:00:00.000' and prc.F1208 is null
group by prc.F01, rpc.F1024, sdp.F1022, obj.F29, obj.F155, prc.F1014, prc.F30, prc.F113, prc.F136, prc.F137, prc.F1007, prc.F1011, prc.F1013, prc.F138, prc.F253) t
left join (select * from STORESQL.dbo.RPT_ITM_D where F1034 = 3) before_sale on (t.UPC = before_sale.F01 and before_sale.F254 < t.[Sale Start] and before_sale.F254 >= dateadd(day, -90, t.[Sale Start]))
group by t.UPC,t.[Sale Revenue], t.[Sale Volume], t.[Sale COGS], t.[Sale Profit], t.[Sale Margin],t.[Dept], t.[Sub-Dept], t.[Desc], t.Brand, t.[Sale End Date], t.Price, t.[Type of Price],
t.[Sale Price], t.[Sale Start], t.[Sale End],
t.[Date Start], t.[Active Price], t.[Next Price Source], t.[Next Price]
order by t.UPC
"""

# before = pd.read_sql(before_sale_start, cnxn)
# after = pd.read_sql(after_sale_start, cnxn)

kos = pd.read_sql(query, cnxn)
print(kos)
# print(before)
# print(after)

# combined = before.merge(after, how="left", on='UPC', suffixes=('_before', '_after'))

# combined['Avg Vol Diff'] = combined['AverageAfterVolume'] - combined['AverageBeforeVolume']
# combined['Avg Rev Diff'] = combined['AverageAfterRevenue'] - combined['AverageBeforeRevenue']
# combined.drop(['Description_after', 'Brand_after', 'Dept_after', 'Sub-Dept_after', 'Regular Price_after', 'Sale Price_after', 
# 'Sale Start_after', 'AverageBeforeVolume', 'AverageBeforeRevenue', 'AverageAfterVolume', 'AverageAfterRevenue'], axis=1)

# combined.rename(columns={'Description_after' : 'Description', 'Brand_after' : 'Brand', 'Dept_after' : 'Dept',
# 'Sub-Dept_after' : 'Sub-Dept', 'Regular Price_after' : 'Reg Price', 'Sale Price_after' : 'Sale Price', 
# 'Sale Start_after' : 'Sale Start', 'AverageBeforeVolume' : 'Before Sale Vol',
# 'AverageBeforeRevenue' : 'Before Sale Rev', 'AverageAfterVolume' : 'During Sale Vol', 'AverageAfterRevenue' : 'During Sale Rev'}, inplace=True)

# combined.insert(len(combined.columns), 'Keep On Sale', '')
# combined = combined.fillna('')
# combined = combined[['UPC', 'Description','Brand','Dept','Sub-Dept','Reg Price','Sale Price','Sale Start', 'Sale End','Keep On Sale', 'Before Sale Vol','Before Sale Rev','During Sale Vol', 'During Sale Rev','Avg Vol Diff', 'Avg Rev Diff']]


dept_list = kos['Dept'].unique().tolist()
print(dept_list)
dept_list.sort()
kos.insert(len(kos.columns), 'Keep On Sale', '')
kos['Profit Difference'] = kos['Sale Profit'] - kos['Before Profit']
kos = kos.fillna('')
kos = kos[['UPC', 'Desc','Brand','Dept','Sub-Dept',
        'Active Price', 'Next Price','Keep On Sale','Profit Difference',
        'Before Volume', 'Before Revenue', 'Before COGS', 'Before Profit', 'Before Margin',
        'Sale Start',
        'Sale Volume', 'Sale Revenue', 'Sale COGS', 'Sale Profit', 'Sale Margin',
        'Sale End']]

kos = kos.sort_values(['Brand', 'UPC'], ascending=[True, True])

headers = [["","","","","","","", "",
            "Before Sale Period","","", "", "",
            "",
            "During Sale Period","","", "", ""],
        ['UPC', 'Desc','Brand','Sub-Dept',
        'Sale Price', 'Next Price','Keep On Sale','Profit Difference',
        'Avg Volume', 'Avg Revenue', 'Avg COGS', 'Avg Profit', 'Avg Margin',
        'Sale Start',
        'Avg Volume','Avg Revenue', 'Avg COGS', 'Avg Profit', 'Avg Margin',
        'Sale End',
        ]]

SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
keep_on_sale_folder_id = config['DEFAULT']['keep_on_sale_folder']
sheet_creds = service_account.Credentials.from_service_account_file(
                    'sheet_credentials.json', scopes=SCOPES)
drive_creds = service_account.Credentials.from_service_account_file(
    'drive_credentials.json', scopes=SCOPES)

#https://googleapis.github.io/google-api-python-client/docs/dyn/drive_v3.html
drive = build('drive', 'v3', credentials=drive_creds)
#https://googleapis.github.io/google-api-python-client/docs/dyn/sheets_v4.html
sheets = build('sheets', 'v4', credentials=sheet_creds)

output_workbook = drive.files().create(
            body={
            'name' : 'Keep On Sale - Late October - 2022' ,
            'parents' : ['%s' % keep_on_sale_folder_id],
            'mimeType' : 'application/vnd.google-apps.spreadsheet'
            },
            fields='id').execute()

workbookId = output_workbook.get('id', '')
format_updates = []
for dept in dept_list:
    if(dept != ''):
        dept_data = kos[kos['Dept'] == dept]
        dept_data = dept_data.drop(['Dept'], axis=1)
        dept_data['Sale Start'] = dept_data['Sale Start'].dt.strftime('%m/%d/%y')
        dept_data['Sale End'] = dept_data['Sale End'].dt.strftime('%m/%d/%y')

        data = dept_data.values.tolist()
        title = dept.split(' ')[0]
        print(title)
        response = sheets.spreadsheets().batchUpdate(
            spreadsheetId = workbookId,
            body = {
                "requests": {
                    "addSheet":{
                        "properties": {
                        "title": title,
                        "gridProperties" : {  "rowCount": dept_data.shape[0]+2, "columnCount" : dept_data.shape[1], "frozenRowCount" : 2 , "frozenColumnCount" : 2}
                            }
                        }
            }}).execute()
        sheetId = [x for x in sheets.spreadsheets().get(spreadsheetId=workbookId).execute().get('sheets', '')
                                if x.get('properties', '').get('title', '') == title][0].get('properties', '').get('sheetId', '')
        #headers = dept_data.columns.tolist()
        result = sheets.spreadsheets().values().append(
            spreadsheetId=workbookId,
            range=title+"!A1:B1",
            body={ "majorDimension" : "ROWS", "values" : headers},
            valueInputOption="RAW"
                ).execute()
        #values
        result = sheets.spreadsheets().values().append(
            spreadsheetId=workbookId,
            range=title+"!A3:B3",
            body={ "majorDimension" : "ROWS", "values" : data},
            valueInputOption="RAW"
                ).execute()
        
        # response = sheets.spreadsheets().batchUpdate(
        #     spreadsheetId = workbookId,
        #     body = {"requests" : [
        format_updates.extend([{"repeatCell" :
        #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/other#GridRange
            {"range" :
                {"sheetId": sheetId,
                "startRowIndex": 0,
                "endRowIndex" : 2,
                "startColumnIndex": 8,
                "endColumnIndex": 13},
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
                    "backgroundColor" : {"red":255/255, "green":255/255, "blue":10/255, "alpha": 0.35},
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
                "startRowIndex": 0,
                "endRowIndex" : 2,
                "startColumnIndex": 14,
                "endColumnIndex": 19},
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
                    "backgroundColor" : {"red":10/255, "green":255/255, "blue":10/255, "alpha": 0.35},
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
            {   "mergeCells":{
                                    
                                "range" : {
                                    "sheetId" : sheetId,
                                    "startRowIndex": 0,
                                    "endRowIndex": 1,
                                    "startColumnIndex": 8,
                                    "endColumnIndex": 13
                                },
                                "mergeType" : "MERGE_ALL"
                            }
            },
            {   "mergeCells":{
                                    
                                "range" : {
                                    "sheetId" : sheetId,
                                    "startRowIndex": 0,
                                    "endRowIndex": 1,
                                    "startColumnIndex": 14,
                                    "endColumnIndex": 19
                                },
                                "mergeType" : "MERGE_ALL"
                            }
            },
            {"repeatCell" :
        #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/other#GridRange
            {"range" :
                {"sheetId": sheetId,
                "startRowIndex": 1,
                "endRowIndex" : 2,
                "startColumnIndex": 0,
                "endColumnIndex": 6},
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
                "startColumnIndex": 6,
                "endColumnIndex": 7},
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
                    "backgroundColor" : {"red":255/255, "green":100/255, "blue":25/255, "alpha": 0.35}
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
                "startColumnIndex": 7,
                "endColumnIndex": 8},
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
                "startColumnIndex": 13,
                "endColumnIndex": 14},
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
                "startColumnIndex": 19,
                "endColumnIndex": 20},
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
                "startColumnIndex": 0,
                "endColumnIndex": dept_data.shape[1]+1},
            #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/cells#CellData
            "cell": {
                "userEnteredFormat":{
                    "textFormat": { "fontFamily" : "Arial", "fontSize" : 11,}
                    },
                },
                #https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#google.protobuf.FieldMask
                #https://cloud.google.com/blog/products/application-development/formatting-cells-with-the-google-sheets-api
                "fields" : """
                            userEnteredFormat.textFormat.fontFamily,
                            userEnteredFormat.textFormat.fontSize"""
            }},
            {"repeatCell" :
        #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/other#GridRange
            {"range" :
                {"sheetId": sheetId,
                "startRowIndex": 2,
                "endRowIndex" : dept_data.shape[0]+2,
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
                "startRowIndex": 2,
                "endRowIndex" : dept_data.shape[0]+2,
                "startColumnIndex": dept_data.columns.get_loc("Before Revenue"),
                "endColumnIndex": dept_data.columns.get_loc("Before Margin")},
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
                "startRowIndex": 2,
                "endRowIndex" : dept_data.shape[0]+2,
                "startColumnIndex": dept_data.columns.get_loc("Active Price"),
                "endColumnIndex": dept_data.columns.get_loc("Keep On Sale")},
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
                "startRowIndex": 2,
                "endRowIndex" : dept_data.shape[0]+2,
                "startColumnIndex": dept_data.columns.get_loc("Sale Margin"),
                "endColumnIndex": dept_data.columns.get_loc("Sale Margin") + 1},
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
                "startRowIndex": 2,
                "endRowIndex" : dept_data.shape[0]+2,
                "startColumnIndex": dept_data.columns.get_loc("Before Margin"),
                "endColumnIndex": dept_data.columns.get_loc("Before Margin") + 1},
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
                "startRowIndex": 2,
                "endRowIndex" : dept_data.shape[0]+2,
                "startColumnIndex": dept_data.columns.get_loc("Profit Difference"),
                "endColumnIndex": dept_data.columns.get_loc("Profit Difference")+1},
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
                "startRowIndex": 2,
                "endRowIndex" : dept_data.shape[0]+2,
                "startColumnIndex": dept_data.columns.get_loc("Sale Revenue"),
                "endColumnIndex": dept_data.columns.get_loc("Sale Margin")},
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
                "startRowIndex": 2,
                "endRowIndex" : dept_data.shape[0]+2,
                "startColumnIndex": dept_data.columns.get_loc("Before Volume"),
                "endColumnIndex": dept_data.columns.get_loc("Before Volume")+1},
            #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/cells#CellData
            "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "NUMBER", "pattern" : "#,##0.00"}}},
                                    "fields" : """userEnteredFormat.numberFormat.type,
                                                    userEnteredFormat.numberFormat.pattern"""
            }
            },
            {"repeatCell" :
        #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/other#GridRange
            {"range" :
                {"sheetId": sheetId,
                "startRowIndex": 2,
                "endRowIndex" : dept_data.shape[0]+2,
                "startColumnIndex": dept_data.columns.get_loc("Sale Volume"),
                "endColumnIndex": dept_data.columns.get_loc("Sale Volume")+1},
            #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/cells#CellData
            "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "NUMBER", "pattern" : "#,##0.00"}}},
                                    "fields" : """userEnteredFormat.numberFormat.type,
                                                    userEnteredFormat.numberFormat.pattern"""
            }
            },
            {
                "repeatCell":{
                    "cell": {
                        "dataValidation": {
                            "condition": {"type": "BOOLEAN"}
                            }
                        },
                    "range": {
                        "sheetId": sheetId,
                        "startRowIndex": 2,
                        "endRowIndex": dept_data.shape[0] + 2,
                        "startColumnIndex": dept_data.columns.get_loc("Keep On Sale"),
                        "endColumnIndex": dept_data.columns.get_loc("Keep On Sale")+1
                        },
                    "fields": "dataValidation"
                }
            },
            {
                "addConditionalFormatRule":{
                    "rule":{
                        "ranges" :[{
                            "sheetId" : sheetId,
                            "startRowIndex": 2,
                            "endRowIndex" : dept_data.shape[0]+2,
                            "startColumnIndex": dept_data.columns.get_loc("Profit Difference"),
                            "endColumnIndex": dept_data.columns.get_loc("Profit Difference")+1
                            }],
                        "booleanRule" : {
                            "condition" : { "type" : "NUMBER_GREATER", "values" : [{"userEnteredValue" : "0"}]},
                            "format" : {"backgroundColor" : {"red":100/255, "green":248/255, "blue":50/255, "alpha": 0.05}}
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
                        "startRowIndex": 2,
                        "endRowIndex" : dept_data.shape[0]+2,
                        "startColumnIndex": dept_data.columns.get_loc("Profit Difference"),
                        "endColumnIndex": dept_data.columns.get_loc("Profit Difference")+1
                        }],
                    "booleanRule" : {
                        "condition" : { "type" : "NUMBER_LESS", "values" : [{"userEnteredValue" : "0"}]},
                        "format" : {"backgroundColor" : {"red":235/255, "green":75/255, "blue":50/255, "alpha": 0.05}}
                        }
                    },
                    "index" : 0
                }
            },
            {
            'setBasicFilter': {
                'filter': {
                    'range': {
                        "sheetId" : sheetId,
                        "startRowIndex" : 1,
                        "endRowIndex" : dept_data.shape[0]+2,
                        "startColumnIndex" : 0,
                        "endColumnIndex" : dept_data.shape[1]
                    }
                }
            }},
            {
            "autoResizeDimensions":{
                "dimensions" : {
                    "sheetId" : sheetId,
                    "dimension" : "COLUMNS",
                    "startIndex" : 0,
                    "endIndex" : dept_data.shape[0]+1
                    }
                }
            }
            # ,
            # {
            #     "addConditionalFormatRule":{
            #         "rule":{    
            #             "ranges" :[{
            #                 "sheetId" : sheetId,
            #                 "startRowIndex": 1,
            #                 "endRowIndex" : dept_data.shape[0]+1,
            #                 "startColumnIndex": dept_data.columns.get_loc("Avg Vol Diff"),
            #                 "endColumnIndex": dept_data.columns.get_loc("Avg Vol Diff")+1
            #                 }],
            #             "booleanRule" : {
            #                 "condition" : { "type" : "NUMBER_LESS", "values" : [{"userEnteredValue" : "0"}]},
            #                 "format" : {"backgroundColor" : {"red":225/255, "green":40/255, "blue":40/255, "alpha": 0.05}}
            #                 }
            #             },
            #             "index" : 0
            #         }
            # },
            # {
            #     "addConditionalFormatRule":{
            #         "rule":{    
            #             "ranges" :[{
            #                 "sheetId" : sheetId,
            #                 "startRowIndex": 1,
            #                 "endRowIndex" : dept_data.shape[0]+1,
            #                 "startColumnIndex": dept_data.columns.get_loc("Avg Vol Diff"),
            #                 "endColumnIndex": dept_data.columns.get_loc("Avg Vol Diff")+1
            #                 }],
            #             "booleanRule" : {
            #                 "condition" : { "type" : "NUMBER_GREATER", "values" : [{"userEnteredValue" : "0"}]},
            #                 "format" : {"backgroundColor" : {"red":50/255, "green":248/255, "blue":40/255, "alpha": 0.05}}
            #                 }
            #             },
            #             "index" : 0
            #         }
            # },
            # {
            #     "addConditionalFormatRule":{
            #         "rule":{    
            #             "ranges" :[{
            #                 "sheetId" : sheetId,
            #                 "startRowIndex": 1,
            #                 "endRowIndex" : dept_data.shape[0]+1,
            #                 "startColumnIndex": dept_data.columns.get_loc("Avg Rev Diff"),
            #                 "endColumnIndex": dept_data.columns.get_loc("Avg Rev Diff")+1
            #                 }],
            #             "booleanRule" : {
            #                 "condition" : { "type" : "NUMBER_LESS", "values" : [{"userEnteredValue" : "0"}]},
            #                 "format" : {"backgroundColor" : {"red":225/255, "green":40/255, "blue":40/255, "alpha": 0.05}}
            #                 }
            #             },
            #             "index" : 0
            #         }
            # },
            # {
            #     "addConditionalFormatRule":{
            #         "rule":{    
            #             "ranges" :[{
            #                 "sheetId" : sheetId,
            #                 "startRowIndex": 1,
            #                 "endRowIndex" : dept_data.shape[0]+1,
            #                 "startColumnIndex": dept_data.columns.get_loc("Avg Rev Diff"),
            #                 "endColumnIndex": dept_data.columns.get_loc("Avg Rev Diff")+1
            #                 }],
            #             "booleanRule" : {
            #                 "condition" : { "type" : "NUMBER_GREATER", "values" : [{"userEnteredValue" : "0"}]},
            #                 "format" : {"backgroundColor" : {"red":50/255, "green":248/255, "blue":40/255, "alpha": 0.05}}
            #                 }
            #             },
            #             "index" : 0
            #         }
            # }
            ])

            #}).execute()
        #time.sleep(7)
response = sheets.spreadsheets().batchUpdate(spreadsheetId = workbookId, body = {"requests": {"deleteSheet":{"sheetId" : 0}}}).execute()
response = sheets.spreadsheets().batchUpdate(spreadsheetId = workbookId, body = {"requests": format_updates}).execute()