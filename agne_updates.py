from sqlite3 import converters
import openpyxl
import xlrd
from pathlib import Path
import datetime
##https://pypi.org/project/pyodbc/
import pyodbc
#https://pandas.pydata.org/docs/reference/index.html
import pandas as pd
import os
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
#from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from datetime import date
import time
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

SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
agne_folder_id = config['DEFAULT']['agne_folder']
sheet_creds = service_account.Credentials.from_service_account_file('sheet_credentials.json', scopes=SCOPES)
drive_creds = service_account.Credentials.from_service_account_file('drive_credentials.json', scopes=SCOPES)

#https://googleapis.github.io/google-api-python-client/docs/dyn/drive_v3.html
drive = build('drive', 'v3', credentials=drive_creds)
#https://googleapis.github.io/google-api-python-client/docs/dyn/sheets_v4.html
sheets = build('sheets', 'v4', credentials=sheet_creds)


query = """select ot.F01 as 'Upc', rpc.F1024 as 'Department', sdp.F1022 as 'SubDept', ot.F22 as 'Size', ot.F29 as 'Description',
ot.F155 as 'Brand', c.F27 as 'Vendor ID', c.F19 as 'Case Size', c.F38 as 'Base Cost', c.F1184 as 'Buying Format',
c.F196 as 'Case Net Cost', c.F1140 as 'Unit Net Cost', prc.F30 as 'Current Price'
from STORESQL.dbo.OBJ_TAB ot
left join STORESQL.dbo.COST_TAB c on ot.F01 = c.F01
LEFT join STORESQL.dbo.RPC_TAB rpc on ot.F18 = rpc.F18
LEFT join STORESQL.dbo.POS_TAB pos on ot.F01 = pos.F01
LEFT join STORESQL.dbo.SDP_TAB sdp on pos.F04 = sdp.F04
left join STORESQL.dbo.PRICE_TAB prc on ot.F01 = prc.F01
where c.F27 = '0241'"""

#https://pandas.pydata.org/docs/reference/api/pandas.read_sql.html
agne = pd.read_sql(query, cnxn)

all_update_frames = []

#open every file in AGNE CSVs folder, then read each .xls into a pandas Dataframe, add that dataframe to the end of the all_update_frames list
print(os.path)
for file in os.listdir('AGNE CSVs/'):
    if file.endswith(".xls"):
        #https://pandas.pydata.org/docs/reference/api/pandas.read_excel.html
        #https://stackoverflow.com/questions/32591466/python-pandas-how-to-specify-data-types-when-reading-an-excel-file
        #it is important to ensure the UPCs are text here, otherwise it will drop the leading 0s, though if that is the case,
        #one could still use: https://pandas.pydata.org/docs/reference/api/pandas.Series.str.zfill.html, but it's much easier to ensure the text type of this column at import time
        all_update_frames.append(pd.read_excel('AGNE CSVs/%s' % file, engine='xlrd', converters={'Upc':str}))

#https://pandas.pydata.org/docs/reference/api/pandas.concat.html
combined = pd.concat(all_update_frames, ignore_index=True)

#https://stackoverflow.com/questions/37001787/remove-ends-of-string-entries-in-pandas-dataframe-column
combined['Upc'] = combined['Upc'].map(lambda x: str(x)[:-1])

#https://en.wikipedia.org/wiki/Enumerated_type
#!!!this is the real trick in this entire program!!!:
#   - build an ordered list of dates and remove duplicates according to it by transforming the "Order Date" column of DateTimes from AGNE into 'categories'/enumerables/enums/mutually distinct tags/whatever you want to call them
#       1. find all the unique "Order Date" dates in the different AGNE files
#       2. a simple sort() method will return them in ascending order which is what we want: [Day 1 < Day 2 < Day 3 < etc.]
#       3. transform this list into a pandas Categorical Series
ordered_dates = pd.Categorical(combined['Ord Date'], categories=combined['Ord Date'].unique().sort(), ordered=True)
#       4. sort all the AGNE items by this Categorical
#       5. drop all duplicate UPCs with a preference for the "largest" date
combined = combined.sort_values(["Upc", "Ord Date"]).drop_duplicates("Upc", keep="last")
#https://pandas.pydata.org/docs/reference/api/pandas.Categorical.html
#https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.sort_values.html
#https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.drop_duplicates.html



#https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.drop.html
#https://stackoverflow.com/questions/13411544/delete-a-column-from-a-pandas-dataframe
combined = combined.drop([
    'Cust','Invoice','Inv Date','Line','Item','Dpt','Pbh',
    'Ord Qty','Shp Qty','Allow','Ext Price','Srpk','Srp',
    'Ext Srp','Shp Date','Weight','Class','Nacs'], axis=1)

#this is just pandas terminology for what is essentially a SQL join operation
#https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.merge.html
merged = agne.merge(combined, how='inner', on='Upc', suffixes=('agne', 'bfc'))
#if the case costs between what is in SMS and the AGNE update are the same, drop those records 
merged['Update'] = (merged['Case Net Cost'] == merged['Price']).astype(bool)
updates = merged[merged['Update']==False]
updates = updates.drop(['Update'], axis=1)

#https://stackoverflow.com/questions/34023918/make-new-column-in-panda-dataframe-by-adding-values-from-other-columns
#calculate delta of margins
updates['Old Margin'] = ((updates['Current Price'] - updates['Unit Net Cost']) / updates['Current Price'])
updates['New Margin'] = ((updates['Current Price'] - (updates['Price']/updates['Case Size'])) / updates['Current Price'])
updates['Margin Diff'] = updates['New Margin'] - updates['Old Margin']  
updates = updates.drop([
    'Sizeagne','Descriptionagne','Vendor ID','Base Cost','Buying Format','Case Net Cost','Pack','Ord Date'
    ], axis=1)

updates['New Net Cost'] = updates['Price'] / updates['Case Size']

#https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.rename.html
updates.rename(columns={
    "Unit Net Cost" : "Current Net Cost",
    "Descriptionbfc" : "Description",
    "Upc" : "UPC", "SubDept" : "Sub-Dept"
}, inplace=True)



#build the Google Sheet we will be sending to the department managers, with the Drive API
#https://googleapis.github.io/google-api-python-client/docs/dyn/drive_v3.files.html#create
output_workbook = drive.files().create(body={
    'name' : 'AGNE Updates %s' % date.today(),
    'parents' : ['%s' % agne_folder_id],
    'mimeType' : 'application/vnd.google-apps.spreadsheet'
}, fields='id').execute()
#callback for Google API file ID lookups
workbookId = output_workbook.get('id', '')


response = sheets.spreadsheets().batchUpdate(
        spreadsheetId = workbookId,
        body = {
            "requests": {
                "addSheet":{
                    "properties": {
                    "title": "Raw Cost Changes",
                    "gridProperties" : { "frozenRowCount" : 1 }
                        }
                    }
        }}).execute()
raw_data = updates[['UPC', 'Case Size', 'Current Net Cost', 'Price', 'New Net Cost']]
headers = raw_data.columns.tolist()
result = sheets.spreadsheets().values().append(
    spreadsheetId=workbookId,
    range="Raw Cost Changes!A1:B1",
    body={ "majorDimension" : "ROWS", "values" : [headers]},
    valueInputOption="RAW"
        ).execute()
#values
result = sheets.spreadsheets().values().append(
    spreadsheetId=workbookId,
    range="Raw Cost Changes!A1:B1",
    body={ "majorDimension" : "ROWS", "values" : raw_data.to_numpy().tolist()},
    valueInputOption="RAW"
        ).execute()
rawSheetId = response.get('replies', '')[0].get('addSheet', '').get('properties', '').get('sheetId', '')
response = sheets.spreadsheets().batchUpdate(
        spreadsheetId = workbookId,
        body = {"requests" : {"repeatCell" :
            {"range" :
            {"sheetId": rawSheetId,
            "startRowIndex": 1,
            "endRowIndex" : raw_data.shape[0]+1,
            "startColumnIndex": raw_data.columns.get_loc("Current Net Cost"),
            "endColumnIndex": raw_data.shape[1]+1},
        "cell": {
            "userEnteredFormat":{"numberFormat": {"type": "NUMBER", "pattern" : "0.00" }}
            },
            "fields" : """userEnteredFormat.numberFormat.type,userEnteredFormat.numberFormat.pattern"""
        }}}).execute()
#we no longer need the AGNE updated case cost
updates.drop(['Price'], axis = 1)

#reorder Dataframe columns to match what we want in Google Sheets
#https://stackoverflow.com/questions/41968732/set-order-of-columns-in-pandas-dataframe
updates = updates[['UPC', 'Brand', 'Description', 'Department', 'Sub-Dept', 'Case Size', 'Current Net Cost', 'New Net Cost', 'Current Price', 'Old Margin', 'New Margin', 'Margin Diff']]

updates['Department'] = updates['Department'].apply(lambda x: x.split()[0])
#get the list of Department names we will be looping over and sort them
depts = updates['Department'].unique().tolist()
depts.sort()
updates = updates.sort_values(by=['Brand'], key=lambda col: col.str.lower()).sort_values(by=['UPC'], key= lambda col: col.str.lower())



#remove the trailing " Department" string from every department name in the list of departments
#https://www.w3schools.com/python/python_lists_comprehension.asp
#https://stackoverflow.com/questions/51314424/extracting-the-first-word-from-every-value-in-a-list
#depts = [x.split()[0] for x in depts]
i = 0
for dept in depts:
    i+=1
    #Set Sheet title property
    title = str(dept.split()[0])

    #filter the updates to just this department
    #not exactly but close, .where and .apply(lambda) would work just as well as mine: https://stackoverflow.com/questions/40134313/conditionally-calculated-column-for-a-pandas-dataframe
    dept_updates = updates[updates['Department']==dept]
    dept_updates = dept_updates.drop(['Department'], axis=1)
    dept_updates = dept_updates.sort_values(['Brand', 'UPC'], ascending=[True, True])
    dept_updates = dept_updates[dept_updates['Margin Diff'].abs() >= 0.03]
    #!!!the Dataframe must by transformed to a numpy array and then to a list for the Google API to successfully read it!!!
    data = dept_updates.to_numpy().tolist()
    
    #https://developers.google.com/sheets/api/samples/sheet
    response = sheets.spreadsheets().batchUpdate(
        spreadsheetId = workbookId,
        body = {
            "requests": {
                "addSheet":{
                    "properties": {
                    "title": title,
                    "gridProperties" : { "frozenRowCount" : 1 }
                        }
                    }
        }}).execute()
    #callback ref for formatting this sheet - most annoying part of the Google API is that these responses are structred as:
    #                   Dict{            List[    Dict{                 Dict{            Dict{...}       }}]}
    sheetId = response.get('replies', '')[0].get('addSheet', '').get('properties', '').get('sheetId', '')
    # or in other words, the 'replies' key in the API response is a list of dictionaries,
    # and most other forms of request and response with the API are dictionaries {key1 : value1, key2 : value2, ...}, not lists [value1, value2, ...]
    

    #####################
    ####DATA WRITING#####
    #####################
    headers = dept_updates.columns.tolist() + ['Updated Price', 'Updated Margin']
    result = sheets.spreadsheets().values().append(
        spreadsheetId=workbookId,
        range=title+"!A1:B1",
        body={ "majorDimension" : "ROWS", "values" : [headers]},
        valueInputOption="RAW"
            ).execute()
    #values
    result = sheets.spreadsheets().values().append(
        spreadsheetId=workbookId,
        range=title+"!A1:B1",
        body={ "majorDimension" : "ROWS", "values" : data},
        valueInputOption="RAW"
            ).execute()


    response = sheets.spreadsheets().batchUpdate(
        spreadsheetId = workbookId,
        body = {"requests" : [
            {"repeatCell" :
        #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/other#GridRange
            {"range" :
                {"sheetId": sheetId,
                "startRowIndex": 0,
                "endRowIndex" : 1,
                "startColumnIndex": 0,
                "endColumnIndex": dept_updates.shape[1]+2},
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
                "startColumnIndex": dept_updates.shape[1],
                "endColumnIndex": dept_updates.shape[1]+2},
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
                    #TEAL
                    #"backgroundColor" : {"red":39/255, "green":225/255, "blue":245/255, "alpha": 0.35}
                    "backgroundColor" : {"red":225/255, "green":225/255, "blue":0/255, "alpha": 0.35}
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
            "endRowIndex" : dept_updates.shape[0]+1,
            "startColumnIndex": 0,
            "endColumnIndex": dept_updates.shape[1]+2},
        #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/cells#CellData
        "cell": {
            "userEnteredFormat":{
                "borders":{
                    "top": {"style" : "SOLID"},
                    "bottom":{"style" : "SOLID"},
                    "left":{"style" : "SOLID"},
                    "right":{"style" : "SOLID"}
                    },
                "textFormat": { "fontFamily" : "Arial", "fontSize" : 12 }
                }
            },
            #https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#google.protobuf.FieldMask
            #https://cloud.google.com/blog/products/application-development/formatting-cells-with-the-google-sheets-api
            "fields" : """userEnteredFormat.borders.top,
                        userEnteredFormat.borders.bottom,
                        userEnteredFormat.borders.left,
                        userEnteredFormat.borders.right,
                        userEnteredFormat.textFormat.fontFamily,
                        userEnteredFormat.textFormat.fontSize"""
        }},
        {"repeatCell" :
        {"range" :
            {"sheetId": sheetId,
            "startRowIndex": 1,
            "endRowIndex" : dept_updates.shape[0]+1,
            #https://stackoverflow.com/questions/13021654/get-column-index-from-column-name-in-python-pandas
            "startColumnIndex": dept_updates.columns.get_loc("Old Margin"),
            "endColumnIndex": dept_updates.columns.get_loc("Margin Diff")+1},
        "cell": {
            "userEnteredFormat":{"numberFormat": { "type" : "PERCENT", "pattern" : "0.00%" }}
            },
            "fields" : """userEnteredFormat.numberFormat.type,userEnteredFormat.numberFormat.pattern"""
        }},
        {"repeatCell" :
        {"range" :
            {"sheetId": sheetId,
            "startRowIndex": 1,
            "endRowIndex" : dept_updates.shape[0]+1,
            "startColumnIndex": dept_updates.columns.get_loc("Current Net Cost"),
            "endColumnIndex": dept_updates.columns.get_loc("Current Price")},
        "cell": {
            "userEnteredFormat":{"numberFormat": {"type": "NUMBER", "pattern" : "0.00" }}
            },
            "fields" : """userEnteredFormat.numberFormat.type,userEnteredFormat.numberFormat.pattern"""
        }},
        {
                "addConditionalFormatRule":{
                    "rule":{
                        "ranges" :[{
                            "sheetId" : sheetId,
                            "startRowIndex": 1,
                            "endRowIndex" : dept_updates.shape[0]+1,
                            "startColumnIndex": dept_updates.columns.get_loc("Margin Diff"),
                            "endColumnIndex": dept_updates.columns.get_loc("Margin Diff")+1
                            }],
                        "booleanRule" : {
                            "condition" : { "type" : "NUMBER_GREATER", "values" : [{"userEnteredValue" : "0"}]},
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
                        "endRowIndex" : dept_updates.shape[0]+1,
                        "startColumnIndex": dept_updates.columns.get_loc("Margin Diff"),
                        "endColumnIndex": dept_updates.columns.get_loc("Margin Diff")+1
                        }],
                    "booleanRule" : {
                        "condition" : { "type" : "NUMBER_LESS", "values" : [{"userEnteredValue" : "0"}]},
                        "format" : {"backgroundColor" : {"red":225/255, "green":40/255, "blue":40/255, "alpha": 0.05}}
                        }
                    },
                    "index" : 0
                }
        },
        {
            "repeatCell":{  
                    "range" :{
                        "sheetId" : sheetId,
                        "startRowIndex": 1,
                        "endRowIndex" : dept_updates.shape[0]+1,
                        "startColumnIndex": dept_updates.columns.get_loc("Margin Diff")+2,
                        "endColumnIndex": dept_updates.columns.get_loc("Margin Diff")+3
                        },
                    "cell" : {
                        "userEnteredValue" : { "formulaValue" :  "=IF(ISBLANK(L2)=FALSE, (L2-G2)/L2, "")"},
                        "userEnteredFormat" : {"numberFormat" : {"type" : "PERCENT", "pattern" : "0.00%" }}
                        },
                    "fields" : """
                                userEnteredFormat.numberFormat.type,
                                userEnteredFormat.numberFormat.pattern,
                                userEnteredValue.formulaValue
                                """
        }},
        {
            "repeatCell":{  
                    "range" :{
                        "sheetId" : sheetId,
                        "startRowIndex": 1,
                        "endRowIndex" : dept_updates.shape[0]+1,
                        "startColumnIndex": dept_updates.columns.get_loc("Margin Diff")+1,
                        "endColumnIndex": dept_updates.columns.get_loc("Margin Diff")+2
                        },
                    "cell" : {
                        "userEnteredFormat" : {"numberFormat" : {"type" : "NUMBER", "pattern" : "0.00" }}
                        },
                    "fields" : """
                                userEnteredFormat.numberFormat.type,
                                userEnteredFormat.numberFormat.pattern
                                """
        }},
        {
        'setBasicFilter': {
            'filter': {
                'range': {
                    "sheetId" : sheetId,
                    "startRowIndex" : 0,
                    "endRowIndex" : dept_updates.shape[0]+1,
                    "startColumnIndex" : 0,
                    "endColumnIndex" : dept_updates.shape[1]+2
                }
            }
        }},
        ]
    }).execute()

    response = sheets.spreadsheets().batchUpdate(
        spreadsheetId = workbookId,
        body = {"requests" : [{
            "autoResizeDimensions":{
                "dimensions" : {
                    "sheetId" : sheetId,
                    "dimension" : "ROWS",
                    "startIndex" : 0,
                    "endIndex" : dept_updates.shape[0]+1
                    }
                }
        },
        {
            "autoResizeDimensions":{
                "dimensions" : {
                    "sheetId" : sheetId,
                    "dimension" : "COLUMNS",
                    "startIndex" : 0,
                    "endIndex" : dept_updates.shape[1]+2
                    }
                }
        }]}).execute()

    if(i != len(depts)): time.sleep(10)

response = sheets.spreadsheets().batchUpdate(spreadsheetId = workbookId, body = {"requests":{"deleteSheet":{"sheetId" : 0}}}).execute()