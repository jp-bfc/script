#SALE RETAIL == $0.00 means that the item should be "buy one, get one", handle this programmatically at some point.

import openpyxl
import tkinter as tk
from tkinter import filedialog
from pathlib import Path
import datetime
import pyodbc
import time
import pandas as pd
import numpy as np
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
#from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import configparser

root = tk.Tk()
root.withdraw()

file_path = filedialog.askopenfilename()

df = pd.read_csv(file_path, dtype='str', delimiter='\t')

#HEADERS IN THIS ORIGIN FILE SHOULD BE:
#|| #, Changes, Department, Brand, Supplier Item Code, Pack Size, Description, Quantity, Featured Item, Off-Shelf, Item Status, UPC Code, Flyer Period, Regular Case Cost, Event Case Cost, Sale Unit Cost, Sale Retail, MSRP


#filter out any duplicate header rows
#EANs have a length of 16 and UPCs have a length of 15, strip out -'s from each, drop last digit of only the UPCs

#lbmx = df[["UPC Code", "Description", "Flyer Period", "Sale Retail", "Event Case Cost", "Sale Unit Cost", 'Rebate Amount', 'Buy-In Promo Start Date', 'Buy-In Promo End Date', 'Co-op Basics Rebate', 'Promoted Price', 'Off-Shelf Rebate']]
lbmx = df[["UPC Code", "Description", "Flyer Period", "Sale Retail", "Event Case Cost",  'Buy-In Promo Start Date', 'Buy-In Promo End Date', "Sale Unit Cost", 'Rebate Amount', 'Co-op Basics Rebate', 'Promoted Price', 'Featured Item', 'Off-Shelf', 'Item Status', 'Extended Amount', 'Promo Discount', 'Off-Shelf Rebate', 'MCB']]

lbmx = lbmx.rename(columns={'Description' : 'Description_UNFI'})
lbmx = lbmx.reindex(columns = lbmx.columns.tolist() + ["Brand", "Dept", "Size", "Sub-Dept", "Current Price", "Current Unit", "Profit Diff", "Case Diff", "Current Case", 'Description'])

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
coop_deals_folder_id = config['DEFAULT']['coop_deals_folder']
sheet_creds = service_account.Credentials.from_service_account_file(
                    'sheet_credentials.json', scopes=SCOPES)
drive_creds = service_account.Credentials.from_service_account_file(
    'drive_credentials.json', scopes=SCOPES)

#https://googleapis.github.io/google-api-python-client/docs/dyn/drive_v3.html
drive = build('drive', 'v3', credentials=drive_creds)
#https://googleapis.github.io/google-api-python-client/docs/dyn/sheets_v4.html
sheets = build('sheets', 'v4', credentials=sheet_creds)


lbmx['EAN']=np.nan
lbmx['EAN']=lbmx['EAN'].astype('boolean')
lbmx['EXISTS']=np.nan
lbmx['EXISTS']=lbmx['EXISTS'].astype('boolean')

query="""
        SELECT OBJ.F01 as 'UPC', OBJ.F155 as 'Brand', CAST(cos.F19 AS FLOAT) as 'Size', rpc.F1024 as 'Department', sdp.F1022 as 'Sub_Dept',
        prc.F30 as 'Price', cos.F1140 as 'Current Unit', cos.F38 as 'Current Case', obj.F29 as 'Description'
        FROM  STORESQL.dbo.OBJ_TAB OBJ
        inner join STORESQL.dbo.POS_TAB pos on OBJ.F01 = pos.F01
        inner join STORESQL.dbo.RPC_TAB rpc on OBJ.F18 = rpc.F18
        left join STORESQL.dbo.COST_TAB cos on OBJ.F01 = cos.f01
        inner join STORESQL.dbo.sdp_tab sdp on pos.F04 = sdp.F04
        inner join STORESQL.dbo.PRICE_TAB prc on OBJ.F01 = prc.F01
        WHERE OBJ.F01 = ?
        Order by OBJ.F01
        """




for index, row in lbmx.iterrows():
    upc = ""
    if(len(row['UPC Code'].strip(' \t\n\r')) < 15):
            print ('op %s ' % len(row['UPC Code']) + row['UPC Code'])
    if(len(row['UPC Code'].strip(' \t\n\r')) > 16):
            print ('oops %s ' % len(row['UPC Code']) + row['UPC Code'])
    match len(row['UPC Code'].strip(' \t\n\r').replace("-", "")):
        case 12:
            upc = row['UPC Code'].strip(' \t\n\r').replace("-", "")
            upc = upc[0:len(upc)-1]
            upc = "00" + upc
            if upc in ['0018713200501','0018713200501','0018713200502','0018713200700','0018713200760','0018713200761','0018713200763','0084132010400','0084132010401']: print(upc)
            lbmx.at[index, 'EAN'] = False
        case 13:
            upc = row['UPC Code'].strip(' \t\n\r').replace("-", "")
            #print(upc)
            lbmx.at[index, 'EAN'] = True
    if(len(upc)>2):
        data = cursor.execute(query, str(upc)).fetchone()
        if upc in ['0018713200501','0018713200501','0018713200502','0018713200700','0018713200760','0018713200761','0018713200763','0084132010400','0084132010401']: print(upc, data)
        if(data!=None):
            lbmx.at[index, 'UPC Code'] = upc
            lbmx.at[index, 'Exists'] = True
            lbmx.at[index, 'Brand'] = data[1]
            lbmx.at[index, 'Dept'] = data[3]
            lbmx.at[index, 'Size'] = data[2]
            lbmx.at[index, 'Sub-Dept'] = data[4]
            lbmx.at[index, 'Current Price'] = data[5]
            lbmx.at[index, 'Current Unit'] = data[6]
            lbmx.at[index, 'Profit Diff'] = str((float(row['Sale Retail']) - float(row['Sale Unit Cost'])) - (float(data[5] if data[5] is not None else 0) - (float(data[6] if data[6] is not None else 0))))
            lbmx.at[index, 'Case Diff'] = str((float(data[7] if data[7] is not None else 0) - float(row['Event Case Cost'])))
            lbmx.at[index, "Current Case"] = data[7]
            lbmx.at[index, "Description"] = data[8]

lbmx = lbmx[lbmx['Exists']==True]
lbmx['EAN'] = lbmx['EAN'].fillna(value=False)
lbmx['Current Price'] = lbmx['Current Price'].astype(float)
lbmx['Current Unit'] = lbmx['Current Unit'].astype(float)
lbmx['Sale Retail'] = lbmx['Sale Retail'].astype(float)
lbmx['Sale Unit Cost'] = lbmx['Sale Unit Cost'].astype(float)
lbmx['Profit Diff'] = lbmx['Profit Diff'].astype(float)
lbmx['Case Diff'] = lbmx['Case Diff'].astype(float)
lbmx['Current Case'] = lbmx['Current Case'].astype(float)
lbmx['Rebate Amount'] = lbmx['Rebate Amount'].astype(float)
lbmx['Co-op Basics Rebate'] = lbmx['Co-op Basics Rebate'].astype(float)
lbmx['Off-Shelf Rebate'] = lbmx['Off-Shelf Rebate'].astype(float)
lbmx['Size'] = lbmx['Size'].astype(float)
lbmx['Promo Discount'] = lbmx['Promo Discount'].astype(float)
lbmx['Co-op Basics Rebate'] = lbmx['Co-op Basics Rebate'].fillna(0.0)
lbmx['Off-Shelf Rebate'] = lbmx['Off-Shelf Rebate'].fillna(0.0)
lbmx['Size'] = lbmx['Size'].fillna(1.0)
lbmx['Promo Discount'] = lbmx['Promo Discount'].fillna(0.0)

lbmx['Rebate Amount'] = lbmx['Rebate Amount'] + lbmx['Co-op Basics Rebate'] + lbmx['Off-Shelf Rebate']

lbmx['Full Unit Discount'] = (lbmx['Size'] * lbmx['Rebate Amount'])
#  + lbmx['Promo Discount'] + lbmx['MCB']
lbmx['Full Discount'] = lbmx['Case Diff'] + lbmx['Full Unit Discount']
lbmx['Calced Unit'] = (lbmx['Current Case'] - lbmx['Full Discount']) / lbmx['Size']
lbmx['Current Price'] = lbmx['Current Price'].apply(lambda x: "{:.2f}".format(x))
lbmx['Current Unit'] = lbmx['Current Unit'].apply(lambda x: "{:.2f}".format(x))
lbmx['Sale Retail'] = lbmx['Sale Retail'].apply(lambda x: "{:.2f}".format(x))
lbmx['Sale Unit Cost'] = lbmx['Sale Unit Cost'].apply(lambda x: "{:.2f}".format(x))
lbmx['Profit Diff'] = lbmx['Profit Diff'].apply(lambda x: "{:.2f}".format(x))
lbmx['Case Diff'] = lbmx['Case Diff'].apply(lambda x: "{:.2f}".format(x))
lbmx['Current Case'] = lbmx['Current Case'].apply(lambda x: "{:.2f}".format(x))
lbmx['Rebate Amount'] = lbmx['Rebate Amount'].apply(lambda x: "{:.2f}".format(x))
lbmx['Full Unit Discount'] = lbmx['Full Unit Discount'].apply(lambda x: "{:.2f}".format(x))
lbmx['Full Discount'] = lbmx['Full Discount'].apply(lambda x: "{:.2f}".format(x))
lbmx['Calced Unit'] = lbmx['Calced Unit'].apply(lambda x: "{:.2f}".format(x))
#lbmx['Rebate Amount'] = lbmx['Rebate Amount'].fillna(value=0.0)

#lbmx.to_excel('deals.xlsx')

lbmx = lbmx.drop(['EXISTS', 'Exists'], axis=1)

# TPR = lbmx[lbmx['Flyer Period'] == 'TPR']
# lbmx = lbmx[lbmx['Flyer Period'] != 'TPR']
EAN = lbmx[lbmx['EAN']==True]
UPCS = lbmx[lbmx['EAN']==False]

upc_A = UPCS[UPCS['Flyer Period']=='A']
upc_B = UPCS[UPCS['Flyer Period']=='B']
upc_AB = UPCS[UPCS['Flyer Period'].isin(['AB', 'TPR'])]
#upc_TPR = UPCS[UPCS['Flyer Period']=='TPR']

upc_A.name = 'A'
upc_B.name = 'B'
upc_AB.name = 'AB'
#upc_TPR.name = 'TPR'

ean_A = EAN[EAN['Flyer Period']=='A']
ean_B = EAN[EAN['Flyer Period']=='B']
ean_AB = EAN[EAN['Flyer Period'].isin(['AB', 'TPR'])]
#ean_TPR = EAN[EAN['Flyer Period']=='TPR']

ean_A.name = 'A-EAN'
ean_B.name = 'B-EAN'
ean_AB.name = 'AB-EAN'
#ean_TPR.name = 'TPR-EAN'

periodA = ['11/2', '11/15']
periodB = ['11/16', '11/29']





#These need separate treatment
# upc_TPR.insert(len(upc_TPR.columns), 'Sale Start Date', periodA[0])
# upc_TPR.insert(len(upc_TPR.columns), 'Sale End Date', periodB[1])
# ean_TPR.insert(len(ean_TPR.columns), 'Sale Start Date', periodA[0])
# ean_TPR.insert(len(ean_TPR.columns), 'Sale End Date', periodB[1])



upc_A.insert(len(upc_A.columns), 'Sale Start Date', periodA[0])
upc_A.insert(len(upc_A.columns), 'Sale End Date', periodA[1])
upc_B.insert(len(upc_B.columns), 'Sale Start Date', periodB[0])
upc_B.insert(len(upc_B.columns), 'Sale End Date', periodB[1])
upc_AB.insert(len(upc_AB.columns), 'Sale Start Date', periodA[0])
upc_AB.insert(len(upc_AB.columns), 'Sale End Date', periodB[1])


ean_A.insert(len(ean_A.columns), 'Sale Start Date', periodA[0])
ean_A.insert(len(ean_A.columns), 'Sale End Date', periodA[1])
ean_B.insert(len(ean_B.columns), 'Sale Start Date', periodB[0])
ean_B.insert(len(ean_B.columns), 'Sale End Date', periodB[1])
ean_AB.insert(len(ean_AB.columns), 'Sale Start Date', periodA[0])
ean_AB.insert(len(ean_AB.columns), 'Sale End Date', periodB[1])


# upc_A['Sale End Date'] = periodA[1]
# upc_B['Sale Start Date'] = periodB[0]
# upc_B['Sale End Date'] = periodB[1]
# upc_AB['Sale Start Date'] = periodA[0]
# upc_AB['Sale End Date'] = periodB[1]
# upc_TPR['Sale Start Date'] = periodA[0]
# upc_TPR['Sale End Date'] = periodB[1]

# ean_A['Sale Start Date'] = periodA[0]
# ean_A['Sale End Date'] = periodA[1]
# ean_B['Sale Start Date'] = periodB[0]
# ean_B['Sale End Date'] = periodB[1]
# ean_AB['Sale Start Date'] = periodA[0]
# ean_AB['Sale End Date'] = periodB[1]
# ean_TPR['Sale Start Date'] = periodA[0]
# ean_TPR['Sale End Date'] = periodB[1]

todays_date = datetime.datetime.today().date().strftime('%m%d%Y')

# upc_A.to_csv("Coop Deals\%s - import_UPC_batch_A.csv" % todays_date)
# upc_B.to_csv("Coop Deals\%s - import_UPC_batch_B.csv" % todays_date)
# upc_AB.to_csv("Coop Deals\%s - import_UPC_batch_AB.csv" % todays_date)
# #upc_TPR.to_csv("Coop Deals\%s - import_UPC_batch_TPR.csv" % todays_date)
# ean_A.to_csv("Coop Deals\%s - import_ean_batch_A.csv" % todays_date)
# ean_B.to_csv("Coop Deals\%s - import_ean_batch_B.csv" % todays_date)
# ean_AB.to_csv("Coop Deals\%s - import_ean_batch_AB.csv" % todays_date)
#ean_TPR.to_csv("Coop Deals\%s - import_ean_batch_TPR.csv" % todays_date)
frames = [frame for frame in (upc_A, upc_B, upc_AB, ean_A, ean_B, ean_AB) if frame.shape[0] > 0]

for frame in frames:
    standard_import = frame[frame['Sale Retail'] != '0.00']
    bogo_import = frame[frame['Sale Retail'] == '0.00']
    standard_import = standard_import[['UPC Code', 'Description', 'Description_UNFI', 'Flyer Period', 'Sale Retail', 'Current Price', 'Sale Start Date', 'Sale End Date']]
    bogo_import = bogo_import[['UPC Code', 'Description', 'Description_UNFI', 'Flyer Period', 'Sale Retail', 'Current Price', 'Sale Start Date', 'Sale End Date', 'Promoted Price']]
    if standard_import.shape[0]>0:
        standard_import.to_csv("Coop Deals\%s - import_standard_%s.csv" % (todays_date, frame.name), index=False)
    if bogo_import.shape[0]>0:
        bogo_import.to_csv("Coop Deals\%s - import_BOGO_%s.csv" % (todays_date, frame.name), index=False)  
    costs = frame[["UPC Code", "Description", 'Description_UNFI', "Flyer Period", "Size", 'Current Case', "Event Case Cost", 'Case Diff', "Sale Unit Cost", "Current Unit", 'Rebate Amount', 'Buy-In Promo Start Date', 'Buy-In Promo End Date', 'Full Unit Discount','Co-op Basics Rebate', 'Promoted Price', 'Off-Shelf Rebate', 'Promo Discount', 'MCB', 'Featured Item', 'Off-Shelf', 'Item Status', 'Extended Amount','Calced Unit', 'Full Discount']]
    if costs.shape[0]>0:
        costs.to_csv("Coop Deals\%s - import_COSTS_%s.csv" % (todays_date, frame.name), index=False)




a_period = pd.concat([upc_A, ean_A], ignore_index=True)
b_period = pd.concat([upc_B, ean_B], ignore_index=True)
#ab_period = pd.concat([pd.concat([pd.concat([upc_AB, ean_AB], ignore_index=True), upc_TPR], ignore_index=True), ean_TPR], ignore_index=True)
ab_period = pd.concat([upc_AB, ean_AB], ignore_index=True)

# print(a_period)
# a_period.rename(columns={ 'UPC Code' : 'UPC' }, inplace=True)
# b_period.rename(columns={ 'UPC Code' : 'UPC' }, inplace=True)
# ab_period.rename(columns={ 'UPC Code' : 'UPC' }, inplace=True)

a_period = a_period[['UPC Code', 'Brand', 'Description', 'Dept', 'Sub-Dept', 'Size', 'Current Price','Current Unit', 'Sale Retail',"Sale Unit Cost", 'Profit Diff', 'Rebate Amount']]
b_period = b_period[['UPC Code', 'Brand', 'Description', 'Dept', 'Sub-Dept', 'Size', 'Current Price','Current Unit', 'Sale Retail',"Sale Unit Cost", 'Profit Diff','Rebate Amount']]
ab_period = ab_period[['UPC Code', 'Brand', 'Description', 'Dept', 'Sub-Dept', 'Size', 'Current Price','Current Unit', 'Sale Retail',"Sale Unit Cost", 'Profit Diff', 'Rebate Amount']]

a_period = a_period.sort_values(['Brand', 'UPC Code'], ascending=[True, True])
b_period = b_period.sort_values(['Brand', 'UPC Code'], ascending=[True, True])
ab_period = ab_period.sort_values(['Brand', 'UPC Code'], ascending=[True, True])

a_period["Profit Diff"] = a_period["Profit Diff"].astype(str)
b_period["Profit Diff"] = b_period["Profit Diff"].astype(str)
ab_period["Profit Diff"] = ab_period["Profit Diff"].astype(str)
#TPR["Profit Diff"] = TPR["Profit Diff"].astype(str)

a_period["Current Price"] = a_period["Current Price"].astype(str)
b_period["Current Price"] = b_period["Current Price"].astype(str)
ab_period["Current Price"] = ab_period["Current Price"].astype(str)
#TPR["Current Price"] = TPR["Current Price"].astype(str)

a_period["Current Unit"] = a_period["Current Unit"].astype(str)
b_period["Current Unit"] = b_period["Current Unit"].astype(str)
ab_period["Current Unit"] = ab_period["Current Unit"].astype(str)
#TPR["Current Unit"] = TPR["Current Unit"].astype(str)

a_period["Sale Unit Cost"] = a_period["Sale Unit Cost"].astype(str)
b_period["Sale Unit Cost"] = b_period["Sale Unit Cost"].astype(str)
ab_period["Sale Unit Cost"] = ab_period["Sale Unit Cost"].astype(str)
#TPR["Sale Unit Cost"] = TPR["Sale Unit Cost"].astype(str)

a_period["Rebate Amount"] = a_period["Rebate Amount"].astype(str)
b_period["Rebate Amount"] = b_period["Rebate Amount"].astype(str)
ab_period["Rebate Amount"] = ab_period["Rebate Amount"].astype(str)
#TPR["Rebate Amount"] = TPR["Rebate Amount"].astype(str)


# a_period[["Sale Retail"]] = a_period[["Sale Retail"]].astype(str)
# b_period[["Sale Retail"]] = b_period[["Sale Retail"]].astype(str)
# ab_period[["Sale Retail"]] = ab_period[["Sale Retail"]].astype(str)
# TPR[["Sale Retail"]] = TPR[["Sale Retail"]].astype(str)
# b_period = b_period.astype({"Diff" : str})
# ab_period = ab_period.astype({"Diff" : str})
# print(a_period.dtypes)
# print("#########################")
a_period.name = "A Period %s - %s" % (periodA[0], periodA[1])
b_period.name = "B Period %s - %s" % (periodB[0], periodB[1])
ab_period.name = "AB Period %s - %s" % (periodA[0], periodB[1])


sheet_frames = [a_period, b_period, ab_period]
headers = ['UPC', 'Brand', 'Description', 'Sub-Dept', 'Size', 'Current Price','Current Unit', 'Sale Retail',"Sale Unit Cost", 'Profit Diff', 'Unit Rebate']
for sheet_frame in sheet_frames:
    file_name = 'Coop Deals - November %s' % sheet_frame.name
    departments = sheet_frame['Dept'].unique().tolist()
    departments.sort()
    output_workbook = drive.files().create(
            body={
            'name' : file_name ,
            'parents' : ['%s' % coop_deals_folder_id],
            'mimeType' : 'application/vnd.google-apps.spreadsheet'
            },
            fields='id').execute()
    format_updates = []
    workbookId = output_workbook.get('id', '')
    for dept in departments:
        this_department = sheet_frame[sheet_frame['Dept'] == dept]
        this_department = this_department.drop(['Dept'], axis=1)
        this_department.loc[this_department['Sale Retail'] == '0.00', 'Sale Retail'] = "BOGO"
        this_department.loc[this_department['Sale Retail'] == 'BOGO', 'Profit Diff'] = ""
        # if(dept == "Supplements Department" or dept == "Housewares Department" or dept == "Haba Department" or dept == "CBM Department"):
        #     wellness_writer = pd.ExcelWriter(file_path, mode='a', if_sheet_exists='overlay', engine='openpyxl')
        #     this_department.to_excel(wellness_writer, sheet_name="Wellness")
        #     wellness_writer.save()
        # else:
        sheet_name = dept if dept == "Taxable Bulk" or dept == "Taxable Grocery" else dept.split()[0]
        response = sheets.spreadsheets().batchUpdate(
            spreadsheetId = workbookId,
            body = {
                "requests":{
                    "addSheet":{
                                
                            "properties" : {
                                "title": sheet_name,
                                "gridProperties" : {
                                    "columnCount" : len(headers),
                                    "rowCount" : this_department.shape[0]+1,
                                    "frozenRowCount" : 1,
                                    "frozenColumnCount" : 0
                                }
                            },
                        }
                    }
                }).execute()

        sheetId = response.get('replies', '')[0].get('addSheet', '').get('properties', '').get('sheetId', '')
        

        result = sheets.spreadsheets().values().append(
                spreadsheetId=workbookId,
                range="%s!A1" % sheet_name,
                body={ "majorDimension" : "ROWS", "values" : [headers]},
                valueInputOption="RAW"
                    ).execute()
        
        result = sheets.spreadsheets().values().append(
                spreadsheetId=workbookId,
                range="%s!A2" % sheet_name,
                body={ "majorDimension" : "ROWS", "values" : this_department.values.tolist()},
                valueInputOption="RAW"
                    ).execute()

        # response = sheets.spreadsheets().batchUpdate(
        # spreadsheetId = workbookId,
        # body = {"requests" :
        format_updates.extend([
                    {"repeatCell" :
                            #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/other#GridRange
                        {"range" :
                            {"sheetId": sheetId,
                            "startRowIndex": 0,
                            "endRowIndex" : 1,
                            "startColumnIndex": 0,
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
                            "startRowIndex": 0,
                            "endRowIndex" : 1,
                            "startColumnIndex": 5,
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
                                    "backgroundColor" : {"red":255/255, "green":255/255, "blue":50/255, "alpha": 0.35}
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
                            "startRowIndex": 0,
                            "endRowIndex" : 1,
                            "startColumnIndex": 7,
                            "endColumnIndex": 9},
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
                                    "backgroundColor" : {"red":75/255, "green":255/255, "blue":50/255, "alpha": 0.35}
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
                            "startRowIndex": 0,
                            "endRowIndex" : 1,
                            "startColumnIndex": 9,
                            "endColumnIndex": len(headers)+1},
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
            "endRowIndex" : this_department.shape[0]+1,
            "startColumnIndex": 0,
            "endColumnIndex": len(headers) + 1},
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
            {
                "range" :
                    {
                        "sheetId": sheetId,
                        "startRowIndex": 1,
                        "endRowIndex" : this_department.shape[0]+1,
                        "startColumnIndex": this_department.columns.get_loc("Current Price"),
                        "endColumnIndex": this_department.columns.get_loc("Rebate Amount")+1
                    },
                "cell":
                    {
                    "userEnteredFormat":
                        {
                            "numberFormat": 
                                {
                                    "type": "CURRENCY",
                                    "pattern" : "$#,##0.00"
                                }
                        }
                    },
                "fields" : """userEnteredFormat.numberFormat.type, userEnteredFormat.numberFormat.pattern"""
            }
        },
        {
                            'setBasicFilter': {
                                'filter': {
                                    'range': {
                                        "sheetId" : sheetId,
                                        "startRowIndex" : 0,
                                        "endRowIndex" : this_department.shape[0]+1,
                                        "startColumnIndex" : 0,
                                        "endColumnIndex" : this_department.shape[1]
                                    }
                                }
                            }},
                            {
                "autoResizeDimensions":{
                    "dimensions" : {
                        "sheetId" : sheetId,
                        "dimension" : "COLUMNS",
                        "startIndex" : 0,
                        "endIndex" : this_department.shape[0]+1
                    }
                }}
        ])
        #         }).execute()
        # response = sheets.spreadsheets().batchUpdate(
        #     spreadsheetId = workbookId,
        #     body = 
        #     }])
        time.sleep(3)
    
    format_updates.append({"deleteSheet":{"sheetId" : 0}})
    
    response = sheets.spreadsheets().batchUpdate(spreadsheetId = workbookId, body = {"requests": format_updates}).execute()
    #time.sleep(7)











# #Standard and BOGO TPR imports need manager feedback, so unnecessary during IT processing
# # standard_TPR_import = TPR[TPR['Sale Retail'] != '0.00']
# bogo_TPR_import = TPR[TPR['Sale Retail'] == '0.00']
# # standard_TPR_import = standard_TPR_import[['UPC Code', 'Description', 'Flyer Period', 'Sale Retail', 'Current Price', 'Sale Start Date', 'Sale End Date']]
# bogo_TPR_import = bogo_TPR_import[['UPC Code', 'Description', 'Flyer Period', 'Sale Retail', 'Current Price', 'Promoted Price']]
# # if standard_TPR_import.shape[0]>0:
# #     standard_TPR_import.to_csv("Coop Deals\%s - import_standard_TPR.csv" % todays_date, index=False)
# if bogo_TPR_import.shape[0]>0:
#         bogo_TPR_import.to_csv("Coop Deals\%s - import_BOGO_TPR.csv" % todays_date, index=False)  
# costs_TPR = TPR[["UPC Code", "Description", 'Description_UNFI', "Flyer Period", "Size", 'Current Case', "Event Case Cost", 'Case Diff', "Sale Unit Cost", "Current Unit", 'Rebate Amount', 'Buy-In Promo Start Date', 'Buy-In Promo End Date', 'Full Unit Discount', 'Co-op Basics Rebate', 'Promoted Price', 'Off-Shelf Rebate']]
# if costs_TPR.shape[0]>0:
#     costs_TPR.to_csv("Coop Deals\%s - import_COSTS_TPR.csv" % todays_date, index=False)

# TPR = TPR[['UPC Code', 'Brand', 'Description', 'Dept', 'Sub-Dept', 'Size', 'Current Price','Current Unit', 'Buy-In Promo Start Date', 'Buy-In Promo End Date','Sale Retail',"Sale Unit Cost", 'Profit Diff', 'Rebate Amount']]

# headers = ['UPC', 'Brand', 'Description', 'Sub-Dept', 'Size', 'Current Price','Current Unit', 'Buy-In Promo Start Date', 'Buy-In Promo End Date', 'Sale Retail',"Sale Unit Cost", 'Profit Diff', 'Unit Rebate','Sale Period Begin', 'Sale Period End']

# file_name = 'Coop Deals - September TPR'
# departments = TPR['Dept'].unique().tolist()
# departments.sort()
# output_workbook = drive.files().create(
#         body={
#         'name' : file_name ,
#         'parents' : ['%s' % coop_deals_folder_id],
#         'mimeType' : 'application/vnd.google-apps.spreadsheet'
#         },
#         fields='id').execute()

# workbookId = output_workbook.get('id', '')

# for dept in departments:
#     this_department = TPR[TPR['Dept'] == dept]
#     this_department = this_department.drop(['Dept'], axis=1)
#     this_department.loc[this_department['Sale Retail'] == '0.00', 'Sale Retail'] = "BOGO"
#     this_department.loc[this_department['Sale Retail'] == 'BOGO', 'Profit Diff'] = ""
#     sheet_name = dept if dept == "Taxable Bulk" or dept == "Taxable Grocery" else dept.split()[0]
#     response = sheets.spreadsheets().batchUpdate(
#         spreadsheetId = workbookId,
#         body = {
#             "requests":{
#                 "addSheet":{
                            
#                         "properties" : {
#                             "title": sheet_name,
#                             "gridProperties" : {
#                                 "columnCount" : len(headers),
#                                 "rowCount" : this_department.shape[0]+2,
#                                 "frozenRowCount" : 1,
#                                 "frozenColumnCount" : 0
#                             }
#                         },
#                     }
#                 }
#             }).execute()

#     sheetId = response.get('replies', '')[0].get('addSheet', '').get('properties', '').get('sheetId', '')
    

#     result = sheets.spreadsheets().values().append(
#             spreadsheetId=workbookId,
#             range="%s!A1" % sheet_name,
#             body={ "majorDimension" : "ROWS", "values" : [headers]},
#             valueInputOption="RAW"
#                 ).execute()
#     result = sheets.spreadsheets().values().append(
#             spreadsheetId=workbookId,
#             range="%s!A2" % sheet_name,
#             body={ "majorDimension" : "ROWS", "values" : this_department.values.tolist()},
#             valueInputOption="RAW"
#                 ).execute()

#     response = sheets.spreadsheets().batchUpdate(
#     spreadsheetId = workbookId,
#     body = {"requests" :[
#                 {"repeatCell" :
#                         #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/other#GridRange
#                     {"range" :
#                         {"sheetId": sheetId,
#                         "startRowIndex": 0,
#                         "endRowIndex" : 1,
#                         "startColumnIndex": 0,
#                         "endColumnIndex": len(headers) - 2},
#                         #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/cells#CellData
#                         "cell": {
#                             "userEnteredFormat":{
#                                 "borders":{
#                                     "top": {"style" : "SOLID_THICK"},
#                                     "bottom":{"style" : "SOLID_THICK"},
#                                     "left":{"style" : "SOLID_THICK"},
#                                     "right":{"style" : "SOLID_THICK"}
#                                     },
#                                 "textFormat": { "fontFamily" : "Arial", "fontSize" : 14, "bold" : True },
#                                 #https://rgbacolorpicker.com/
#                                 "backgroundColor" : {"red":39/255, "green":175/255, "blue":245/255, "alpha": 0.35}
#                                 },
#                             },
#                         #https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#google.protobuf.FieldMask
#                         #https://cloud.google.com/blog/products/application-development/formatting-cells-with-the-google-sheets-api
#                         "fields" : """userEnteredFormat.textFormat.bold,
#                                     userEnteredFormat.backgroundColor.red,
#                                     userEnteredFormat.backgroundColor.green,
#                                     userEnteredFormat.backgroundColor.blue,
#                                     userEnteredFormat.backgroundColor.alpha,
#                                     userEnteredFormat.textFormat.fontFamily,
#                                     userEnteredFormat.textFormat.fontSize,
#                                     userEnteredFormat.borders.top,
#                                     userEnteredFormat.borders.bottom,
#                                     userEnteredFormat.borders.left,
#                                     userEnteredFormat.borders.right"""
#                                 }
#                 },
#                 {"repeatCell":
#                     {"range" :
#                             {"sheetId": sheetId,
#                             "startRowIndex": 0,
#                             "endRowIndex" : 1,
#                             "startColumnIndex": len(headers)-2,
#                             "endColumnIndex": len(headers) + 1},
#                             #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/cells#CellData
#                             "cell": {
#                                 "userEnteredFormat":{
#                                     "borders":{
#                                         "top": {"style" : "SOLID_THICK"},
#                                         "bottom":{"style" : "SOLID_THICK"},
#                                         "left":{"style" : "SOLID_THICK"},
#                                         "right":{"style" : "SOLID_THICK"}
#                                         },
#                                     "textFormat": { "fontFamily" : "Arial", "fontSize" : 14, "bold" : True },
#                                     #https://rgbacolorpicker.com/
#                                     "backgroundColor" : {"red":245/255, "green":175/255, "blue":25/255, "alpha": 0.35}
#                                     },
#                                 },
#                             #https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#google.protobuf.FieldMask
#                             #https://cloud.google.com/blog/products/application-development/formatting-cells-with-the-google-sheets-api
#                             "fields" : """userEnteredFormat.textFormat.bold,
#                                         userEnteredFormat.backgroundColor.red,
#                                         userEnteredFormat.backgroundColor.green,
#                                         userEnteredFormat.backgroundColor.blue,
#                                         userEnteredFormat.backgroundColor.alpha,
#                                         userEnteredFormat.textFormat.fontFamily,
#                                         userEnteredFormat.textFormat.fontSize,
#                                         userEnteredFormat.borders.top,
#                                         userEnteredFormat.borders.bottom,
#                                         userEnteredFormat.borders.left,
#                                         userEnteredFormat.borders.right"""
#                 }},    
#                 {"repeatCell" :
# #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/other#GridRange
#     {"range" :
#         {"sheetId": sheetId,
#         "startRowIndex": 1,
#         "endRowIndex" : this_department.shape[0]+1,
#         "startColumnIndex": 0,
#         "endColumnIndex": len(headers) + 1},
#     #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/cells#CellData
#     "cell": {
#         "userEnteredFormat":{
#             "borders":{
#                 "top": {"style" : "SOLID"},
#                 "bottom":{"style" : "SOLID"},
#                 "left":{"style" : "SOLID"},
#                 "right":{"style" : "SOLID"}
#                 },
#             "textFormat": { "fontFamily" : "Arial", "fontSize" : 12 }
#             }
#         },
#         #https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#google.protobuf.FieldMask
#         #https://cloud.google.com/blog/products/application-development/formatting-cells-with-the-google-sheets-api
#         "fields" : """userEnteredFormat.borders.top,
#                     userEnteredFormat.borders.bottom,
#                     userEnteredFormat.borders.left,
#                     userEnteredFormat.borders.right,
#                     userEnteredFormat.textFormat.fontFamily,
#                     userEnteredFormat.textFormat.fontSize"""
#     }},
#     {"repeatCell" :
#         {"range" :
#             {"sheetId": sheetId,
#             "startRowIndex": 1,
#             "endRowIndex" : this_department.shape[0]+1,
#             "startColumnIndex": this_department.columns.get_loc("Current Price"),
#             "endColumnIndex": this_department.columns.get_loc("Current Unit")+1
#             },
#         "cell": {
#             "userEnteredFormat":{"numberFormat": {"type": "CURRENCY", "pattern" : "$#,##0.00" }}
#             },
#             "fields" : """userEnteredFormat.numberFormat.type, userEnteredFormat.numberFormat.pattern"""
#     }},
#     {"repeatCell" :
#         {"range" :
#             {"sheetId": sheetId,
#             "startRowIndex": 1,
#             "endRowIndex" : this_department.shape[0]+1,
#             "startColumnIndex": this_department.columns.get_loc("Sale Retail"),
#             "endColumnIndex": this_department.columns.get_loc("Rebate Amount")+1
#             },
#         "cell": {
#             "userEnteredFormat":{"numberFormat": {"type": "CURRENCY", "pattern" : "$#,##0.00" }}
#             },
#             "fields" : """userEnteredFormat.numberFormat.type, userEnteredFormat.numberFormat.pattern"""
#         }
#     },
#     {"repeatCell" :
#         {"range" :
#             {"sheetId": sheetId,
#             "startRowIndex": 1,
#             "endRowIndex" : this_department.shape[0]+1,
#             "startColumnIndex": this_department.columns.get_loc("Buy-In Promo Start Date"),
#             "endColumnIndex": this_department.columns.get_loc("Buy-In Promo Start Date")+1},
#         "cell": {
#             "userEnteredFormat":{"numberFormat": {"type": "DATE", "pattern" : "mm/dd/yyyy" }}
#             },
#             "fields" : """userEnteredFormat.numberFormat.type,userEnteredFormat.numberFormat.pattern"""
#         }
#     },
#     {"repeatCell" :
#         {"range" :
#             {"sheetId": sheetId,
#             "startRowIndex": 1,
#             "endRowIndex" : this_department.shape[0]+1,
#             "startColumnIndex": headers.index("Sale Period Begin"),
#             "endColumnIndex": headers.index("Sale Period End")+1},
#         "cell": {
#             "userEnteredFormat":{"numberFormat": {"type": "DATE", "pattern" : "mm/dd/yyyy" }}
#             },
#             "fields" : """userEnteredFormat.numberFormat.type,userEnteredFormat.numberFormat.pattern"""
#         }
#     },
#         {
#                             'setBasicFilter': {
#                                 'filter': {
#                                     'range': {
#                                         "sheetId" : sheetId,
#                                         "startRowIndex" : 0,
#                                         "endRowIndex" : this_department.shape[0]+1,
#                                         "startColumnIndex" : 0,
#                                         "endColumnIndex" : this_department.shape[1]+3
#                                     }
#                                 }
#                             }}
#     ]
#             }).execute()
    
#     response = sheets.spreadsheets().batchUpdate(
#     spreadsheetId = workbookId,
#     body = {"requests": [{
#         "autoResizeDimensions":{
#             "dimensions" : {
#                 "sheetId" : sheetId,
#                 "dimension" : "COLUMNS",
#                 "startIndex" : 0,
#                 "endIndex" : this_department.shape[0]+1
#             }
#         }
#     }]}).execute()
#     time.sleep(3)
