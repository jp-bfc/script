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

config = configparser.ConfigParser()
config.read('script_configs.ini')

SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
sheet_creds = service_account.Credentials.from_service_account_file(
                    'sheet_credentials.json', scopes=SCOPES)
drive_creds = service_account.Credentials.from_service_account_file(
    'drive_credentials.json', scopes=SCOPES)

#https://googleapis.github.io/google-api-python-client/docs/dyn/drive_v3.html
drive = build('drive', 'v3', credentials=drive_creds)
#https://googleapis.github.io/google-api-python-client/docs/dyn/sheets_v4.html
sheets = build('sheets', 'v4', credentials=sheet_creds)

import_choice = input("What type of file are you trying to import into SMS? [AGNE, KOS, UNFI]: ")

match import_choice:
    case "AGNE":
        agne_folder_id = config['DEFAULT']['agne_folder']

        file_name = input("Which period of AGNE Price Updates are you grabbing? ")
        response = drive.files().list(q="parents in '%s'" % agne_folder_id,
                                        spaces='drive',
                                        fields='nextPageToken, files(id, name)',
                                        pageToken=None).execute()

        workbook = [x for x in response['files'] if file_name in x['name']][0]['id']

        headers = ['Dept', 'UPC','Brand', 'Description','Sub-Dept','Case Size','Current Net Cost',
                    'New Net Cost','Current Price','Old Margin','New Margin','Margin Diff',
                    'Updated Price','Updated Margin']
        #custom_headers = ['Dept', 'UPC','Brand','Description','Size','Sub dept','Vnd ID','Vnd Code','Case Cost','Case Size','Unit Cost','Price','Margin','Updated Price','Updated Margin']
        update_list = []
        for sh in sheets.spreadsheets().get(spreadsheetId=workbook).execute()['sheets']:
            sheetId = sh['properties']['sheetId']
            sheet_name = sh['properties']['title']
            print(sheet_name)
            result = sheets.spreadsheets().values().get(
                    spreadsheetId=workbook, range=sheet_name + '!A2:N').execute()
            rows = result.get('values', [])
            for x in rows: x.insert(0, sheet_name.split()[0])
            update_list.extend(rows)
        print(update_list)
        df = pd.DataFrame(update_list, columns=headers)
        df = df.fillna("")
        df = df[df['Updated Price'] != '']
        df = df[['Dept','UPC', 'Updated Price']]
        df['Updated Price'] = df['Updated Price'].str.replace('$', '', regex=False)
        df['Updated Price'] = df['Updated Price'].astype(float)
        #https://stackoverflow.com/questions/35019156/pandas-format-column-as-currency
        #https://realpython.com/python-formatted-output/#the-string-format-method-arguments
        df['Updated Price'] = df['Updated Price'].apply(lambda x: "{:.2f}".format(x))
        print(df)
        df.to_csv('\\\SMSSERV001901\storeman\XchFile\Jon Imports\AGNE %s Update Import.csv' % file_name, index=False)
    case "KOS":
        keep_on_sale_folder_id = config['DEFAULT']['keep_on_sale_folder']

        file_name = input("Which period of KOS are you grabbing? ")
        #https://stackoverflow.com/questions/15226898/python-3-2-input-date-function
        user_date = input("What will be the end of the sale period? (MM-DD format): ")
        month, day = map(int, user_date.split('-'))
        next_date = datetime.date(datetime.datetime.today().year, month, day).strftime('%x')
        response = drive.files().list(q="parents in '%s'" % keep_on_sale_folder_id,
                                        spaces='drive',
                                        fields='nextPageToken, files(id, name)',
                                        pageToken=None).execute()

        workbook = [x for x in response['files'] if file_name in x['name']][0]['id']

        headers = ['UPC','Desc','Brand','Sub-Dept','Sale Price','Next Price','Keep On Sale','Profit Difference','Normal Volume','Normal Revenue','Normal COGS',
            'Normal Profit','Normal Margin','Sale Start','Avg Volume','Avg Revenu',	'Avg COGS',	'Avg Profit','Avg Margin','Sale End']

        update_list = []
        for sh in sheets.spreadsheets().get(spreadsheetId=workbook).execute()['sheets']:
            sheetId = sh['properties']['sheetId']
            sheet_name = sh['properties']['title']
            
            result = sheets.spreadsheets().values().get(
                    spreadsheetId=workbook, range=sheet_name + '!A3:T').execute()
            rows = result.get('values', [])
            update_list.extend(rows)

        df = pd.DataFrame(update_list, columns=headers)
        df = df[df['Keep On Sale'] == 'TRUE']
        df = df[['UPC', 'Sale Price', 'Sale Start']]
        df['Sale Price'] = df['Sale Price'].str.replace('$', '', regex=False)
        df['Sale Price'] = df['Sale Price'].astype(float)
        #https://stackoverflow.com/questions/35019156/pandas-format-column-as-currency
        #https://realpython.com/python-formatted-output/#the-string-format-method-arguments
        df['Sale Price'] = df['Sale Price'].apply(lambda x: "{:.2f}".format(x))
        df['Sale End Date'] = next_date

        df.to_csv(r'\\SMSSERV001901\storeman\XchFile\Jon Imports\KOS %s Update Import.csv' % file_name, index=False)

    case "UNFI":
        UNFI_folder_id = config['DEFAULT']['unfi_folder']

        file_name = input("Which period of UNFI Price Updates are you grabbing? ")
        response = drive.files().list(q="parents in '%s'" % UNFI_folder_id,
                                        spaces='drive',
                                        fields='nextPageToken, files(id, name)',
                                        pageToken=None).execute()

        workbook = [x for x in response['files'] if file_name in x['name']][0]['id']

        headers = ['Dept', 'UPC','Brand', 'Description','Sub-Dept','Case Size','Current Unit $',
                    'New Net $','Current Price','Old Margin','New Margin','Margin Diff',
                    'Updated Price','Updated Margin']

        update_list = []
        for sh in sheets.spreadsheets().get(spreadsheetId=workbook).execute()['sheets']:
            sheetId = sh['properties']['sheetId']
            sheet_name = sh['properties']['title']
            if sheet_name.split()[0] != "Raw":
                result = sheets.spreadsheets().values().get(
                        spreadsheetId=workbook, range=sheet_name + '!A2:M').execute()
                rows = result.get('values', [])
                for x in rows: x.insert(0, sheet_name.split()[0])
                update_list.extend(rows)

        df = pd.DataFrame(update_list, columns=headers)
        df = df.fillna("")
        df = df[df['Updated Price'] != '']
        df = df[df['Updated Price'] != " "]
        df = df[['Dept','UPC', 'Updated Price']]
        df['Updated Price'] = df['Updated Price'].str.replace('$', '', regex=False)
        df['Updated Price'] = df['Updated Price'].astype(float)
        #https://stackoverflow.com/questions/35019156/pandas-format-column-as-currency
        #https://realpython.com/python-formatted-output/#the-string-format-method-arguments
        df['Updated Price'] = df['Updated Price'].apply(lambda x: "{:.2f}".format(x))
        df.to_csv(r'\\SMSSERV001901\storeman\XchFile\Jon Imports\UNFI %s Update Import.csv' % file_name, index=False)