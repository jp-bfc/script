#https://drive.google.com/drive/folders/18SOpfwssEOTmBthiSy-3I3m-3Cc_hgAL?usp=sharing
import configparser
from operator import add
from timeit import repeat
from turtle import color
import pyodbc
import pandas as pd
from sqlite3 import converters
from sqlalchemy import all_
import xlrd
#https://docs.python.org/3/library/tkinter.html
import tkinter as tk
from tkinter import filedialog
from pathlib import Path
import datetime as dt
import os
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
#from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import time
import numpy as np
import calendar
import configparser


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

dashboards_folder_id = config['DEFAULT']['dashboard_folder']
plan_figure_file_id = config['DEFAULT']['plan_figure_sheet']


#EVENTUALLY: https://stackoverflow.com/questions/71082494/getting-a-warning-when-using-a-pyodbc-connection-object-with-pandas
#PANDAS DEPRECATING SUPPORT FOR PYODBC
# connection_string = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=dagger;DATABASE=test;UID=user;PWD=password"
# connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
# engine = create_engine(connection_url)
cursor = cnxn.cursor()

quarterly_data_frame_headers = [
    'Quarter', 'Dept',
    'Revenue', 'Plan Quarterly Sales', 'RevPercentPlan', 'LY REVENUE', 'REVENUE_GROWTH',
    'Cost', 'Plan Quarterly COGS', 'COGSPercentPlan', 'LY COST', 'COST_GROWTH',
    'Margin', 'Plan Margin', 'MARGINPercentPlan', 'LY MARGIN', 'YoY Diff',
    'Profit', 'Plan Profit', 'ProfitPercentPlan', 'LY PROFIT', 'PROFIT_GROWTH',
    'Volume', 'LY VOLUME', 'VOLUME_GROWTH'
]

yearly_data_frame_headers = [
    'Annualize','Year', 'Dept',
    'Revenue', 'Plan Yearly Sales', 'RevPercentPlan', 'LY REVENUE', 'REVENUE_GROWTH',
    'Cost', 'Plan Yearly COGS', 'COGSPercentPlan', 'LY COST', 'COST_GROWTH',
    'Margin', 'Plan Margin', 'MARGINPercentPlan', 'LY MARGIN', 'YoY Diff',
    'Profit', 'Plan Profit', 'ProfitPercentPlan', 'LY PROFIT', 'PROFIT_GROWTH',
    'Volume', 'LY VOLUME', 'VOLUME_GROWTH'
]

weekly_data_frame_headers = [
'Day', 'Dept',
'Revenue', 'Plan Weekly Sales', 'RevPercentPlan', 'Ninety_Trailing_Revenue', 'LY REVENUE', 'REVENUE_GROWTH',
'Cost', 'Plan Weekly COGS', 'COGSPercentPlan', 'Ninety_Trailing_Cost', 'LY COST', 'COST_GROWTH',
'Margin', 'Plan Margin', 'MARGINPercentPlan', 'Ninety_Trailing_Margin', 'LY MARGIN', 'YoY Diff',
'Profit', 'Plan Profit', 'ProfitPercentPlan', 'Ninety_Trailing_Profit', 'LY PROFIT', 'PROFIT_GROWTH',
'Volume', 'Ninety_Trailing_Volume', 'LY VOLUME', 'VOLUME_GROWTH'

]

monthly_data_frame_headers = [
    'Day', 'Dept', 'Revenue', 'Plan Monthly Sales', 'RevPercentPlan', 'Ninety_Trailing_Revenue', 'LY REVENUE', 'REVENUE_GROWTH',
    'Cost', 'Plan Monthly COGS', 'COGSPercentPlan', 'Ninety_Trailing_Cost', 'LY COST', 'COST_GROWTH',
    'Margin', 'Plan Margin', 'MARGINPercentPlan', 'Ninety_Trailing_Margin', 'LY MARGIN', 'YoY Diff',
    'Profit', 'Plan Profit', 'ProfitPercentPlan', 'Ninety_Trailing_Profit', 'LY PROFIT', 'PROFIT_GROWTH',
    'Volume', 'Ninety_Trailing_Volume', 'LY VOLUME', 'VOLUME_GROWTH'
]

daily_data_frame_headers = [
    'Day', 'Dept', 'Revenue', 'Plan Daily Sales', 'RevPercentPlan', 'LW Revenue', 'Ninety_Trailing_Revenue', 'LY REVENUE', 'REVENUE_GROWTH',
    'Cost', 'Plan Daily COGS', 'COGSPercentPlan', 'LW Cost', 'Ninety_Trailing_Cost', 'LY COST', 'COST_GROWTH',
    'Margin', 'Plan Margin', 'MARGINPercentPlan', 'LW Margin', 'Ninety_Trailing_Margin', 'LY MARGIN', 'YoY Diff',
    'Profit', 'Plan Profit', 'ProfitPercentPlan', 'LW Profit', 'Ninety_Trailing_Profit', 'LY PROFIT', 'PROFIT_GROWTH',
    'Volume', 'LW Volume', 'Ninety_Trailing_Volume', 'LY VOLUME', 'VOLUME_GROWTH'
]

mergeDailyHeaders = [
    ["",
    "Gross Sales","","","","","","",
    "COGS","","","","","","",
    "Gross Margin","","","","","","",
    "Profit","","","","","","",
    "Volume","","","","",
    "Variance","","",
    "Labor $","","","","",
    "Labor Hours","","","","",
    "Sales $ per Labor Hour","","","","",
    "Margin-Labor","","","",""],
    ["",
    "Actual","Plan","% Plan","Last Week","90 Day","LY","% LY",
    "Actual","Plan","% Plan","Last Week","90 Day","LY","% LY",
    "Actual","Plan","Diff","Last Week","90 Day","LY","YoY Diff",
    "Actual","Plan","% Plan","Last Week","90 Day","LY","% LY",
    "Volume","Last Week","90 Day","LY","% LY",
    "Total GP Variance","Due to Margin %","Due to Sales Volume",
    "Actual","Plan","% Plan","LY","% LY",
    "Actual","Plan","% Plan","LY","% LY",
    "Actual","Plan","% Plan","LY","% LY",
    "Actual","Plan","% Plan","LY","% LY"]
    ]

totalDailyHeaders = [
    ["","",
    "Gross Sales","","","","","","",
    "COGS","","","","","","",
    "Gross Margin","","","","","","",
    "Profit","","","","","","",
    "Volume","","","","",
    "Variance","","",
    "Labor $","","","","",
    "Labor Hours","","","","",
    "Sales $ per Labor Hour","","","","",
    "Margin-Labor","","","",""],
    ["","Dept",
    "Actual","Plan","% Plan","Last Week","90 Day","LY","% LY",
    "Actual","Plan","% Plan","Last Week","90 Day","LY","% LY",
    "Actual","Plan","Diff","Last Week","90 Day","LY","YoY Diff",
    "Actual","Plan","% Plan","Last Week","90 Day","LY","% LY",
    "Volume","Last Week","90 Day","LY","% LY",
    "Total GP Variance","Due to Margin %","Due to Sales Volume",
    "Actual","Plan","% Plan","LY","% LY",
    "Actual","Plan","% Plan","LY","% LY",
    "Actual","Plan","% Plan","LY","% LY",
    "Actual","Plan","% Plan","LY","% LY"]
    ]

daily_merge_ranges = [
    #Gross Sales
    [1,8],
    #COGS
    [8,15],
    #Gross Margin
    [15,22],
    #Profit
    [22,29],
    #Volume
    [29,34],
    #Variance
    [34,37],
    #Labor $
    [37,42],
    #Labor Hours
    [42,47],
    #Sales $ per Labor Hour
    [47,52],
    #Margin less Labor
    [52,57]
]

mergeWeeklyMonthlyHeaders = [
    ["",
    "Gross Sales","","","","","",
    "COGS","","","","","",
    "Gross Margin","","","","","",
    "Profit","","","","","",
    "Volume","","","",
    "Variance","","",
    "Labor $","","","","",
    "Labor Hours","","","","",
    "Sales $ per Labor Hour","","","","",
    "Margin-Labor","","","",""],
    ["",
    "Actual","Plan","% Plan","90 Day","LY","% LY",
    "Actual","Plan","% Plan","90 Day","LY","% LY",
    "Actual","Plan","Diff","90 Day","LY","YoY Diff",
    "Actual","Plan","% Plan","90 Day","LY","% LY",
    "Volume","90 Day","LY","% LY",
    "Total GP Variance","Due to Margin %","Due to Sales Volume",
    "Actual","Plan","% Plan","LY","% LY",
    "Actual","Plan","% Plan","LY","% LY",
    "Actual","Plan","% Plan","LY","% LY",
    "Actual","Plan","% Plan","LY","% LY"]
    ]

totalWeeklyMonthlyHeaders = [
    ["","",
    "Gross Sales","","","","","",
    "COGS","","","","","",
    "Gross Margin","","","","","",
    "Profit","","","","","",
    "Volume","","","",
    "Variance","","",
    "Labor $","","","","",
    "Labor Hours","","","","",
    "Sales $ per Labor Hour","","","","",
    "Margin-Labor","","","",""],
    ["","Dept",
    "Actual","Plan","% Plan","90 Day","LY","% LY",
    "Actual","Plan","% Plan","90 Day","LY","% LY",
    "Actual","Plan","Diff","90 Day","LY","YoY Diff",
    "Actual","Plan","% Plan","90 Day","LY","% LY",
    "Volume","90 Day","LY","% LY",
    "Total GP Variance","Due to Margin %","Due to Sales Volume",
    "Actual","Plan","% Plan","LY","% LY",
    "Actual","Plan","% Plan","LY","% LY",
    "Actual","Plan","% Plan","LY","% LY",
    "Actual","Plan","% Plan","LY","% LY"]
    ]

weekly_monthly_merge_ranges = [
    #Gross Sales
    [1,7],
    #COGS
    [7,13],
    #Gross Margin
    [13,19],
    #Profit
    [19,25],
    #Volume
    [25,29],
    #Variance
    [29,32],
    #Labor $
    [32,37],
    #Labor Hours
    [37,42],
    #Sales $ per Labor Hour
    [42,47],
    #Margin less Labor
    [47,52]
]

mergeQuarterlyHeaders = [
    ["",
    "Gross Sales","","","","",
    "COGS","","","","",
    "Gross Margin","","","","",
    "Profit","","","","",
    "Volume","","",
    "Variance","","",
    "Labor $","","","","",
    "Labor Hours","","","","",
    "Sales $ per Labor Hour","","","","",
    "Margin-Labor","","","",""],
    ["",
    "Actual","Plan","% Plan","LY","% LY",
    "Actual","Plan","% Plan","LY","% LY",
    "Actual","Plan","Diff","LY","YoY Diff",
    "Actual","Plan","% Plan","LY","% LY",
    "Volume","LY","% LY",
    "Total GP Variance","Due to Margin %","Due to Sales Volume",
    "Actual","Plan","% Plan","LY","% LY",
    "Actual","Plan","% Plan","LY","% LY",
    "Actual","Plan","% Plan","LY","% LY",
    "Actual","Plan","% Plan","LY","% LY"]
    ]

totalQuarterlyHeaders = [
    ["","",
    "Gross Sales","","","","",
    "COGS","","","","",
    "Gross Margin","","","","",
    "Profit","","","","",
    "Volume","","",
    "Variance","","",
    "Labor $","","","","",
    "Labor Hours","","","","",
    "Sales $ per Labor Hour","","","","",
    "Margin-Labor","","","",""],
    ["","Dept",
    "Actual","Plan","% Plan","LY","% LY",
    "Actual","Plan","% Plan","LY","% LY",
    "Actual","Plan","Diff","LY","YoY Diff",
    "Actual","Plan","% Plan","LY","% LY",
    "Volume","LY","% LY",
    "Total GP Variance","Due to Margin %","Due to Sales Volume",
    "Actual","Plan","% Plan","LY","% LY",
    "Actual","Plan","% Plan","LY","% LY",
    "Actual","Plan","% Plan","LY","% LY",
    "Actual","Plan","% Plan","LY","% LY"]
    ]

quarterly_merge_ranges = [
    #Gross Sales
    [1,6],
    #COGS
    [6,11],
    #Gross Margin
    [11,16],
    #Profit
    [16,21],
    #Volume
    [21,24],
    #Variance
    [24,27],
    #Labor $
    [27,32],
    #Labor Hours
    [32,37],
    #Sales $ per Labor Hour
    [37,42],
    #Margin less Labor
    [42,47]
]

mergeYearlyHeaders = [
    ["",
    "Gross Sales","","","","",
    "COGS","","","","",
    "Gross Margin","","","","",
    "Profit","","","","",
    "Volume","","",
    "Variance","","",
    "Labor $","","","","",
    "Labor Hours","","","","",
    "Sales $ per Labor Hour","","","","",
    "Margin-Labor","","","",""],
    ["",
    "Actual","Plan","% Plan","LY","% LY",
    "Actual","Plan","% Plan","LY","% LY",
    "Actual","Plan","Diff","LY","YoY Diff",
    "Actual","Plan","% Plan","LY","% LY",
    "Volume","LY","% LY",
    "Total GP Variance","Due to Margin %","Due to Sales Volume",
    "Actual","Plan","% Plan","LY","% LY",
    "Actual","Plan","% Plan","LY","% LY",
    "Actual","Plan","% Plan","LY","% LY",
    "Actual","Plan","% Plan","LY","% LY"]
    ]

totalYearlyHeaders = [
    ["","",
    "Gross Sales","","","","",
    "COGS","","","","",
    "Gross Margin","","","","",
    "Profit","","","","",
    "Volume","","",
    "Variance","","",
    "Labor $","","","","",
    "Labor Hours","","","","",
    "Sales $ per Labor Hour","","","","",
    "Margin-Labor","","","",""],
    ["","Dept",
    "Actual","Plan","% Plan","LY","% LY",
    "Actual","Plan","% Plan","LY","% LY",
    "Actual","Plan","Diff","LY","YoY Diff",
    "Actual","Plan","% Plan","LY","% LY",
    "Volume","LY","% LY",
    "Total GP Variance","Due to Margin %","Due to Sales Volume",
    "Actual","Plan","% Plan","LY","% LY",
    "Actual","Plan","% Plan","LY","% LY",
    "Actual","Plan","% Plan","LY","% LY",
    "Actual","Plan","% Plan","LY","% LY"]
    ]

yearly_merge_ranges = [
    #Gross Sales
    [1,6],
    #COGS
    [6,11],
    #Gross Margin
    [11,16],
    #Profit
    [16,21],
    #Volume
    [21,24],
    #Variance
    [24,27],
    #Labor $
    [27,32],
    #Labor Hours
    [32,37],
    #Sales $ per Labor Hour
    [37,42],
    #Margin less Labor
    [42,47]
]

grand_daily_merge_range = [[x+1 for x in l] for l in daily_merge_ranges]
grand_weekly_monthly_merge_range = [[x+1 for x in l] for l in weekly_monthly_merge_ranges]
grand_quarterly_merge_range = [[x+1 for x in l] for l in quarterly_merge_ranges]
grand_yearly_merge_range = [[x+1 for x in l] for l in yearly_merge_ranges]

class LaborReport:
    def __init__(self):
        df = pd.read_csv("Labor Running Total 2022-09-12.csv")
        df['Week'] = pd.to_datetime(df['Week'])
        df = df.sort_values("Week", ascending=False)
        self.Cheese = df[df['Dept'] == '110-Cheese']
        self.Dairy = df[df['Dept'] == '300-Dairy']
        self.TotalStore = df[df['Dept'] == 'Total Store']
        self.Seafood = df[df['Dept'] == '910-Seafood']
        self.Meat = df[df['Dept'] == '900-Meat']
        self.Deli = df[df['Dept'] == '800-Deli']
        self.Receiving = df[df['Dept'] == '700-Receiving']
        self.Wellness = df[df['Dept'] == '600-Wellness']
        self.Bulk = df[df['Dept'] == '500-Bulk']
        self.Beer = df[df['Dept'] == '400-Beer']
        self.Administrative = df[df['Dept'] == '310-Administrative']
        self.IT = df[df['Dept'] == '280-IT']
        self.People = df[df['Dept'] == '230-People']
        self.Training = df[df['Dept'] == '220-Training']
        self.Produce = df[df['Dept'] == '200-Produce']
        self.Marketing = df[df['Dept'] == '180-Marketing']
        self.Finance = df[df['Dept'] == '170-Finance']
        self.General = df[df['Dept'] == '160-General']
        self.Maintenance = df[df['Dept'] == '150-Maintenance']
        self.Cashiers = df[df['Dept'] == '140-Cashiers']
        self.Curbside = df[df['Dept'] == '135-Curbside']
        self.Supervisors = df[df['Dept'] == '130-Supervisors']
        self.Grocery = df[df['Dept'] == '100-Grocery']
        self.report_look_ups = {
            "Cheese" : self.Cheese,
            "Dairy" : self.Dairy,
            "Store Wide" : self.TotalStore,
            "Seafood" : self.Seafood,
            "Meat" : self.Meat,
            "Deli" : self.Deli,
            "Wellness" : self.Wellness,
            "Bulk" : self.Bulk,
            "Beer" : self.Beer,
            "Produce" : self.Produce,
            "Marketing" : self.Marketing,
            "Grocery" : self.Grocery
        }

    def report(self, report_code):
        return self.report_look_ups[report_code]

    def print(self):
        print(
        self.Cheese, self.Dairy, self.TotalStore, self.Seafood, self.Meat, self.Deli,
        self.Receiving, self.Wellness, self.Bulk, self.Beer, self.Administrative, self.IT, 
        self.People, self.Training, self.Produce, self.Marketing, self.Finance, self.General, 
        self.Maintenance, self.Cashiers, self.Curbside, self.Supervisors, self.Grocery
        )

# "Grocery" : Grocery, Taxable, Dairy, Frozen,  Bulk, CTH
# "Alcohol" : Beer, Wine
# "Wellness" : Haba, , Supplements, CBM, Housewares
# "Cheese" : Cheese
# "Deli" : Deli
# "Meat" : Meat
# "Seafood" : Seafood
# "Produce" : Produce, Floral
# "Marketing" : Marketing

report_groups = {
    "Produce" : [2, 18],
    "Cheese" : [10],
    "Alcohol" : [4, 13],
    "Deli" : [8],
    "Wellness" : [6, 11, 16, 19],
    "Grocery" : [1, 3, 5, 7, 15],
    "Meat" : [9],
    "Seafood" : [22],
    "Bulk" : [5,17],
    "Dairy" : [3],
    "Floral" : [18]
}

dept_list = {4 : 'Beer',
5 : 'Bulk',
19 : 'CBM',
10 :'Cheese',
3 : 'Dairy',
8 : 'Deli',
18 : 'Floral',
15 : 'Frozen',
1 : 'Grocery',
6 : 'Haba',
16 : 'Housewares',
9 : 'Meat',
2 : 'Produce',
22 : 'Seafood',
11 : 'Supplements',
7 : 'Taxable',
17 : 'TCH',
13 : 'Wine',
0 : 'Store Wide'}

dept_order = {
'Beer Department' : 0,
'Bulk Department' : 1,
'CBM Department' : 2,
'Cheese Department' : 3,
'Dairy Department' : 4,
'Deli Department' : 5,
'Floral Department' : 6,
'Frozen Department' : 7,
'Grocery Department' : 8,
'Haba Department' : 9,
'Housewares Department' : 10,
'Meat Department' : 11,
'Produce Department' : 12,
'Seafood Department' : 13,
'Supplements Department' : 14,
'Taxable Grocery' : 15,
'TCH Department' : 16,
'Wine Department' : 17,
'Store Wide' : 18,
'Total' : 19
}

dept_daily_query = """
                    select revenueReport.F254 as 'Day',
                    reportCodeTable.F1024 as 'Dept',
                    sum(revenueReport.F65) as 'Revenue',
                    LAG(sum(revenueReport.F65), 7) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03) as 'LW Revenue',
                    ROUND(AVG(sum(revenueReport.F65)) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03 ROWS BETWEEN 88 PRECEDING AND 1 PRECEDING), 2) as 'Ninety_Trailing_Revenue',
                    LAG(sum(revenueReport.F65), 358) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03) as 'LY REVENUE',
                    ROUND((sum(revenueReport.F65)/LAG(sum(revenueReport.F65), 358) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03)-1), 4) as 'REVENUE_GROWTH',
                    costReport.F65 as 'Cost',
                    LAG(costReport.F65, 7) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03) as 'LW Cost',
                    ROUND(AVG(costReport.F65) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03 ROWS BETWEEN 88 PRECEDING AND 1 PRECEDING), 2) as 'Ninety_Trailing_Cost',
                    LAG(costReport.F65, 358) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03) as 'LY COST',
                    ROUND((costReport.F65/LAG(costReport.F65, 358) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03)-1), 4) as 'COST_GROWTH',
                    ROUND(((sum(revenueReport.F65) - costReport.F65) / sum(revenueReport.F65)), 4) as 'Margin',
                    ROUND(AVG(((sum(revenueReport.F65)-costReport.F65)/sum(revenueReport.F65))) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03 ROWS BETWEEN 88 PRECEDING AND 1 PRECEDING), 4) as 'Ninety_Trailing_Margin',
                    LAG(ROUND(((sum(revenueReport.F65) - costReport.F65) / sum(revenueReport.F65)), 4), 7) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03) as 'LW Margin',
                    LAG(ROUND(((sum(revenueReport.F65) - costReport.F65) / sum(revenueReport.F65)), 4), 358) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03) as 'LY MARGIN',
                    ROUND(((sum(revenueReport.F65) - costReport.F65) / sum(revenueReport.F65)), 4) - LAG(ROUND(((sum(revenueReport.F65) - costReport.F65) / sum(revenueReport.F65)), 4), 358) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03) as 'YoY Diff',
                    sum(revenueReport.F65) - costReport.F65 as 'Profit',
                    LAG(sum(revenueReport.F65) - costReport.F65, 7) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03) as 'LW Profit',
                    ROUND((AVG(sum(revenueReport.F65)-costReport.F65) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03 ROWS BETWEEN 88 PRECEDING AND 1 PRECEDING)), 4) as 'Ninety_Trailing_Profit',
                    LAG(sum(revenueReport.F65) - costReport.F65, 358) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03) as 'LY PROFIT',
                    ROUND(((sum(revenueReport.F65) - costReport.F65)/LAG(sum(revenueReport.F65) - costReport.F65, 358) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03) - 1), 4) as 'PROFIT_GROWTH',
                    sum(revenueReport.F64) as 'Volume',
                    LAG(sum(revenueReport.F64), 7) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03) as 'LW Volume',
                    ROUND(AVG(sum(revenueReport.F64)) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03 ROWS BETWEEN 88 PRECEDING AND 1 PRECEDING), 2) as 'Ninety_Trailing_Volume',
                    LAG(sum(revenueReport.F64), 358) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03) as 'LY VOLUME',
                    ROUND((sum(revenueReport.F64)/LAG(sum(revenueReport.F64), 358) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03)-1), 4) as 'VOLUME_GROWTH'
                    from
                    (select * from STORESQL.dbo.RPT_DPT 
                    where F1031 = 'D' and F1034 = 3) revenueReport
                    inner join (select * from STORESQL.dbo.RPT_DPT where F1031 = 'D' and F1034 = 8101) costReport on revenueReport.F254 = costReport.F254 and revenueReport.F03 = costReport.F03
                    inner join (select F18, F1024 from STORESQL.dbo.RPC_TAB) reportCodeTable on revenueReport.F03 = reportCodeTable.F18
                    where revenueReport.F03 not in (14, 21, 23, 97, 98, 99) 
                    group by revenueReport.F254, revenueReport.F03, costReport.F254, costReport.F03, costReport.F65, reportCodeTable.F1024
                    order by revenueReport.F254 desc, reportCodeTable.F1024
                    """

dept_weekly_query = """
select revenueReport.F254 as 'Day',
reportCodeTable.F1024 as 'Dept',
sum(revenueReport.F65) as 'Revenue',
ROUND(AVG(sum(revenueReport.F65)) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03 ROWS BETWEEN 13 PRECEDING AND 1 PRECEDING), 2) as 'Ninety_Trailing_Revenue',
LAG(sum(revenueReport.F65), 52) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03) as 'LY REVENUE',
ROUND((sum(revenueReport.F65)/LAG(sum(revenueReport.F65), 52) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03)-1), 4) as 'REVENUE_GROWTH',
costReport.F65 as 'Cost',
ROUND(AVG(costReport.F65) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03 ROWS BETWEEN 13 PRECEDING AND 1 PRECEDING), 2) as 'Ninety_Trailing_Cost',
LAG(costReport.F65, 52) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03) as 'LY COST',
ROUND((costReport.F65/LAG(costReport.F65, 52) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03)-1), 4) as 'COST_GROWTH',
ROUND(((sum(revenueReport.F65) - costReport.F65) / sum(revenueReport.F65)), 4) as 'Margin',
ROUND(AVG(((sum(revenueReport.F65)-costReport.F65)/sum(revenueReport.F65))) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03 ROWS BETWEEN 13 PRECEDING AND 1 PRECEDING), 4) as 'Ninety_Trailing_Margin',
LAG(ROUND(((sum(revenueReport.F65) - costReport.F65) / sum(revenueReport.F65)), 4), 52) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03) as 'LY MARGIN',
ROUND(((sum(revenueReport.F65) - costReport.F65) / sum(revenueReport.F65)), 4) - LAG(ROUND(((sum(revenueReport.F65) - costReport.F65) / sum(revenueReport.F65)), 4), 52) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03) as 'YoY Diff',
sum(revenueReport.F65) - costReport.F65 as 'Profit',
ROUND((AVG(sum(revenueReport.F65)-costReport.F65) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03 ROWS BETWEEN 13 PRECEDING AND 1 PRECEDING)), 4) as 'Ninety_Trailing_Profit',
LAG(sum(revenueReport.F65) - costReport.F65, 52) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03) as 'LY PROFIT',
ROUND(((sum(revenueReport.F65) - costReport.F65)/LAG(sum(revenueReport.F65) - costReport.F65, 52) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03) - 1), 4) as 'PROFIT_GROWTH',
sum(revenueReport.F64) as 'Volume',
ROUND(AVG(sum(revenueReport.F64)) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03 ROWS BETWEEN 13 PRECEDING AND 1 PRECEDING), 2) as 'Ninety_Trailing_Volume',
LAG(sum(revenueReport.F64), 52) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03) as 'LY VOLUME',
ROUND((sum(revenueReport.F64)/LAG(sum(revenueReport.F64), 52) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03)-1), 4) as 'VOLUME_GROWTH'
from
(select * from STORESQL.dbo.RPT_DPT 
where F1031 = 'W' and F1034 = 3) revenueReport
inner join (select * from STORESQL.dbo.RPT_DPT where F1031 = 'W' and F1034 = 8101) costReport on revenueReport.F254 = costReport.F254 and revenueReport.F03 = costReport.F03
inner join (select F18, F1024 from STORESQL.dbo.RPC_TAB) reportCodeTable on revenueReport.F03 = reportCodeTable.F18
where revenueReport.F03 not in (14, 21, 23, 97, 98, 99) 
group by revenueReport.F254, revenueReport.F03, costReport.F254, costReport.F03, costReport.F65, reportCodeTable.F1024
order by revenueReport.F254 desc, reportCodeTable.F1024
"""

dept_monthly_query = """
select revenueReport.F254 as 'Day',
reportCodeTable.F1024 as 'Dept',
sum(revenueReport.F65) as 'Revenue',
ROUND(AVG(sum(revenueReport.F65)) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03 ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) as 'Ninety_Trailing_Revenue',
LAG(sum(revenueReport.F65), 12) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03) as 'LY REVENUE',
ROUND((sum(revenueReport.F65)/LAG(sum(revenueReport.F65), 12) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03)-1), 4) as 'REVENUE_GROWTH',
costReport.F65 as 'Cost',
ROUND(AVG(costReport.F65) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03 ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) as 'Ninety_Trailing_Cost',
LAG(costReport.F65, 12) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03) as 'LY COST',
ROUND((costReport.F65/LAG(costReport.F65, 12) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03)-1), 4) as 'COST_GROWTH',
ROUND(((sum(revenueReport.F65) - costReport.F65) / sum(revenueReport.F65)), 4) as 'Margin',
ROUND(AVG(((sum(revenueReport.F65)-costReport.F65)/sum(revenueReport.F65))) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03 ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 4) as 'Ninety_Trailing_Margin',
LAG(ROUND(((sum(revenueReport.F65) - costReport.F65) / sum(revenueReport.F65)), 4), 12) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03) as 'LY MARGIN',
ROUND(((sum(revenueReport.F65) - costReport.F65) / sum(revenueReport.F65)), 4) - LAG(ROUND(((sum(revenueReport.F65) - costReport.F65) / sum(revenueReport.F65)), 4), 12) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03) as 'YoY Diff',
sum(revenueReport.F65) - costReport.F65 as 'Profit',
ROUND((AVG(sum(revenueReport.F65)-costReport.F65) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03 ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING)), 4) as 'Ninety_Trailing_Profit',
LAG(sum(revenueReport.F65) - costReport.F65, 12) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03) as 'LY PROFIT',
ROUND(((sum(revenueReport.F65) - costReport.F65)/LAG(sum(revenueReport.F65) - costReport.F65, 12) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03) - 1), 4) as 'PROFIT_GROWTH',
sum(revenueReport.F64) as 'Volume',
ROUND(AVG(sum(revenueReport.F64)) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03 ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) as 'Ninety_Trailing_Volume',
LAG(sum(revenueReport.F64), 12) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03) as 'LY VOLUME',
ROUND((sum(revenueReport.F64)/LAG(sum(revenueReport.F64), 12) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03)-1), 4) as 'VOLUME_GROWTH'
from
(select * from STORESQL.dbo.RPT_DPT 
where F1031 = 'M' and F1034 = 3) revenueReport
inner join (select * from STORESQL.dbo.RPT_DPT where F1031 = 'M' and F1034 = 8101) costReport on revenueReport.F254 = costReport.F254 and revenueReport.F03 = costReport.F03
inner join (select F18, F1024 from STORESQL.dbo.RPC_TAB) reportCodeTable on revenueReport.F03 = reportCodeTable.F18
where revenueReport.F03 not in (14, 21, 23, 97, 98, 99) 
group by revenueReport.F254, revenueReport.F03, costReport.F254, costReport.F03, costReport.F65, reportCodeTable.F1024
order by revenueReport.F254 desc, reportCodeTable.F1024
"""

dept_yearly_query = """
select revenueReport.F254 as 'Day',
reportCodeTable.F1024 as 'Dept',
sum(revenueReport.F65) as 'Revenue',
LAG(sum(revenueReport.F65), 1) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03) as 'LY REVENUE',
ROUND((sum(revenueReport.F65)/LAG(sum(revenueReport.F65), 1) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03)-1), 4) as 'REVENUE_GROWTH',
costReport.F65 as 'Cost',
LAG(costReport.F65, 1) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03) as 'LY COST',
ROUND((costReport.F65/LAG(costReport.F65, 1) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03)-1), 4) as 'COST_GROWTH',
ROUND(((sum(revenueReport.F65) - costReport.F65) / sum(revenueReport.F65)), 4) as 'Margin',
LAG(ROUND(((sum(revenueReport.F65) - costReport.F65) / sum(revenueReport.F65)), 4), 1) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03) as 'LY MARGIN',
ROUND(((sum(revenueReport.F65) - costReport.F65) / sum(revenueReport.F65)), 4) - LAG(ROUND(((sum(revenueReport.F65) - costReport.F65) / sum(revenueReport.F65)), 4), 1) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03) as 'YoY Diff',
sum(revenueReport.F65) - costReport.F65 as 'Profit',
LAG(sum(revenueReport.F65) - costReport.F65, 1) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03) as 'LY PROFIT',
ROUND(((sum(revenueReport.F65) - costReport.F65)/LAG(sum(revenueReport.F65) - costReport.F65, 1) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03) - 1), 4) as 'PROFIT_GROWTH',
sum(revenueReport.F64) as 'Volume',
LAG(sum(revenueReport.F64), 1) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03) as 'LY VOLUME',
ROUND((sum(revenueReport.F64)/LAG(sum(revenueReport.F64), 1) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03)-1), 4) as 'VOLUME_GROWTH'
from
(select * from STORESQL.dbo.RPT_DPT 
where F1031 = 'Y' and F1034 = 3) revenueReport
inner join (select * from STORESQL.dbo.RPT_DPT where F1031 = 'Y' and F1034 = 8101) costReport on revenueReport.F254 = costReport.F254 and revenueReport.F03 = costReport.F03
inner join (select F18, F1024 from STORESQL.dbo.RPC_TAB) reportCodeTable on revenueReport.F03 = reportCodeTable.F18
where revenueReport.F03 not in (14, 21, 23, 97, 98, 99) 
group by revenueReport.F254, revenueReport.F03, costReport.F254, costReport.F03, costReport.F65, reportCodeTable.F1024
order by revenueReport.F254 desc, reportCodeTable.F1024
"""

grand_daily_query = """
select revenueReport.F254 as 'Day',
'Store Wide' as 'Dept',
sum(revenueReport.F65) as 'Revenue',
LAG(sum(revenueReport.F65), 7) OVER (order by revenueReport.F254) as 'LW Revenue',
ROUND(AVG(sum(revenueReport.F65)) OVER (order by revenueReport.F254 ROWS BETWEEN 88 PRECEDING AND 1 PRECEDING), 2) as 'Ninety_Trailing_Revenue',
LAG(sum(revenueReport.F65), 358) OVER (order by revenueReport.F254) as 'LY REVENUE',
ROUND((sum(revenueReport.F65)/(LAG(sum(revenueReport.F65), 358) OVER (order by revenueReport.F254))-1), 4) as 'REVENUE_GROWTH',
costReport.F65 as 'Cost',
LAG(costReport.F65, 7) OVER (order by revenueReport.F254) as 'LW Cost',
ROUND(AVG(costReport.F65) OVER (order by revenueReport.F254 ROWS BETWEEN 88 PRECEDING AND 1 PRECEDING), 2) as 'Ninety_Trailing_Cost',
LAG(costReport.F65, 358) OVER (order by revenueReport.F254) as 'LY COST',
ROUND((costReport.F65/(LAG(costReport.F65, 358) OVER (order by revenueReport.F254))-1), 4) as 'COST_GROWTH',
ROUND(((sum(revenueReport.F65)-costReport.F65)/sum(revenueReport.F65)),4) as 'Margin',
LAG(ROUND(((sum(revenueReport.F65) - costReport.F65) / sum(revenueReport.F65)), 4), 7) OVER (order by revenueReport.F254) as 'LW Margin',
ROUND(AVG(((sum(revenueReport.F65)-costReport.F65)/sum(revenueReport.F65))) OVER (order by revenueReport.F254 ROWS BETWEEN 88 PRECEDING AND 1 PRECEDING), 4) as 'Ninety_Trailing_Margin',
LAG(ROUND(((sum(revenueReport.F65) - costReport.F65) / sum(revenueReport.F65)), 4), 358) OVER (order by revenueReport.F254) as 'LY MARGIN',
ROUND(((sum(revenueReport.F65) - costReport.F65) / sum(revenueReport.F65)), 4) - LAG(ROUND(((sum(revenueReport.F65) - costReport.F65) / sum(revenueReport.F65)), 4), 358) OVER (order by revenueReport.F254) as 'YoY Diff',
(sum(revenueReport.F65)-costReport.F65) as 'Profit',
LAG((sum(revenueReport.F65)-costReport.F65), 7) OVER (order by revenueReport.F254) as 'LW Profit',
ROUND(AVG((sum(revenueReport.F65)-costReport.F65)) OVER (order by revenueReport.F254 ROWS BETWEEN 88 PRECEDING AND 1 PRECEDING), 2) as 'Ninety_Trailing_Profit',
LAG((sum(revenueReport.F65)-costReport.F65), 358) OVER (order by revenueReport.F254) as 'LY PROFIT',
ROUND((sum(revenueReport.F65)-costReport.F65)/(LAG((sum(revenueReport.F65)-costReport.F65), 358) OVER (order by revenueReport.F254))-1, 4) as 'PROFIT_GROWTH',
sum(revenueReport.F64) as 'Volume',
LAG(sum(revenueReport.F64), 7) OVER (order by revenueReport.F254) as 'LW Volume',
ROUND(AVG(sum(revenueReport.F64)) OVER (order by revenueReport.F254 ROWS BETWEEN 88 PRECEDING AND 1 PRECEDING), 2) as 'Ninety_Trailing_Volume',
LAG(sum(revenueReport.F64), 358) OVER (order by revenueReport.F254) as 'LY VOLUME',
ROUND((sum(revenueReport.F64)/(LAG(sum(revenueReport.F64), 358) OVER (order by revenueReport.F254))-1), 4) as 'VOLUME_GROWTH'
from (select * from STORESQL.dbo.RPT_FIN
where F1031 = 'D' and F1034 = 2) revenueReport
inner join (select * from STORESQL.dbo.RPT_FIN where F1031 = 'D' and F1034 = 8101) costReport on revenueReport.F254 = costReport.F254
group by revenueReport.F254, costReport.F65
order by revenueReport.F254 desc
"""

grand_weekly_query = """
select revenueReport.F254 as 'Day',
'Store Wide' as 'Dept',
sum(revenueReport.F65) as 'Revenue',
ROUND(AVG(sum(revenueReport.F65)) OVER (order by revenueReport.F254 ROWS BETWEEN 13 PRECEDING AND 1 PRECEDING), 2) as 'Ninety_Trailing_Revenue',
LAG(sum(revenueReport.F65), 52) OVER (order by revenueReport.F254) as 'LY REVENUE',
ROUND((sum(revenueReport.F65)/(LAG(sum(revenueReport.F65), 52) OVER (order by revenueReport.F254))-1), 4) as 'REVENUE_GROWTH',
costReport.F65 as 'Cost',
ROUND(AVG(costReport.F65) OVER (order by revenueReport.F254 ROWS BETWEEN 13 PRECEDING AND 1 PRECEDING), 2) as 'Ninety_Trailing_Cost',
LAG(costReport.F65, 52) OVER (order by revenueReport.F254) as 'LY COST',
ROUND((costReport.F65/(LAG(costReport.F65, 52) OVER (order by revenueReport.F254))-1), 4) as 'COST_GROWTH',
ROUND(((sum(revenueReport.F65)-costReport.F65)/sum(revenueReport.F65)),4) as 'Margin',
ROUND(AVG(((sum(revenueReport.F65)-costReport.F65)/sum(revenueReport.F65))) OVER (order by revenueReport.F254 ROWS BETWEEN 13 PRECEDING AND 1 PRECEDING), 4) as 'Ninety_Trailing_Margin',
LAG(ROUND(((sum(revenueReport.F65) - costReport.F65) / sum(revenueReport.F65)), 4), 52) OVER (order by revenueReport.F254) as 'LY MARGIN',
ROUND(((sum(revenueReport.F65) - costReport.F65) / sum(revenueReport.F65)), 4) - LAG(ROUND(((sum(revenueReport.F65) - costReport.F65) / sum(revenueReport.F65)), 4), 52) OVER (order by revenueReport.F254) as 'YoY Diff',
(sum(revenueReport.F65)-costReport.F65) as 'Profit',
ROUND(AVG((sum(revenueReport.F65)-costReport.F65)) OVER (order by revenueReport.F254 ROWS BETWEEN 13 PRECEDING AND 1 PRECEDING), 2) as 'Ninety_Trailing_Profit',
LAG((sum(revenueReport.F65)-costReport.F65), 52) OVER (order by revenueReport.F254) as 'LY PROFIT',
ROUND((sum(revenueReport.F65)-costReport.F65)/(LAG((sum(revenueReport.F65)-costReport.F65), 52) OVER (order by revenueReport.F254))-1, 4) as 'PROFIT_GROWTH',
sum(revenueReport.F64) as 'Volume',
ROUND(AVG(sum(revenueReport.F64)) OVER (order by revenueReport.F254 ROWS BETWEEN 13 PRECEDING AND 1 PRECEDING), 2) as 'Ninety_Trailing_Volume',
LAG(sum(revenueReport.F64), 52) OVER (order by revenueReport.F254) as 'LY VOLUME',
ROUND((sum(revenueReport.F64)/(LAG(sum(revenueReport.F64), 52) OVER (order by revenueReport.F254))-1), 4) as 'VOLUME_GROWTH'
from (select * from STORESQL.dbo.RPT_FIN
where F1031 = 'W' and F1034 = 2) revenueReport
inner join (select * from STORESQL.dbo.RPT_FIN where F1031 = 'W' and F1034 = 8101) costReport on revenueReport.F254 = costReport.F254
group by revenueReport.F254, costReport.F65
order by revenueReport.F254 desc
"""

grand_monthly_query = """
select revenueReport.F254 as 'Day',
'Store Wide' as 'Dept',
sum(revenueReport.F65) as 'Revenue',
ROUND(AVG(sum(revenueReport.F65)) OVER (order by revenueReport.F254 ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) as 'Ninety_Trailing_Revenue',
LAG(sum(revenueReport.F65), 12) OVER (order by revenueReport.F254) as 'LY REVENUE',
ROUND((sum(revenueReport.F65)/(LAG(sum(revenueReport.F65), 12) OVER (order by revenueReport.F254))-1), 4) as 'REVENUE_GROWTH',
costReport.F65 as 'Cost',
ROUND(AVG(costReport.F65) OVER (order by revenueReport.F254 ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) as 'Ninety_Trailing_Cost',
LAG(costReport.F65, 12) OVER (order by revenueReport.F254) as 'LY COST',
ROUND((costReport.F65/(LAG(costReport.F65, 12) OVER (order by revenueReport.F254))-1), 4) as 'COST_GROWTH',
ROUND(((sum(revenueReport.F65)-costReport.F65)/sum(revenueReport.F65)),4) as 'Margin',
ROUND(AVG(((sum(revenueReport.F65)-costReport.F65)/sum(revenueReport.F65))) OVER (order by revenueReport.F254 ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 4) as 'Ninety_Trailing_Margin',
LAG(ROUND(((sum(revenueReport.F65) - costReport.F65) / sum(revenueReport.F65)), 4), 12) OVER (order by revenueReport.F254) as 'LY MARGIN',
ROUND(((sum(revenueReport.F65) - costReport.F65) / sum(revenueReport.F65)), 4) - LAG(ROUND(((sum(revenueReport.F65) - costReport.F65) / sum(revenueReport.F65)), 4), 12) OVER (order by revenueReport.F254) as 'YoY Diff',
(sum(revenueReport.F65)-costReport.F65) as 'Profit',
ROUND(AVG((sum(revenueReport.F65)-costReport.F65)) OVER (order by revenueReport.F254 ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) as 'Ninety_Trailing_Profit',
LAG((sum(revenueReport.F65)-costReport.F65), 12) OVER (order by revenueReport.F254) as 'LY PROFIT',
ROUND((sum(revenueReport.F65)-costReport.F65)/(LAG((sum(revenueReport.F65)-costReport.F65), 12) OVER (order by revenueReport.F254))-1, 4) as 'PROFIT_GROWTH',
sum(revenueReport.F64) as 'Volume',
ROUND(AVG(sum(revenueReport.F64)) OVER (order by revenueReport.F254 ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) as 'Ninety_Trailing_Volume',
LAG(sum(revenueReport.F64), 12) OVER (order by revenueReport.F254) as 'LY VOLUME',
ROUND((sum(revenueReport.F64)/(LAG(sum(revenueReport.F64), 12) OVER (order by revenueReport.F254))-1), 4) as 'VOLUME_GROWTH'
from (select * from STORESQL.dbo.RPT_FIN
where F1031 = 'M' and F1034 = 2) revenueReport
inner join (select * from STORESQL.dbo.RPT_FIN where F1031 = 'M' and F1034 = 8101) costReport on revenueReport.F254 = costReport.F254
group by revenueReport.F254, costReport.F65
order by revenueReport.F254 desc
"""

grand_yearly_query = """
select revenueReport.F254 as 'Day',
'Store Wide' as 'Dept',
sum(revenueReport.F65) as 'Revenue',
LAG(sum(revenueReport.F65), 1) OVER (order by revenueReport.F254) as 'LY REVENUE',
ROUND((sum(revenueReport.F65)/(LAG(sum(revenueReport.F65), 1) OVER (order by revenueReport.F254))-1), 4) as 'REVENUE_GROWTH',
costReport.F65 as 'Cost',
LAG(costReport.F65, 1) OVER (order by revenueReport.F254) as 'LY COST',
ROUND((costReport.F65/(LAG(costReport.F65, 1) OVER (order by revenueReport.F254))-1), 4) as 'COST_GROWTH',
ROUND(((sum(revenueReport.F65)-costReport.F65)/sum(revenueReport.F65)),4) as 'Margin',
LAG(ROUND(((sum(revenueReport.F65) - costReport.F65) / sum(revenueReport.F65)), 4), 1) OVER (order by revenueReport.F254) as 'LY MARGIN',
ROUND(((sum(revenueReport.F65) - costReport.F65) / sum(revenueReport.F65)), 4) - LAG(ROUND(((sum(revenueReport.F65) - costReport.F65) / sum(revenueReport.F65)), 4), 1) OVER (order by revenueReport.F254) as 'YoY Diff',
(sum(revenueReport.F65)-costReport.F65) as 'Profit',
LAG((sum(revenueReport.F65)-costReport.F65), 1) OVER (order by revenueReport.F254) as 'LY PROFIT',
ROUND((sum(revenueReport.F65)-costReport.F65)/(LAG((sum(revenueReport.F65)-costReport.F65), 1) OVER (order by revenueReport.F254))-1, 4) as 'PROFIT_GROWTH',
sum(revenueReport.F64) as 'Volume',
LAG(sum(revenueReport.F64), 1) OVER (order by revenueReport.F254) as 'LY VOLUME',
ROUND((sum(revenueReport.F64)/(LAG(sum(revenueReport.F64), 1) OVER (order by revenueReport.F254))-1), 4) as 'VOLUME_GROWTH'
from (select * from STORESQL.dbo.RPT_FIN
where F1031 = 'Y' and F1034 = 2) revenueReport
inner join (select * from STORESQL.dbo.RPT_FIN where F1031 = 'Y' and F1034 = 8101) costReport on revenueReport.F254 = costReport.F254
group by revenueReport.F254, costReport.F65
order by revenueReport.F254 desc
"""


def createBook(bookName, folderId, mimeType, drive_api_client):
    return drive_api_client.files().create(
                body={
                'name' : bookName,
                'parents' : ['%s' % folderId],
                'mimeType' : mimeType
                },
                fields='id').execute()

def batchUpdate(workbookId, body):
        #print(body)
        return sheets.spreadsheets().batchUpdate(
            spreadsheetId = workbookId,
            body = {
                "requests" : body
            }
        ).execute()

def append(workbookId, range, dimension, data, valueInput="RAW"):
    return sheets.spreadsheets().values().append(
            spreadsheetId=workbookId,
            range=range,
            body={ "majorDimension" : dimension, "values" : data},
            valueInputOption=valueInput
            ).execute()

def gridRange(sheet_id, startRowIndex=None, endRowIndex=None,startColumnIndex=None,endColumnIndex=None):
    if sheet_id is not None and sheet_id != "":
        returned_range = {"sheetId" : sheet_id}
        if startRowIndex is not None:
            returned_range["startRowIndex"] = startRowIndex
        if endRowIndex is not None:
            returned_range["endRowIndex"] = endRowIndex
        if startColumnIndex is not None:
            returned_range["startColumnIndex"] = startColumnIndex
        if endColumnIndex is not None:
            returned_range["endColumnIndex"] = endColumnIndex
        return returned_range
    else:
        print("No Sheet ID passed to gridRange call.")

def cellData(
        userEnteredValue=None, effectiveValue=None, formattedValue=None, userEnteredFormat=None, effectiveFormat=None,
        hyperlink=None, note=None, textFormatRuns=None, dataValidation=None, pivotTable=None, dataSourceTable=None, dataSourceFormula=None
    ):
    returned_cell = {}
    if userEnteredValue is not None: returned_cell["userEnteredValue"] = userEnteredValue
    if effectiveValue is not None: returned_cell["effectiveValue"] = effectiveValue
    if formattedValue is not None: returned_cell["formattedValue"] = formattedValue
    if userEnteredFormat is not None: returned_cell["userEnteredFormat"] = userEnteredFormat
    if effectiveFormat is not None: returned_cell["effectiveFormat"] = effectiveFormat
    if hyperlink is not None: returned_cell["hyperlink"] = hyperlink
    if note is not None: returned_cell["note"] = note
    if textFormatRuns is not None: returned_cell["textFormatRuns"] = textFormatRuns
    if dataValidation is not None: returned_cell["dataValidation"] = dataValidation
    if pivotTable is not None: returned_cell["pivotTable"] = pivotTable
    if dataSourceTable is not None: returned_cell["dataSourceTable"] = dataSourceTable
    if dataSourceFormula is not None: returned_cell["dataSourceFormula"] = dataSourceFormula
    return returned_cell

def extendedValue(numberValue=None, stringValue=None, boolValue=None, formulaValue=None, errorValue=None):
    return_value = {}
    if numberValue is not None: return_value["numberValue"] = numberValue
    if stringValue is not None: return_value["stringValue"] = stringValue
    if boolValue is not None: return_value["boolValue"] = boolValue
    if formulaValue is not None: return_value["formulaValue"] = formulaValue
    if errorValue is not None: return_value["errorValue"] = errorValue
    return return_value

def cellFormat(
        numberFormat=None, backgroundColor=None, backgroundColorStyle=None, borders=None, padding=None,
        horizontalAlignment=None, verticalAlignment=None, wrapStrategy=None,
        textDirection=None, textFormat=None, hyperlinkDisplayType=None, textRotation=None
    ):
    return_format = {}
    if numberFormat is not None: return_format["numberFormat"] = numberFormat
    if backgroundColor is not None: return_format["backgroundColor"] = backgroundColor
    if backgroundColorStyle is not None: return_format["backgroundColorStyle"] = backgroundColorStyle
    if borders is not None: return_format["borders"] = borders
    if padding is not None: return_format["padding"] = padding
    if horizontalAlignment is not None: return_format["horizontalAlignment"] = horizontalAlignment
    if verticalAlignment is not None: return_format["verticalAlignment"] = verticalAlignment
    if wrapStrategy is not None: return_format["wrapStrategy"] = wrapStrategy
    if textDirection is not None: return_format["textDirection"] = textDirection
    if textFormat is not None: return_format["textFormat"] = textFormat
    if hyperlinkDisplayType is not None: return_format["hyperlinkDisplayType"] = hyperlinkDisplayType
    if textRotation is not None: return_format["textRotation"] = textRotation
    return return_format

def textFormat(
        foregroundColorStyle=None, fontFamily=None, fontSize=None,
        bold=None, italic=None, strikethrough=None, underline=None, link=None
    ):
    text_form = {}
    if foregroundColorStyle is not None: text_form["foregroundColorStyle"] = foregroundColorStyle
    if fontFamily is not None: text_form["fontFamily"] = fontFamily
    if fontSize is not None: text_form["fontSize"] = fontSize
    if bold is not None: text_form["bold"] = bold
    if italic is not None: text_form["italic"] = italic
    if strikethrough is not None: text_form["strikethrough"] = strikethrough
    if underline is not None: text_form["underline"] = underline
    if link is not None: text_form["link"] = link
    return text_form

def numberFormat(type=None, pattern=None):
    num_form = {}
    if type is not None: num_form["type"] = type
    if pattern is not None: num_form["pattern"] = pattern
    return num_form

def getColor(red=None, green=None, blue=None, alpha=None):
    color = {}
    if red is not None: color["red"] = red/255
    if green is not None: color["green"] = green/255
    if blue is not None: color["blue"] = blue/255
    if alpha is not None: color["alpha"] = alpha
    return color

def borders(top=None, bottom=None, left=None, right=None):
    border_form = {}
    if top is not None: border_form["top"] = top
    if bottom is not None: border_form["bottom"] = bottom
    if left is not None: border_form["left"] = left
    if right is not None: border_form["right"] = right
    return border_form

def border_style(style=None, width=None, color=None, colorStyle=None):
    style_return = {}
    if style is not None: style_return["style"] = style
    if width is not None: style_return["width"] = width
    if color is not None: style_return["color"] = color
    if colorStyle is not None: style_return["colorStyle"] = colorStyle
    return style_return

#https://stackoverflow.com/questions/49120625/list-concatenated-keys-of-a-nested-dictionary
def find_deep(dictionary, parent):
    ans = []
    for key in list(dictionary):
        # To make sure the first level doesn't get a preceding dot
        initial_dot = "" if parent == "" else "."

        if type(dictionary[key]) == dict:
            # The recursion progresses
            if parent not in ["userEnteredFormat.borders.right","userEnteredFormat.borders.left","userEnteredFormat.borders.top","userEnteredFormat.borders.bottom"]:
                ans.extend(find_deep(dictionary[key], initial_dot.join((parent, key))))
        else:
            # The recursion terminates
            ans.extend([initial_dot.join((parent, key))])
    
    return ans

def repeatCell(gridRange, cell):
    if (gridRange is not None and gridRange != {}) and (cell is not None and cell != {}):
        repeat_cell = {}
        repeat_cell["range"] = gridRange
        repeat_cell["cell"] = cell
        init_field = find_deep(cell, "")
        #print(init_field)
        fields = ", ".join(init_field)
        #print(fields)
        repeat_cell["fields"] = "%s" % fields
        
        #print(repeat_cell)
        return {"repeatCell": repeat_cell}
    else:
        print("Something missing in repeatCellRequest.")

def deleteSheet(sheetId):
    return {"deleteSheet":{"sheetId" : sheetId}}

def gridProp(row=None, column=None, frozenRow=None, frozenColumn=None, hideGrid=None, rowGroup=None, columnGroup=None):
    prop = {}
    if row is not None: prop["rowCount"] = row
    if column is not None: prop["columnCount"] = column
    if frozenRow is not None: prop["frozenRowCount"] = frozenRow
    if frozenColumn is not None: prop["frozenColumnCount"] = frozenColumn
    if hideGrid is not None: prop["hideGridLines"] = hideGrid
    if rowGroup is not None: prop["rowGroupControlAfter"] = rowGroup
    if columnGroup is not None: prop["columnGroupControlAfter"] = columnGroup
    return prop

def workbookProp(title=None, locale=None, autoRecalc=None, timeZone=None, defaultFormat=None, iterativeCalculationSettings=None, spreadsheetTheme=None):
    prop = {}
    if title is not None: prop["title"] = title
    if locale is not None: prop["locale"] = locale
    if autoRecalc is not None: prop["autoRecalc"] = autoRecalc
    if timeZone is not None: prop["timeZone"] = timeZone
    if defaultFormat is not None: prop["defaultFormat"] = defaultFormat
    if iterativeCalculationSettings is not None: prop["iterativeCalculationSettings"] = iterativeCalculationSettings
    if spreadsheetTheme is not None: prop["spreadsheetTheme"] = spreadsheetTheme
    return prop

def sheetProp(sheetId=None, title=None, index=None, sheetType=None, gridProperties=None, hidden=None, tabColor=None, tabColorStyle=None, rightToLeft=None, dataSourceSheetProperties=None):
    prop = {}
    if sheetId is not None: prop["sheetId"] = sheetId
    if title is not None: prop["title"] = title
    if index is not None: prop["index"] = index
    if sheetType is not None: prop["sheetType"] = sheetType
    if gridProperties is not None: prop["gridProperties"] = gridProperties
    if hidden is not None: prop["hidden"] = hidden
    if tabColor is not None: prop["tabColor"] = tabColor
    if tabColorStyle is not None: prop["tabColorStyle"] = tabColorStyle
    if rightToLeft is not None: prop["rightToLeft"] = rightToLeft
    if dataSourceSheetProperties is not None: prop["dataSourceSheetProperties"] = dataSourceSheetProperties
    return prop

def addSheet(properties=None):
    if properties is not None:
        return {"addSheet": {"properties" : properties}}
    else: print("No sheet properties given to addSheet call.")
    
def mergeCells(range=None, mergeType=None):
    merge = {}
    if range is not None: merge["range"] = range
    if mergeType is not None: merge["mergeType"] = mergeType
    return {"mergeCells" : merge}

def autoSize(sheetId=None, dimension=None, startIndex=None, endIndex=None):
    auto = {}
    if sheetId is not None: auto["sheetId"] = sheetId
    if dimension is not None: auto["dimension"] = dimension
    if startIndex is not None: auto["startIndex"] = startIndex
    if endIndex is not None: auto["endIndex"] = endIndex
    if len(auto) > 0: return {"autoResizeDimensions": {"dimensions": auto}}
    else:
        print("No auto-resizing information given.")

def condVal(userEnteredValue=None, relativeDate=None):
    v = {}
    if userEnteredValue is not None: v["userEnteredValue"] = userEnteredValue
    if relativeDate is not None: v["relativeDate"] = relativeDate
    return v

def boolCond(type=None, values=None):
    c = {}
    if type is not None: c["type"] = type
    if values is not None: c["values"] = values
    return c

def boolRule(condition=None, format=None):
    b = {}
    if condition is not None: b["condition"] = condition
    if format is not None: b["format"] = format
    return b

def rule(ranges=None, boolean=None, gradientRule=None):
    r = {}
    if ranges is not None: r["ranges"] = ranges
    if boolean is not None: r["booleanRule"] = boolean
    if gradientRule is not None: r["gradientRule"] = gradientRule
    return r

def addConditional(rule=None, index=None):
    cond = {}
    if rule is not None: cond["rule"] = rule
    if index is not None: cond["index"] = index
    return {"addConditionalFormatRule" : cond}

def updateConditional(index=None, rule=None, sheetId=None, newIndex=None):
    if sheetId is None ^ newIndex is None:
        print("Both Sheet ID and New Index must be given or left out together from updateConditional calls.")
    else:
        c = {}
        if index is not None: c["index"] = index
        if rule is not None: c["rule"] = rule
        if sheetId is not None: c["sheetId"] = sheetId
        if newIndex is not None: c["newIndex"] = newIndex
        return {"updateConditionalFormatRule": c}

def veryBasicFilter(gridRange=None):
    if gridRange is not None:
        return {"setBasicFilter" : { "filter": {"range" : gridRange}}}
    else:
        print("Very Basic Filter not configured correctly.")

def get_values(spreadsheet_id, range_name, val_render_option = 'FORMATTED_VALUE', date_render_option='SERIAL_NUMBER'):
        """
        Creates the batch_update the user has access to.
        Load pre-authorized user credentials from the environment.
        TODO(developer) - See https://developers.google.com/identity
        for guides on implementing OAuth2 for the application.\n"
            """
        
        # pylint: disable=maybe-no-member
        try:
            result = sheets.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id, range=range_name, valueRenderOption=val_render_option, dateTimeRenderOption=date_render_option).execute()
            rows = result.get('values', [])
            return rows
        except HttpError as error:
            print(f"An error occurred: {error}")
            return error

def update_values(spreadsheet_id, range_name, value_input_option, _values):
    """
    Creates the batch_update the user has access to.
    Load pre-authorized user credentials from the environment.
    TODO(developer) - See https://developers.google.com/identity
    for guides on implementing OAuth2 for the application.
        """
    try:
        values = [
            [
                # Cell values ...
            ],
            # Additional rows ...
        ]
        body = {
            'values': _values
        }
        result = sheets.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id, range=range_name,
            valueInputOption=value_input_option, body=body).execute()
        #print(f"{result.get('updatedCells')} cells updated.")
        return result
    except HttpError as error:
        print(f"An error occurred: {error}")
        return error

solid = border_style(style="SOLID")
thick = border_style(style="SOLID_THICK")
all_thick = [thick, thick, thick, thick]
all_solid = [solid, solid, solid, solid]
percent = numberFormat(type="PERCENT", pattern="0.00%")
currency = numberFormat(type="CURRENCY", pattern="$#,##0.00")
nmbr = numberFormat(type="NUMBER", pattern="#,##0.00")
greenVal = getColor(50,248,40,0.05)
redVal = getColor(225, 40, 40, 0.05)



class Department:
    def __init__(self, name, rpc):
        self.Name = name
        self.report_code = rpc
        self.year_sales = 0.0
        self.year_COGS = 0.0
        self.year_margin = 0.0
        self.quarterly_margin = []
        self.quarterly_sales = []
        self.quarterly_COGS = []
        self.weekly_sales = []
        self.weekly_COGS = []
        self.daily_sales = []
        self.daily_COGS = []
        self.monthly_sales = []
        self.monthly_COGS = []
        self.daily_totals = pd.DataFrame(data=None, columns=daily_data_frame_headers)
        self.weekly_totals = pd.DataFrame(data=None, columns=weekly_data_frame_headers)
        self.monthly_totals = pd.DataFrame(data=None, columns=monthly_data_frame_headers)
        self.quarterly_totals = pd.DataFrame(data=None, columns=quarterly_data_frame_headers)
        self.yearly_totals = pd.DataFrame(data=None, columns=yearly_data_frame_headers)
        self.quarters = [
            pd.Period('2023Q1'),pd.Period('2023Q2'),pd.Period('2023Q3'),pd.Period('2023Q4'),
            pd.Period('2024Q1'),pd.Period('2024Q2'),pd.Period('2024Q3'),pd.Period('2024Q4'),
            pd.Period('2025Q1'),pd.Period('2025Q2'),pd.Period('2025Q3'),pd.Period('2025Q4'),
            pd.Period('2026Q1'),pd.Period('2026Q2'),pd.Period('2026Q3'),pd.Period('2026Q4')
        ]
        self.months = [
            pd.Period('2023-01'),pd.Period('2023-02'),pd.Period('2023-03'),pd.Period('2023-04'),pd.Period('2023-05'),pd.Period('2023-06'),pd.Period('2023-07'),pd.Period('2023-08'),pd.Period('2023-09'),pd.Period('2023-10'),pd.Period('2023-11'),pd.Period('2023-12'),
            pd.Period('2024-01'),pd.Period('2024-02'),pd.Period('2024-03'),pd.Period('2024-04'),pd.Period('2024-05'),pd.Period('2024-06'),pd.Period('2024-07'),pd.Period('2024-08'),pd.Period('2024-09'),pd.Period('2024-10'),pd.Period('2024-11'),pd.Period('2024-12'),
            pd.Period('2025-01'),pd.Period('2025-02'),pd.Period('2025-03'),pd.Period('2025-04'),pd.Period('2025-05'),pd.Period('2025-06'),pd.Period('2025-07'),pd.Period('2025-08'),pd.Period('2025-09'),pd.Period('2025-10'),pd.Period('2025-11'),pd.Period('2025-12'),
            pd.Period('2026-01'),pd.Period('2026-02'),pd.Period('2026-03'),pd.Period('2026-04'),pd.Period('2026-05'),pd.Period('2026-06'),pd.Period('2026-07'),pd.Period('2026-08'),pd.Period('2026-09'),pd.Period('2026-10'),pd.Period('2026-11'),pd.Period('2026-12')
        ]
        self.years = ["2023", "2024", "2025", "2026"]
        self.workbook_id = ""
        self.daily_sh_id = ""
        self.weekly_sh_id = ""
        self.monthly_sh_id = ""
        self.yearly_sh_id = ""
    
    def build_book(self, folder_id, driveClient):
        output_workbook = createBook(self.Name + " Dashboard", folder_id, 'application/vnd.google-apps.spreadsheet', driveClient)
        
        self.workbook_id = output_workbook.get('id', '')
        tabs = {"Daily" : 57, "Weekly" : 52, "Monthly" : 52, "Quarterly" : 47, "Yearly" : 47}
        for tab_name in tabs:
            response = batchUpdate(self.workbook_id, body=addSheet(sheetProp(title=tab_name, gridProperties=gridProp(column=tabs[tab_name], frozenRow=2, frozenColumn=1))))
            
            match tab_name:
                case "Daily": self.daily_sh_id = response.get('replies', '')[0].get('addSheet', '').get('properties', '').get('sheetId', '')
                case "Weekly": self.weekly_sh_id = response.get('replies', '')[0].get('addSheet', '').get('properties', '').get('sheetId', '')
                case "Monthly": self.monthly_sh_id = response.get('replies', '')[0].get('addSheet', '').get('properties', '').get('sheetId', '')
                case "Quarterly": self.quarterly_sh_id = response.get('replies', '')[0].get('addSheet', '').get('properties', '').get('sheetId', '')
                case "Yearly": self.yearly_sh_id = response.get('replies', '')[0].get('addSheet', '').get('properties', '').get('sheetId', '')
        self.build_plan_sheet()
        batchUpdate(self.workbook_id, deleteSheet(0))
    def sheet_template(self, period):
        
        match period:
            case "Daily":
                df = self.daily_totals
                heads = mergeDailyHeaders
                merge_list = daily_merge_ranges
                sheet_id = self.daily_sh_id
                ninety_day_offset = [4, 2]
            case "Weekly":
                df = self.weekly_totals
                heads = mergeWeeklyMonthlyHeaders
                merge_list = weekly_monthly_merge_ranges
                sheet_id = self.weekly_sh_id
                ninety_day_offset = [3, 1]
            case "Monthly":
                df = self.monthly_totals
                heads = mergeWeeklyMonthlyHeaders
                merge_list = weekly_monthly_merge_ranges
                sheet_id = self.monthly_sh_id
                ninety_day_offset = [3, 1]
            case "Quarterly":
                df = self.quarterly_totals
                heads = mergeQuarterlyHeaders
                merge_list = quarterly_merge_ranges
                sheet_id = self.quarterly_sh_id
                ninety_day_offset = [0, 0]
            case "Yearly":
                df = self.yearly_totals
                merge_list = yearly_merge_ranges
                sheet_id = self.yearly_sh_id
                ninety_day_offset = [0, 0]
        df = df.drop(['Dept'], inplace=True)
        append(self.workbook_id, period+"!A1:B1", "ROWS", heads, "USER_ENTERED")
        append(self.workbook_id, period+"!A3:B3", "ROWS", df.values.tolist(), "USER_ENTERED")
        format_updates = [repeatCell(
                            gridRange(sheet_id, 2, df.shape[0]+2, 0, 1),
                            cellData(userEnteredFormat=cellFormat(
                                                        numberFormat=numberFormat("DATE", "m/d/yy"),
                                                        borders=borders(*all_thick),
                                                        textFormat=textFormat(fontFamily="Arial", fontSize=12, bold=True)
                                                    )))
                    ]
        first_format = []
        #second row of headers                                    
        first_format.append(repeatCell(gridRange(sheet_id, 1, 2, 1, merge_list[9][1]),
                                        cellData(userEnteredFormat=cellFormat(
                                            borders=borders(*all_thick),
                                            horizontalAlignment="CENTER",
                                            verticalAlignment="MIDDLE",
                                            textFormat=textFormat(fontFamily="Arial", fontSize=12, bold=True),
                                            backgroundColor=getColor(239, 175, 0, 0.1)
                                        ))))
        #broad data formatting
        first_format.append(repeatCell(gridRange(sheet_id, 2, df.shape[0]+2, 0, merge_list[9][1]),
                                        cellData(userEnteredFormat=cellFormat(
                                            borders=borders(*all_solid),
                                            textFormat=textFormat(fontFamily="Arial", fontSize=12)
                                        ))))
        batchUpdate(self.workbook_id, first_format)
        
        k = 0
        for i in merge_list:
            firstRow = gridRange(sheet_id, 0, 1, i[0], i[1])
            
            format_updates.append(mergeCells(firstRow, "MERGE_ALL"))
            format_updates.append(repeatCell(firstRow, cellData(userEnteredFormat=cellFormat(
                                                borders=borders(*all_thick),
                                                horizontalAlignment="CENTER",
                                                verticalAlignment="MIDDLE",
                                                textFormat=textFormat(fontFamily="Arial", fontSize=14, bold=True),
                                                backgroundColor=getColor(239, 175, 0, 0.1)
                                            ))))
            format_updates.append(repeatCell(gridRange(sheet_id, 2, df.shape[0]+2, i[1]-1, i[1]), cellData(userEnteredFormat=cellFormat(borders=borders(right=thick)))))
            #Sales, COGS, Profit
            if k in [0, 1, 3]:
                format_updates.append(repeatCell(gridRange(sheet_id, 2, df.shape[0]+2, i[0], i[0]+2), cellData(userEnteredFormat=cellFormat(numberFormat=currency))))
                format_updates.append(repeatCell(gridRange(sheet_id, 2, df.shape[0]+2, i[0]+2, i[0]+3), cellData(userEnteredFormat=cellFormat(numberFormat=percent))))
                format_updates.append(repeatCell(gridRange(sheet_id, 2, df.shape[0]+2, i[0]+3, i[1]-1), cellData(userEnteredFormat=cellFormat(numberFormat=currency))))
                format_updates.append(repeatCell(gridRange(sheet_id, 2, df.shape[0]+2, i[1]-1, i[1]), cellData(userEnteredFormat=cellFormat(numberFormat=percent))))
                #https://stackoverflow.com/questions/58129090/how-to-append-a-relative-formula-into-a-sheet-using-the-google-sheet-api
                if ninety_day_offset[0] > 0:
                    format_updates.append(addConditional(rule(gridRange(sheet_id, 2, df.shape[0]+2, i[1]-3, i[1]-2), boolRule(boolCond("CUSTOM_FORMULA", [condVal("=INDIRECT(\"R[0]C[-%s]\", FALSE)>INDIRECT(\"R[0]C[0]\", FALSE)" % ninety_day_offset[0])]), cellFormat(backgroundColor=greenVal))), 0))
                    format_updates.append(addConditional(rule(gridRange(sheet_id, 2, df.shape[0]+2, i[1]-3, i[1]-2), boolRule(boolCond("CUSTOM_FORMULA", [condVal("=INDIRECT(\"R[0]C[-%s]\", FALSE)<INDIRECT(\"R[0]C[0]\", FALSE)" % ninety_day_offset[0])]), cellFormat(backgroundColor=redVal))), 0))
                format_updates.append(addConditional(rule(gridRange(sheet_id, 2, df.shape[0]+2, i[0]+2, i[0]+3), boolRule(boolCond("NUMBER_GREATER", [condVal("0")]), cellFormat(backgroundColor=greenVal))), 0))
                format_updates.append(addConditional(rule(gridRange(sheet_id, 2, df.shape[0]+2, i[0]+2, i[0]+3), boolRule(boolCond("NUMBER_LESS", [condVal("0")]), cellFormat(backgroundColor=redVal))), 0))
                format_updates.append(addConditional(rule(gridRange(sheet_id, 2, df.shape[0]+2, i[1]-1, i[1]), boolRule(boolCond("NUMBER_GREATER", [condVal("0")]), cellFormat(backgroundColor=greenVal))), 0))
                format_updates.append(addConditional(rule(gridRange(sheet_id, 2, df.shape[0]+2, i[1]-1, i[1]), boolRule(boolCond("NUMBER_LESS", [condVal("0")]), cellFormat(backgroundColor=redVal))), 0))
            #Margin
            elif k == 2:
                format_updates.append(repeatCell(gridRange(sheet_id, 2, df.shape[0]+2, i[0], i[1]+1), cellData(userEnteredFormat=cellFormat(numberFormat=percent))))
                if ninety_day_offset[0] > 0:
                    format_updates.append(addConditional(rule(gridRange(sheet_id, 2, df.shape[0]+2, i[1]-3, i[1]-2), boolRule(boolCond("CUSTOM_FORMULA", [condVal("=INDIRECT(\"R[0]C[-%s]\", FALSE)>INDIRECT(\"R[0]C[0]\", FALSE)" % ninety_day_offset[0])]), cellFormat(backgroundColor=greenVal))), 0))
                    format_updates.append(addConditional(rule(gridRange(sheet_id, 2, df.shape[0]+2, i[1]-3, i[1]-2), boolRule(boolCond("CUSTOM_FORMULA", [condVal("=INDIRECT(\"R[0]C[-%s]\", FALSE)<INDIRECT(\"R[0]C[0]\", FALSE)" % ninety_day_offset[0])]), cellFormat(backgroundColor=redVal))), 0))
                format_updates.append(addConditional(rule(gridRange(sheet_id, 2, df.shape[0]+2, i[0]+2, i[0]+3), boolRule(boolCond("NUMBER_GREATER", [condVal("0")]), cellFormat(backgroundColor=greenVal))), 0))
                format_updates.append(addConditional(rule(gridRange(sheet_id, 2, df.shape[0]+2, i[0]+2, i[0]+3), boolRule(boolCond("NUMBER_LESS", [condVal("0")]), cellFormat(backgroundColor=redVal))), 0))
                format_updates.append(addConditional(rule(gridRange(sheet_id, 2, df.shape[0]+2, i[1]-1, i[1]), boolRule(boolCond("NUMBER_GREATER", [condVal("0")]), cellFormat(backgroundColor=greenVal))), 0))
                format_updates.append(addConditional(rule(gridRange(sheet_id, 2, df.shape[0]+2, i[1]-1, i[1]), boolRule(boolCond("NUMBER_LESS", [condVal("0")]), cellFormat(backgroundColor=redVal))), 0))
            #Volume
            elif k == 4:
                format_updates.append(repeatCell(gridRange(sheet_id, 2, df.shape[0]+2, i[0], i[1]-1), cellData(userEnteredFormat=cellFormat(numberFormat=nmbr))))
                format_updates.append(repeatCell(gridRange(sheet_id, 2, df.shape[0]+2, i[1]-1, i[1]), cellData(userEnteredFormat=cellFormat(numberFormat=percent))))
                if ninety_day_offset[0] > 0:
                    format_updates.append(addConditional(rule(gridRange(sheet_id, 2, df.shape[0]+2, i[1]-3, i[1]-2), boolRule(boolCond("CUSTOM_FORMULA", [condVal("=INDIRECT(\"R[0]C[-%s]\", FALSE)>INDIRECT(\"R[0]C[0]\", FALSE)" % ninety_day_offset[1])]), cellFormat(backgroundColor=greenVal))), 0))
                    format_updates.append(addConditional(rule(gridRange(sheet_id, 2, df.shape[0]+2, i[1]-3, i[1]-2), boolRule(boolCond("CUSTOM_FORMULA", [condVal("=INDIRECT(\"R[0]C[-%s]\", FALSE)<INDIRECT(\"R[0]C[0]\", FALSE)" % ninety_day_offset[1])]), cellFormat(backgroundColor=redVal))), 0))
                format_updates.append(addConditional(rule(gridRange(sheet_id, 2, df.shape[0]+2, i[1]-1, i[1]), boolRule(boolCond("NUMBER_GREATER", [condVal("0")]), cellFormat(backgroundColor=greenVal))), 0))
                format_updates.append(addConditional(rule(gridRange(sheet_id, 2, df.shape[0]+2, i[1]-1, i[1]), boolRule(boolCond("NUMBER_LESS", [condVal("0")]), cellFormat(backgroundColor=redVal))), 0))
            
            k+=1
            batchUpdate(self.workbook_id, body=format_updates)
    def build_sheet(self, period):
        if self.workbook_id != "":
            self.sheet_template(period)
        else:
            print("No workbook associated with this department yet.")
    def build_plan_sheet(self):
        if self.workbook_id != "":
            response = batchUpdate(self.workbook_id, body=addSheet(sheetProp(title="Plan Figures FY23", gridProperties=gridProp(row=6, column=4))))

            sheet_id = response.get('replies', '')[0].get('addSheet', '').get('properties', '').get('sheetId', '')

            append(self.workbook_id, "Plan Figures FY23!A1:B1", "ROWS", [['Period', 'Margin', 'Sales', 'COGS']])
            update_values(self.workbook_id, "Plan Figures FY23!A1:D1", 'USER_ENTERED', [['Period', 'Margin', 'Sales', 'COGS']])
            update_values(self.workbook_id, "Plan Figures FY23!A2:A", 'USER_ENTERED', [['Yearly'], ['Q1'], ['Q2'], ['Q3'], ['Q4']])
            update_values(self.workbook_id, "Plan Figures FY23!B2:B", 'USER_ENTERED', [[el] for el in [self.year_margin] + self.quarterly_margin])
            update_values(self.workbook_id, "Plan Figures FY23!C2:C", 'USER_ENTERED', [[el] for el in [self.year_sales] + self.quarterly_sales])
            update_values(self.workbook_id, "Plan Figures FY23!D2:D", 'USER_ENTERED', [[el] for el in [self.year_COGS] + self.quarterly_COGS])
            
            format_body = [
                repeatCell( gridRange(sheet_id, 0, 1, 0, 6),
                            cellData(userEnteredFormat=cellFormat(
                                                            borders=borders(*all_thick),
                                                            horizontalAlignment="CENTER",
                                                            verticalAlignment="MIDDLE",
                                                            textFormat=textFormat(fontFamily="Arial", fontSize=14, bold=True)
                                                        )
                                )
                    ),
                repeatCell(gridRange(sheet_id, 1, 6, 0, 6), cellData(userEnteredFormat=cellFormat(borders=borders(*all_solid), textFormat=textFormat(fontFamily="Arial", fontSize=12)))),
                repeatCell(gridRange(sheet_id, 1, 6, 1, 2), cellData(userEnteredFormat=cellFormat(numberFormat=percent))),
                repeatCell(gridRange(sheet_id, 1, 6, 2, 4), cellData(userEnteredFormat=cellFormat(numberFormat=currency))),
                autoSize(sheet_id, "COLUMNS")
            ]
            
            batchUpdate(self.workbook_id, body=format_body)
        else:
            print("No workbook built for this department yet.")
    def query(self, query):
        match query:
            case "Daily":
                self.daily_totals
    
    def populate_plan_data(self, plan):
        lookup_name = ""
        if self.Name != "Store Wide":
            lookup_name = self.Name + " Department" if self.Name != "Taxable" else "Taxable Grocery"
        else:
            lookup_name = self.Name
        if lookup_name != "":
            dept_plan_data = [x for x in plan if x[0] == lookup_name]
            if(len(dept_plan_data)==1):
                data = dept_plan_data[0]
                self.year_sales = data[1]
                self.year_COGS = data[2]
                self.year_margin = data[3]
                self.quarterly_margin = data[4:8]
                self.quarterly_sales = data[8:12]
                self.quarterly_COGS = data[12:16]
                self.weekly_sales = data[16:20]
                self.weekly_COGS = data[20:24]
                self.daily_sales = data[24:28]
                self.daily_COGS = data[28:32]
                self.monthly_sales = data[32:44]
                self.monthly_COGS = data[44:56]
            else:
                print("weird lookup")
        else:
            print("broke")
    def populate_single_period(self, period, cnxn: pyodbc.Connection, labor: LaborReport):
        starting_quarter = pd.Period('2023Q1')
        starting_month = pd.Period('2023-01')
        actual_date = dt.datetime.today()
        if self.report_code != 0:
            match period:
                case "Daily":
                    daily = pd.read_sql("""
                    select revenueReport.F254 as 'Day',
                    reportCodeTable.F1024 as 'Dept',
                    sum(revenueReport.F65) as 'Revenue',
                    LAG(sum(revenueReport.F65), 7) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03) as 'LW Revenue',
                    ROUND(AVG(sum(revenueReport.F65)) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03 ROWS BETWEEN 88 PRECEDING AND 1 PRECEDING), 2) as 'Ninety_Trailing_Revenue',
                    LAG(sum(revenueReport.F65), 358) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03) as 'LY REVENUE',
                    ROUND((sum(revenueReport.F65)/LAG(sum(revenueReport.F65), 358) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03)-1), 4) as 'REVENUE_GROWTH',
                    costReport.F65 as 'Cost',
                    LAG(costReport.F65, 7) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03) as 'LW Cost',
                    ROUND(AVG(costReport.F65) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03 ROWS BETWEEN 88 PRECEDING AND 1 PRECEDING), 2) as 'Ninety_Trailing_Cost',
                    LAG(costReport.F65, 358) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03) as 'LY COST',
                    ROUND((costReport.F65/LAG(costReport.F65, 358) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03)-1), 4) as 'COST_GROWTH',
                    ROUND(((sum(revenueReport.F65) - costReport.F65) / sum(revenueReport.F65)), 4) as 'Margin',
                    ROUND(AVG(((sum(revenueReport.F65)-costReport.F65)/sum(revenueReport.F65))) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03 ROWS BETWEEN 88 PRECEDING AND 1 PRECEDING), 4) as 'Ninety_Trailing_Margin',
                    LAG(ROUND(((sum(revenueReport.F65) - costReport.F65) / sum(revenueReport.F65)), 4), 7) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03) as 'LW Margin',
                    LAG(ROUND(((sum(revenueReport.F65) - costReport.F65) / sum(revenueReport.F65)), 4), 358) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03) as 'LY MARGIN',
                    ROUND(((sum(revenueReport.F65) - costReport.F65) / sum(revenueReport.F65)), 4) - LAG(ROUND(((sum(revenueReport.F65) - costReport.F65) / sum(revenueReport.F65)), 4), 358) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03) as 'YoY Diff',
                    sum(revenueReport.F65) - costReport.F65 as 'Profit',
                    LAG(sum(revenueReport.F65) - costReport.F65, 7) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03) as 'LW Profit',
                    ROUND((AVG(sum(revenueReport.F65)-costReport.F65) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03 ROWS BETWEEN 88 PRECEDING AND 1 PRECEDING)), 4) as 'Ninety_Trailing_Profit',
                    LAG(sum(revenueReport.F65) - costReport.F65, 358) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03) as 'LY PROFIT',
                    ROUND(((sum(revenueReport.F65) - costReport.F65)/LAG(sum(revenueReport.F65) - costReport.F65, 358) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03) - 1), 4) as 'PROFIT_GROWTH',
                    sum(revenueReport.F64) as 'Volume',
                    LAG(sum(revenueReport.F64), 7) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03) as 'LW Volume',
                    ROUND(AVG(sum(revenueReport.F64)) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03 ROWS BETWEEN 88 PRECEDING AND 1 PRECEDING), 2) as 'Ninety_Trailing_Volume',
                    LAG(sum(revenueReport.F64), 358) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03) as 'LY VOLUME',
                    ROUND((sum(revenueReport.F64)/LAG(sum(revenueReport.F64), 358) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03)-1), 4) as 'VOLUME_GROWTH'
                    from
                    (select * from STORESQL.dbo.RPT_DPT 
                    where F1031 = 'D' and F1034 in (3, 3303) and F03 = %s) revenueReport
                    inner join (select * from STORESQL.dbo.RPT_DPT where F1031 = 'D' and F1034 = 8101 and F03 = %s ) costReport on revenueReport.F254 = costReport.F254 and revenueReport.F03 = costReport.F03
                    inner join (select F18, F1024 from STORESQL.dbo.RPC_TAB) reportCodeTable on revenueReport.F03 = reportCodeTable.F18
                    where revenueReport.F03 not in (14, 21, 23, 97, 98, 99) 
                    group by revenueReport.F254, revenueReport.F03, costReport.F254, costReport.F03, costReport.F65, reportCodeTable.F1024
                    order by revenueReport.F254 desc, reportCodeTable.F1024
                    """ % (self.report_code, self.report_code),cnxn)
                    
                    daily['Revenue'] = daily['Revenue'].astype(float)
                    daily['Q'] = daily['Day'].apply(lambda x: (pd.to_datetime(x) + pd.offsets.QuarterEnd(n=3)).to_period("Q"))
                    daily['Plan Daily Sales'] = daily['Q'].apply(lambda x: float(self.daily_sales[self.quarters.index(x)]) if x >= starting_quarter else np.nan)
                    daily['RevPercentPlan'] = (daily['Revenue']/daily['Plan Daily Sales']) - 1
                    daily['Plan Daily COGS'] = daily['Q'].apply(lambda x: float(self.daily_COGS[self.quarters.index(x)]) if x >= starting_quarter else np.nan)
                    daily['COGSPercentPlan'] = (daily['Cost']/daily['Plan Daily COGS']) - 1
                    daily['Plan Profit'] = daily['Plan Daily Sales'] - daily['Plan Daily COGS']
                    daily['ProfitPercentPlan'] = (daily['Profit']/daily['Plan Profit']) - 1
                    daily['Plan Margin'] = daily['Q'].apply(lambda x: float(self.quarterly_margin[self.quarters.index(x)]) if x >= starting_quarter else np.nan)
                    daily['MARGINPercentPlan'] = (daily['Margin'] - daily['Plan Margin'])
                    daily.fillna('', inplace=True)
                    daily = daily.astype({"Day" : str})
                    daily = daily[[
                                'Day', 'Dept', 'Revenue', 'Plan Daily Sales', 'RevPercentPlan', 'LW Revenue', 'Ninety_Trailing_Revenue', 'LY REVENUE', 'REVENUE_GROWTH',
                                'Cost', 'Plan Daily COGS', 'COGSPercentPlan', 'LW Cost', 'Ninety_Trailing_Cost', 'LY COST', 'COST_GROWTH',
                                'Margin', 'Plan Margin', 'MARGINPercentPlan', 'LW Margin', 'Ninety_Trailing_Margin', 'LY MARGIN', 'YoY Diff',
                                'Profit', 'Plan Profit', 'ProfitPercentPlan', 'LW Profit', 'Ninety_Trailing_Profit', 'LY PROFIT', 'PROFIT_GROWTH',
                                'Volume', 'LW Volume', 'Ninety_Trailing_Volume', 'LY VOLUME', 'VOLUME_GROWTH'
                            ]]
                    self.daily_totals = daily
                case "Weekly":
                    weekly = pd.read_sql("""
                                select
                                1.0 - CAST(DATEDIFF(day, GETDATE(), '2022-09-18')/7.0 as float) as 'Annualize',
                                revenueReport.F254 as 'Day',
                                reportCodeTable.F1024 as 'Dept',
                                sum(revenueReport.F65) as 'Revenue',
                                ROUND(AVG(sum(revenueReport.F65)) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03 ROWS BETWEEN 13 PRECEDING AND 1 PRECEDING), 2) as 'Ninety_Trailing_Revenue',
                                LAG(sum(revenueReport.F65), 52) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03) as 'LY REVENUE',
                                ROUND((sum(revenueReport.F65)/LAG(sum(revenueReport.F65), 52) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03)-1), 4) as 'REVENUE_GROWTH',
                                costReport.F65 as 'Cost',
                                ROUND(AVG(costReport.F65) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03 ROWS BETWEEN 13 PRECEDING AND 1 PRECEDING), 2) as 'Ninety_Trailing_Cost',
                                LAG(costReport.F65, 52) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03) as 'LY COST',
                                ROUND((costReport.F65/LAG(costReport.F65, 52) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03)-1), 4) as 'COST_GROWTH',
                                ROUND(((sum(revenueReport.F65) - costReport.F65) / sum(revenueReport.F65)), 4) as 'Margin',
                                ROUND(AVG(((sum(revenueReport.F65)-costReport.F65)/sum(revenueReport.F65))) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03 ROWS BETWEEN 13 PRECEDING AND 1 PRECEDING), 4) as 'Ninety_Trailing_Margin',
                                LAG(ROUND(((sum(revenueReport.F65) - costReport.F65) / sum(revenueReport.F65)), 4), 52) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03) as 'LY MARGIN',
                                ROUND(((sum(revenueReport.F65) - costReport.F65) / sum(revenueReport.F65)), 4) - LAG(ROUND(((sum(revenueReport.F65) - costReport.F65) / sum(revenueReport.F65)), 4), 52) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03) as 'YoY Diff',
                                sum(revenueReport.F65) - costReport.F65 as 'Profit',
                                ROUND((AVG(sum(revenueReport.F65)-costReport.F65) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03 ROWS BETWEEN 13 PRECEDING AND 1 PRECEDING)), 4) as 'Ninety_Trailing_Profit',
                                LAG(sum(revenueReport.F65) - costReport.F65, 52) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03) as 'LY PROFIT',
                                ROUND(((sum(revenueReport.F65) - costReport.F65)/LAG(sum(revenueReport.F65) - costReport.F65, 52) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03) - 1), 4) as 'PROFIT_GROWTH',
                                sum(revenueReport.F64) as 'Volume',
                                ROUND(AVG(sum(revenueReport.F64)) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03 ROWS BETWEEN 13 PRECEDING AND 1 PRECEDING), 2) as 'Ninety_Trailing_Volume',
                                LAG(sum(revenueReport.F64), 52) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03) as 'LY VOLUME',
                                ROUND((sum(revenueReport.F64)/LAG(sum(revenueReport.F64), 52) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03)-1), 4) as 'VOLUME_GROWTH'
                                from
                                (select * from STORESQL.dbo.RPT_DPT 
                                where F1031 = 'W' and F1034 in (3, 3303) and F03 = %s) revenueReport
                                inner join (select * from STORESQL.dbo.RPT_DPT where F1031 = 'W' and F1034 = 8101 and F03 = %s) costReport on revenueReport.F254 = costReport.F254 and revenueReport.F03 = costReport.F03
                                inner join (select F18, F1024 from STORESQL.dbo.RPC_TAB) reportCodeTable on revenueReport.F03 = reportCodeTable.F18
                                where revenueReport.F03 not in (14, 21, 23, 97, 98, 99) 
                                group by revenueReport.F254, revenueReport.F03, costReport.F254, costReport.F03, costReport.F65, reportCodeTable.F1024
                                order by revenueReport.F254 desc, reportCodeTable.F1024
                        """ % (self.report_code, self.report_code),cnxn)
                    
                    weekly['Revenue'] = weekly['Revenue'].astype(float)
                    weekly['DOW'] = weekly['Day'].apply(lambda x: x.date().weekday())
                    weekly['Q'] = weekly['Day'].apply(lambda x: (pd.to_datetime(x) + pd.offsets.QuarterEnd(n=3)).to_period("Q"))
                    weekly['Plan Weekly Sales'] = weekly['Q'].apply(lambda x: float(self.weekly_sales[self.quarters.index(x)]) if x >= starting_quarter else np.nan)
                    weekly['RevPercentPlan'] = weekly.apply(lambda x: (x['Revenue']/(x['Plan Weekly Sales'] * x['Annualize'])) - 1 if pd.to_datetime(x['Day']) > actual_date else (x['Revenue']/(x['Plan Weekly Sales']) - 1), axis=1)
                    weekly['Plan Weekly COGS'] = weekly['Q'].apply(lambda x: float(self.weekly_COGS[self.quarters.index(x)]) if x >= starting_quarter else np.nan)
                    weekly['COGSPercentPlan'] = weekly.apply(lambda x: (x['Cost']/(x['Plan Weekly COGS'] * x['Annualize'])) - 1 if pd.to_datetime(x['Day']) > actual_date else (x['Cost']/(x['Plan Weekly COGS']) - 1), axis=1)
                    weekly['Plan Profit'] = weekly['Plan Weekly Sales'] - weekly['Plan Weekly COGS']
                    weekly['ProfitPercentPlan'] = weekly.apply(lambda x: (x['Profit']/(x['Plan Profit'] * x['Annualize'])) - 1 if pd.to_datetime(x['Day']) > actual_date else (x['Profit']/(x['Plan Profit']) - 1), axis=1)
                    weekly['Plan Margin'] = weekly['Q'].apply(lambda x: float(self.quarterly_margin[self.quarters.index(x)]) if x >= starting_quarter else np.nan)
                    weekly['MARGINPercentPlan'] = (weekly['Margin'] - weekly['Plan Margin'])
                    weekly['REVENUE_GROWTH'] = weekly.apply(lambda x: (x['Revenue']/(x['LY REVENUE'] * x['Annualize'])) - 1 if pd.to_datetime(x['Day']) > actual_date else x['REVENUE_GROWTH'], axis=1)
                    weekly['COST_GROWTH'] = weekly.apply(lambda x: (x['Cost']/(x['LY COST'] * x['Annualize'])) - 1 if pd.to_datetime(x['Day']) > actual_date else x['COST_GROWTH'], axis=1)
                    weekly['PROFIT_GROWTH'] = weekly.apply(lambda x: (x['Profit']/(x['LY PROFIT'] * x['Annualize'])) - 1 if pd.to_datetime(x['Day']) > actual_date else x['PROFIT_GROWTH'], axis=1)
                    weekly['VOLUME_GROWTH'] = weekly.apply(lambda x: (x['Volume']/(x['LY VOLUME'] * x['Annualize'])) - 1 if pd.to_datetime(x['Day']) > actual_date else x['VOLUME_GROWTH'], axis=1)
                    weekly.fillna('', inplace=True)
                    weekly = weekly.astype({"Day" : str})
                    weekly = weekly[[
                                'Annualize','Day', 'Dept', 'Revenue', 'Plan Weekly Sales', 'RevPercentPlan', 'Ninety_Trailing_Revenue', 'LY REVENUE', 'REVENUE_GROWTH',
                                'Cost', 'Plan Weekly COGS', 'COGSPercentPlan', 'Ninety_Trailing_Cost', 'LY COST', 'COST_GROWTH',
                                'Margin', 'Plan Margin', 'MARGINPercentPlan', 'Ninety_Trailing_Margin', 'LY MARGIN', 'YoY Diff',
                                'Profit', 'Plan Profit', 'ProfitPercentPlan', 'Ninety_Trailing_Profit', 'LY PROFIT', 'PROFIT_GROWTH',
                                'Volume', 'Ninety_Trailing_Volume', 'LY VOLUME', 'VOLUME_GROWTH'
                            ]]
                    if self.Name in labor.report_look_ups:
                        weekly = weekly.merge(labor.report(self.Name)[["Week", "Worked Wages", "Worked Hours"]].astype({'Week':str}), how='left', left_on='Day', right_on='Week')
                        weekly['TotalGPVariance'] = ""
                        weekly['VarDuetoMargin'] = ""
                        weekly['VarDueToSalesVolume'] = ""
                        weekly['LaborCost'] = weekly['Worked Wages']
                        weekly['PlanLaborCost'] = ""
                        weekly['PercentPlanLaborCost'] = ""
                        weekly['LYLaborCost'] = ""
                        weekly['LaborCostGrowth'] = ""
                        weekly['LaborHours'] = weekly['Worked Hours']
                        weekly['PlanLaborHours'] = ""
                        weekly['PercentPlanLaborHours'] = ""
                        weekly['LYLaborHours'] = ""
                        weekly['LaborHoursGrowth'] = ""
                        weekly['SalesToLaborHours'] = weekly['LaborCost'] / weekly['LaborHours']
                        weekly['PlanSalesToLaborHours'] = ""
                        weekly['PercentPlanSalesToLaborHours'] = ""
                        weekly['LYSalesToLaborHours'] = ""
                        weekly['SalesToLaborHoursGrowth'] = ""
                        weekly['MarginAfterLabor'] = ((weekly['Revenue']-weekly['LaborCost']) - weekly['Cost'])/weekly['Revenue']
                        weekly['PlanMarginAfterLabor'] = ""
                        weekly['PercentPlanMarginAfterLabor'] = ""
                        weekly['LYMarginAfterLabor'] = ""
                        weekly['MarginAfterLaborGrowth'] = ""
                        weekly.fillna("", inplace=True)
                    else:
                        weekly['Week'] = ""
                        weekly['Worked Wages'] = ""
                        weekly['Worked Hours'] = ""

                        weekly['TotalGPVariance'] = ""
                        weekly['VarDuetoMargin'] = ""
                        weekly['VarDueToSalesVolume'] = ""
                        weekly['LaborCost'] = ""
                        weekly['PlanLaborCost'] = ""
                        weekly['PercentPlanLaborCost'] = ""
                        weekly['LYLaborCost'] = ""
                        weekly['LaborCostGrowth'] = ""
                        weekly['LaborHours'] = ""
                        weekly['PlanLaborHours'] = ""
                        weekly['PercentPlanLaborHours'] = ""
                        weekly['LYLaborHours'] = ""
                        weekly['LaborHoursGrowth'] = ""
                        weekly['SalesToLaborHours'] = ""
                        weekly['PlanSalesToLaborHours'] = ""
                        weekly['PercentPlanSalesToLaborHours'] = ""
                        weekly['LYSalesToLaborHours'] = ""
                        weekly['SalesToLaborHoursGrowth'] = ""
                        weekly['MarginAfterLabor'] = ""
                        weekly['PlanMarginAfterLabor'] = ""
                        weekly['PercentPlanMarginAfterLabor'] = ""
                        weekly['LYMarginAfterLabor'] = ""
                        weekly['MarginAfterLaborGrowth'] = ""
                    #    weekly['Week','Worked Wages','Worked Hours','TotalGPVariance','VarDuetoMargin','VarDueToSalesVolume','LaborCost','PlanLaborCost','PercentPlanLaborCost',
                    #    'LYLaborCost','LaborCostGrowth','LaborHours','PlanLaborHours','PercentPlanLaborHours','LYLaborHours','LaborHoursGrowth','SalesToLaborHours',
                    #    'PlanSalesToLaborHours','PercentPlanSalesToLaborHours','LYSalesToLaborHours','SalesToLaborHoursGrowth','MarginAfterLabor','PlanMarginAfterLabor',
                    #    'PercentPlanMarginAfterLabor','LYMarginAfterLabor','MarginAfterLaborGrowth'] = ""

                    weekly = weekly[[
                                'Annualize','Day', 'Dept', 'Revenue', 'Plan Weekly Sales', 'RevPercentPlan', 'Ninety_Trailing_Revenue', 'LY REVENUE', 'REVENUE_GROWTH',
                                'Cost', 'Plan Weekly COGS', 'COGSPercentPlan', 'Ninety_Trailing_Cost', 'LY COST', 'COST_GROWTH',
                                'Margin', 'Plan Margin', 'MARGINPercentPlan', 'Ninety_Trailing_Margin', 'LY MARGIN', 'YoY Diff',
                                'Profit', 'Plan Profit', 'ProfitPercentPlan', 'Ninety_Trailing_Profit', 'LY PROFIT', 'PROFIT_GROWTH',
                                'Volume', 'Ninety_Trailing_Volume', 'LY VOLUME', 'VOLUME_GROWTH',
                                'TotalGPVariance',
                                'VarDuetoMargin',
                                'VarDueToSalesVolume',
                                'LaborCost',
                                'PlanLaborCost',
                                'PercentPlanLaborCost',
                                'LYLaborCost',
                                'LaborCostGrowth',
                                'LaborHours',
                                'PlanLaborHours',
                                'PercentPlanLaborHours',
                                'LYLaborHours',
                                'LaborHoursGrowth',
                                'SalesToLaborHours',
                                'PlanSalesToLaborHours',
                                'PercentPlanSalesToLaborHours',
                                'LYSalesToLaborHours',
                                'SalesToLaborHoursGrowth',
                                'MarginAfterLabor',
                                'PlanMarginAfterLabor',
                                'PercentPlanMarginAfterLabor',
                                'LYMarginAfterLabor',
                                'MarginAfterLaborGrowth'
                            ]]
                    #print(weekly)
                    self.weekly_totals = weekly
                case "Monthly":
                    monthly = pd.read_sql("""
                    select
                    1.0 - CAST(DATEDIFF(day, GETDATE(), '2022-09-30')/30.0 as float) as 'Annualize',
                    revenueReport.F254 as 'Day',
                    reportCodeTable.F1024 as 'Dept',
                    sum(revenueReport.F65) as 'Revenue',
                    ROUND(AVG(sum(revenueReport.F65)) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03 ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) as 'Ninety_Trailing_Revenue',
                    LAG(sum(revenueReport.F65), 12) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03) as 'LY REVENUE',
                    ROUND((sum(revenueReport.F65)/LAG(sum(revenueReport.F65), 12) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03)-1), 4) as 'REVENUE_GROWTH',
                    costReport.F65 as 'Cost',
                    ROUND(AVG(costReport.F65) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03 ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) as 'Ninety_Trailing_Cost',
                    LAG(costReport.F65, 12) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03) as 'LY COST',
                    ROUND((costReport.F65/LAG(costReport.F65, 12) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03)-1), 4) as 'COST_GROWTH',
                    ROUND(((sum(revenueReport.F65) - costReport.F65) / sum(revenueReport.F65)), 4) as 'Margin',
                    ROUND(AVG(((sum(revenueReport.F65)-costReport.F65)/sum(revenueReport.F65))) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03 ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 4) as 'Ninety_Trailing_Margin',
                    LAG(ROUND(((sum(revenueReport.F65) - costReport.F65) / sum(revenueReport.F65)), 4), 12) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03) as 'LY MARGIN',
                    ROUND(((sum(revenueReport.F65) - costReport.F65) / sum(revenueReport.F65)), 4) - LAG(ROUND(((sum(revenueReport.F65) - costReport.F65) / sum(revenueReport.F65)), 4), 12) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03) as 'YoY Diff',
                    sum(revenueReport.F65) - costReport.F65 as 'Profit',
                    ROUND((AVG(sum(revenueReport.F65)-costReport.F65) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03 ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING)), 4) as 'Ninety_Trailing_Profit',
                    LAG(sum(revenueReport.F65) - costReport.F65, 12) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03) as 'LY PROFIT',
                    ROUND(((sum(revenueReport.F65) - costReport.F65)/LAG(sum(revenueReport.F65) - costReport.F65, 12) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03) - 1), 4) as 'PROFIT_GROWTH',
                    sum(revenueReport.F64) as 'Volume',
                    ROUND(AVG(sum(revenueReport.F64)) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03 ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) as 'Ninety_Trailing_Volume',
                    LAG(sum(revenueReport.F64), 12) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03) as 'LY VOLUME',
                    ROUND((sum(revenueReport.F64)/LAG(sum(revenueReport.F64), 12) OVER (partition by revenueReport.F03 order by revenueReport.F254, revenueReport.F03)-1), 4) as 'VOLUME_GROWTH'
                    from
                    (select * from STORESQL.dbo.RPT_DPT 
                    where F1031 = 'M' and F1034 in (3, 3303) and F03 = %s) revenueReport
                    inner join (select * from STORESQL.dbo.RPT_DPT where F1031 = 'M' and F1034 = 8101 and F03 = %s) costReport on revenueReport.F254 = costReport.F254 and revenueReport.F03 = costReport.F03
                    inner join (select F18, F1024 from STORESQL.dbo.RPC_TAB) reportCodeTable on revenueReport.F03 = reportCodeTable.F18
                    where revenueReport.F03 not in (14, 21, 23, 97, 98, 99) 
                    group by revenueReport.F254, revenueReport.F03, costReport.F254, costReport.F03, costReport.F65, reportCodeTable.F1024
                    order by revenueReport.F254 desc, reportCodeTable.F1024
                    """ % (self.report_code, self.report_code),cnxn)
                   
                    monthly['Revenue'] = monthly['Revenue'].astype(float)
                    monthly['DOM'] = monthly['Day'].apply(lambda x: x.date())
                    monthly['Q'] = monthly['Day'].apply(lambda x: (pd.to_datetime(x) + pd.offsets.QuarterEnd(n=3)).to_period("Q"))
                    monthly['M'] = monthly['Day'].apply(lambda x: (pd.to_datetime(x) + pd.offsets.MonthEnd(n=6)).to_period("M"))
                    monthly['Plan Monthly Sales'] = monthly['M'].apply(lambda x: float(self.monthly_sales[self.months.index(x)]) if x >= starting_month else np.nan)
                    monthly['RevPercentPlan'] = monthly.apply(lambda x: (x['Revenue']/(x['Plan Monthly Sales'] * x['Annualize'])) - 1 if pd.to_datetime(x['Day']) > actual_date else (x['Revenue']/(x['Plan Monthly Sales']) - 1), axis=1)
                    monthly['Plan Monthly COGS'] = monthly['M'].apply(lambda x: float(self.monthly_COGS[self.months.index(x)]) if x >= starting_month else np.nan)
                    monthly['COGSPercentPlan'] = monthly.apply(lambda x: (x['Cost']/(x['Plan Monthly COGS'] * x['Annualize'])) - 1 if pd.to_datetime(x['Day']) > actual_date else (x['Cost']/(x['Plan Monthly COGS']) - 1), axis=1)
                    monthly['Plan Profit'] = monthly['Plan Monthly Sales'] - monthly['Plan Monthly COGS']
                    monthly['ProfitPercentPlan'] = monthly.apply(lambda x: (x['Profit']/(x['Plan Profit'] * x['Annualize'])) - 1 if pd.to_datetime(x['Day']) > actual_date else (x['Profit']/(x['Plan Profit']) - 1), axis=1)
                    monthly['Plan Margin'] = monthly['Q'].apply(lambda x: float(self.quarterly_margin[self.quarters.index(x)]) if x >= starting_quarter else np.nan)
                    monthly['MARGINPercentPlan'] = (monthly['Margin'] - monthly['Plan Margin'])
                    monthly['REVENUE_GROWTH'] = monthly.apply(lambda x: (x['Revenue']/(x['LY REVENUE'] * x['Annualize'])) - 1 if pd.to_datetime(x['Day']) > actual_date else x['REVENUE_GROWTH'], axis=1)
                    monthly['COST_GROWTH'] = monthly.apply(lambda x: (x['Cost']/(x['LY COST'] * x['Annualize'])) - 1 if pd.to_datetime(x['Day']) > actual_date else x['COST_GROWTH'], axis=1)
                    monthly['PROFIT_GROWTH'] = monthly.apply(lambda x: (x['Profit']/(x['LY PROFIT'] * x['Annualize'])) - 1 if pd.to_datetime(x['Day']) > actual_date else x['PROFIT_GROWTH'], axis=1)
                    monthly['VOLUME_GROWTH'] = monthly.apply(lambda x: (x['Volume']/(x['LY VOLUME'] * x['Annualize'])) - 1 if pd.to_datetime(x['Day']) > actual_date else x['VOLUME_GROWTH'], axis=1)
                    monthly.fillna('', inplace=True)
                    monthly = monthly.astype({"Day" : str})
                    monthly = monthly[[
                                'Annualize','Day', 'Dept', 'Revenue', 'Plan Monthly Sales', 'RevPercentPlan', 'Ninety_Trailing_Revenue', 'LY REVENUE', 'REVENUE_GROWTH',
                                'Cost', 'Plan Monthly COGS', 'COGSPercentPlan', 'Ninety_Trailing_Cost', 'LY COST', 'COST_GROWTH',
                                'Margin', 'Plan Margin', 'MARGINPercentPlan', 'Ninety_Trailing_Margin', 'LY MARGIN', 'YoY Diff',
                                'Profit', 'Plan Profit', 'ProfitPercentPlan', 'Ninety_Trailing_Profit', 'LY PROFIT', 'PROFIT_GROWTH',
                                'Volume', 'Ninety_Trailing_Volume', 'LY VOLUME', 'VOLUME_GROWTH'
                            ]]
                    self.monthly_totals = monthly
                case "Quarterly":
                    quarterly = pd.read_sql("""
                    select
                    1.0 - CAST(DATEDIFF(day, GETDATE(), '2022-09-30')/92.0 as float) as 'Annualize',
                    qtr.[Begin], qtr.[End], DATEDIFF(dd, qtr.[Begin], qtr.[End]) as 'QtrLength',
                    qtr.[Quarter], qtr.Dept,
                    sum(qtr.Revenue) as 'Revenue',
                    LAG(sum(qtr.Revenue), 4) OVER (partition by qtr.Dept order by qtr.[Quarter], qtr.Dept) as 'LY REVENUE',
                    ROUND((sum(qtr.Revenue)/LAG(sum(qtr.Revenue), 4) OVER (partition by qtr.Dept order by qtr.[Quarter], qtr.Dept))-1, 4) as 'REVENUE_GROWTH',
                    sum(qtr.Cost) as 'Cost',
                    LAG(sum(qtr.Cost), 4) OVER (partition by qtr.Dept order by qtr.[Quarter], qtr.Dept) as 'LY COST',
                    ROUND((sum(qtr.Cost)/LAG(sum(qtr.Cost), 4) OVER (partition by qtr.Dept order by qtr.[Quarter], qtr.Dept))-1, 4) as 'COST_GROWTH',
                    sum(qtr.Revenue) - sum(qtr.Cost) as 'Profit',
                    LAG(sum(qtr.Revenue)-sum(qtr.Cost), 4) OVER (partition by qtr.Dept order by qtr.[Quarter], qtr.Dept) as 'LY PROFIT',
                    ROUND((sum(qtr.Revenue) - sum(qtr.Cost)) / LAG(sum(qtr.Revenue)-sum(qtr.Cost), 4) OVER (partition by qtr.Dept order by qtr.[Quarter], qtr.Dept) -1, 4) as 'PROFIT_GROWTH',
                    (sum(qtr.Revenue) - sum(qtr.Cost))/ sum(qtr.Revenue) as 'Margin',
                    LAG((sum(qtr.Revenue)-sum(qtr.Cost))/sum(qtr.Revenue), 4) OVER (partition by qtr.Dept order by qtr.[Quarter], qtr.Dept) as 'LY MARGIN',
                    (sum(qtr.Revenue) - sum(qtr.Cost))/ sum(qtr.Revenue) - LAG((sum(qtr.Revenue)-sum(qtr.Cost))/sum(qtr.Revenue), 4) OVER (partition by qtr.Dept order by qtr.[Quarter], qtr.Dept) as 'YoY Diff',
                    sum(qtr.Volume) as 'Volume',
                    LAG(sum(qtr.Volume), 4) OVER (partition by qtr.Dept order by qtr.[Quarter], qtr.Dept) as 'LY VOLUME',
                    ROUND((sum(qtr.Volume)/LAG(sum(qtr.Volume), 4) OVER (partition by qtr.Dept order by qtr.[Quarter], qtr.Dept))-1, 4) as 'VOLUME_GROWTH'
                    from 
                    (select 
                    DATEADD(qq, DATEDIFF(qq, 0, revenueReport.F254), 0) as 'Begin',
                    DATEADD(d, -1, DATEADD(q, DATEDIFF(q, 0, revenueReport.F254) + 1, 0)) as 'End',
                    CASE
                            WHEN MONTH(revenueReport.F254) BETWEEN 7 AND 9 THEN convert(char(4), YEAR(revenueReport.F254) + 1) + 'Q1'
                            WHEN MONTH(revenueReport.F254) BETWEEN 10 AND 12 THEN convert(char(4), YEAR(revenueReport.F254) + 1) + 'Q2'
                            WHEN MONTH(revenueReport.F254) BETWEEN 1 AND 3 THEN convert(char(4), YEAR(revenueReport.F254)) + 'Q3'
                            WHEN MONTH(revenueReport.F254) BETWEEN 4 AND 6 THEN convert(char(4), YEAR(revenueReport.F254)) + 'Q4'
                    END AS 'Quarter',
                    reportCodeTable.F1024 as 'Dept',
                    sum(revenueReport.F65) as 'Revenue',
                    costReport.F65 as 'Cost',
                    sum(revenueReport.F64) as 'Volume'
                    from
                    (select * from STORESQL.dbo.RPT_DPT 
                    where F1031 = 'M' and F1034 in (3, 3303) and F03 = %s) revenueReport
                    inner join (select * from STORESQL.dbo.RPT_DPT where F1031 = 'M' and F1034 = 8101 and F03 = %s) costReport on revenueReport.F254 = costReport.F254 and revenueReport.F03 = costReport.F03
                    inner join (select F18, F1024 from STORESQL.dbo.RPC_TAB) reportCodeTable on revenueReport.F03 = reportCodeTable.F18
                    where revenueReport.F03 not in (14, 21, 23, 97, 98, 99)
                    group by
                    DATEADD(qq, DATEDIFF(qq, 0, revenueReport.F254), 0),
                    DATEADD(d, -1, DATEADD(q, DATEDIFF(q, 0, revenueReport.F254) + 1, 0)),
                    CASE
                            WHEN MONTH(revenueReport.F254) BETWEEN 7 AND 9 THEN convert(char(4), YEAR(revenueReport.F254) + 1) + 'Q1'
                            WHEN MONTH(revenueReport.F254) BETWEEN 10 AND 12 THEN convert(char(4), YEAR(revenueReport.F254) + 1) + 'Q2'
                            WHEN MONTH(revenueReport.F254) BETWEEN 1 AND 3 THEN convert(char(4), YEAR(revenueReport.F254)) + 'Q3'
                            WHEN MONTH(revenueReport.F254) BETWEEN 4 AND 6 THEN convert(char(4), YEAR(revenueReport.F254)) + 'Q4'
                    END,
                    revenueReport.F03, costReport.F03, costReport.F65, reportCodeTable.F1024)
                    qtr
                    group by qtr.[Begin], qtr.[End], qtr.[Quarter], qtr.[Dept]
                    order by qtr.[Dept], qtr.[Quarter] desc
                    """ % (self.report_code, self.report_code),cnxn)
                    
                    quarterly['Revenue'] = quarterly['Revenue'].astype(float)
                    quarterly['Q'] = quarterly['Begin'].apply(lambda x: (pd.to_datetime(x) + pd.offsets.QuarterEnd(n=3)).to_period("Q"))
                    quarterly['Plan Quarterly Sales'] = quarterly['Q'].apply(lambda x: float(self.quarterly_sales[self.quarters.index(x)]) if x >= starting_quarter else np.nan)
                    quarterly['RevPercentPlan'] = quarterly.apply(lambda x: (x['Revenue']/(x['Plan Quarterly Sales'] * ( (x['QtrLength'] - (pd.to_datetime(x['End']).day_of_year - pd.to_datetime(actual_date).day_of_year)) / x['QtrLength']))) - 1 if pd.to_datetime(x['End']) > actual_date else (x['Revenue']/(x['Plan Quarterly Sales']) - 1), axis=1)
                    quarterly['Plan Quarterly COGS'] = quarterly['Q'].apply(lambda x: float(self.quarterly_COGS[self.quarters.index(x)]) if x >= starting_quarter else np.nan)
                    quarterly['COGSPercentPlan'] = quarterly.apply(lambda x: (x['Cost']/(x['Plan Quarterly COGS'] * ( (x['QtrLength'] - (pd.to_datetime(x['End']).day_of_year - pd.to_datetime(actual_date).day_of_year)) / x['QtrLength']))) - 1 if pd.to_datetime(x['End']) > actual_date else (x['Cost']/(x['Plan Quarterly COGS']) - 1), axis=1)
                    quarterly['Plan Profit'] = quarterly['Plan Quarterly Sales'] - quarterly['Plan Quarterly COGS']
                    quarterly['ProfitPercentPlan'] = quarterly.apply(lambda x: (x['Profit']/(x['Plan Profit'] * ( (x['QtrLength'] - (pd.to_datetime(x['End']).day_of_year - pd.to_datetime(actual_date).day_of_year)) / x['QtrLength']))) - 1 if pd.to_datetime(x['End']) > actual_date else (x['Profit']/(x['Plan Profit']) - 1), axis=1)
                    quarterly['Plan Margin'] = quarterly['Q'].apply(lambda x: float(self.quarterly_margin[self.quarters.index(x)]) if x >= starting_quarter else np.nan)
                    quarterly['MARGINPercentPlan'] = (quarterly['Margin'] - quarterly['Plan Margin'])
                    quarterly['REVENUE_GROWTH'] = quarterly.apply(lambda x: (x['Revenue']/(x['LY REVENUE'] * x['Annualize'])) - 1 if pd.to_datetime(x['End']) > actual_date else x['REVENUE_GROWTH'], axis=1)
                    quarterly['COST_GROWTH'] = quarterly.apply(lambda x: (x['Cost']/(x['LY COST'] * x['Annualize'])) - 1 if pd.to_datetime(x['End']) > actual_date else x['COST_GROWTH'], axis=1)
                    quarterly['PROFIT_GROWTH'] = quarterly.apply(lambda x: (x['Profit']/(x['LY PROFIT'] * x['Annualize'])) - 1 if pd.to_datetime(x['End']) > actual_date else x['PROFIT_GROWTH'], axis=1)
                    quarterly['VOLUME_GROWTH'] = quarterly.apply(lambda x: (x['Volume']/(x['LY VOLUME'] * x['Annualize'])) - 1 if pd.to_datetime(x['End']) > actual_date else x['VOLUME_GROWTH'], axis=1)
                    quarterly.fillna('', inplace=True)

                    quarterly = quarterly[[
                                'End', 'Annualize','Quarter', 'Dept', 'Revenue', 'Plan Quarterly Sales', 'RevPercentPlan', 'LY REVENUE', 'REVENUE_GROWTH',
                                'Cost', 'Plan Quarterly COGS', 'COGSPercentPlan', 'LY COST', 'COST_GROWTH',
                                'Margin', 'Plan Margin', 'MARGINPercentPlan', 'LY MARGIN', 'YoY Diff',
                                'Profit', 'Plan Profit', 'ProfitPercentPlan', 'LY PROFIT', 'PROFIT_GROWTH',
                                'Volume', 'LY VOLUME', 'VOLUME_GROWTH'
                            ]]

                    if self.Name in labor.report_look_ups:
                        
                        labor = labor.report(self.Name).groupby('Dept').agg({"Worked Wages" : sum, "Worked Hours" : sum})
                        labor['Quarter'] = '2023Q1'
                        print("LABOR", labor)
                        quarterly = quarterly.merge(labor, how='left', left_on='Quarter', right_on='Quarter')
                        quarterly['TotalGPVariance'] = ""
                        quarterly['VarDuetoMargin'] = ""
                        quarterly['VarDueToSalesVolume'] = ""
                        quarterly['LaborCost'] = quarterly['Worked Wages']
                        quarterly['PlanLaborCost'] = ""
                        quarterly['PercentPlanLaborCost'] = ""
                        quarterly['LYLaborCost'] = ""
                        quarterly['LaborCostGrowth'] = ""
                        quarterly['LaborHours'] = quarterly['Worked Hours']
                        quarterly['PlanLaborHours'] = ""
                        quarterly['PercentPlanLaborHours'] = ""
                        quarterly['LYLaborHours'] = ""
                        quarterly['LaborHoursGrowth'] = ""
                        quarterly['SalesToLaborHours'] = quarterly['Revenue'] / quarterly['LaborHours']
                        quarterly['PlanSalesToLaborHours'] = ""
                        quarterly['PercentPlanSalesToLaborHours'] = ""
                        quarterly['LYSalesToLaborHours'] = ""
                        quarterly['SalesToLaborHoursGrowth'] = ""
                        quarterly['MarginAfterLabor'] = ((quarterly['Revenue']-quarterly['LaborCost']) - quarterly['Cost'])/quarterly['Revenue']
                        quarterly['PlanMarginAfterLabor'] = ""
                        quarterly['PercentPlanMarginAfterLabor'] = ""
                        quarterly['LYMarginAfterLabor'] = ""
                        quarterly['MarginAfterLaborGrowth'] = ""
                        quarterly.fillna("", inplace=True)
                    else:
                        
                        quarterly['Worked Wages'] = ""
                        quarterly['Worked Hours'] = ""

                        quarterly['TotalGPVariance'] = ""
                        quarterly['VarDuetoMargin'] = ""
                        quarterly['VarDueToSalesVolume'] = ""
                        quarterly['LaborCost'] = ""
                        quarterly['PlanLaborCost'] = ""
                        quarterly['PercentPlanLaborCost'] = ""
                        quarterly['LYLaborCost'] = ""
                        quarterly['LaborCostGrowth'] = ""
                        quarterly['LaborHours'] = ""
                        quarterly['PlanLaborHours'] = ""
                        quarterly['PercentPlanLaborHours'] = ""
                        quarterly['LYLaborHours'] = ""
                        quarterly['LaborHoursGrowth'] = ""
                        quarterly['SalesToLaborHours'] = ""
                        quarterly['PlanSalesToLaborHours'] = ""
                        quarterly['PercentPlanSalesToLaborHours'] = ""
                        quarterly['LYSalesToLaborHours'] = ""
                        quarterly['SalesToLaborHoursGrowth'] = ""
                        quarterly['MarginAfterLabor'] = ""
                        quarterly['PlanMarginAfterLabor'] = ""
                        quarterly['PercentPlanMarginAfterLabor'] = ""
                        quarterly['LYMarginAfterLabor'] = ""
                        quarterly['MarginAfterLaborGrowth'] = ""
                    #    quarterly['Week','Worked Wages','Worked Hours','TotalGPVariance','VarDuetoMargin','VarDueToSalesVolume','LaborCost','PlanLaborCost','PercentPlanLaborCost',
                    #    'LYLaborCost','LaborCostGrowth','LaborHours','PlanLaborHours','PercentPlanLaborHours','LYLaborHours','LaborHoursGrowth','SalesToLaborHours',
                    #    'PlanSalesToLaborHours','PercentPlanSalesToLaborHours','LYSalesToLaborHours','SalesToLaborHoursGrowth','MarginAfterLabor','PlanMarginAfterLabor',
                    #    'PercentPlanMarginAfterLabor','LYMarginAfterLabor','MarginAfterLaborGrowth'] = ""

                    quarterly = quarterly[[
                                'End', 'Annualize','Quarter', 'Dept', 'Revenue', 'Plan Quarterly Sales', 'RevPercentPlan', 'LY REVENUE', 'REVENUE_GROWTH',
                                'Cost', 'Plan Quarterly COGS', 'COGSPercentPlan', 'LY COST', 'COST_GROWTH',
                                'Margin', 'Plan Margin', 'MARGINPercentPlan', 'LY MARGIN', 'YoY Diff',
                                'Profit', 'Plan Profit', 'ProfitPercentPlan', 'LY PROFIT', 'PROFIT_GROWTH',
                                'Volume', 'LY VOLUME', 'VOLUME_GROWTH',
                                'TotalGPVariance',
                                'VarDuetoMargin',
                                'VarDueToSalesVolume',
                                'LaborCost',
                                'PlanLaborCost',
                                'PercentPlanLaborCost',
                                'LYLaborCost',
                                'LaborCostGrowth',
                                'LaborHours',
                                'PlanLaborHours',
                                'PercentPlanLaborHours',
                                'LYLaborHours',
                                'LaborHoursGrowth',
                                'SalesToLaborHours',
                                'PlanSalesToLaborHours',
                                'PercentPlanSalesToLaborHours',
                                'LYSalesToLaborHours',
                                'SalesToLaborHoursGrowth',
                                'MarginAfterLabor',
                                'PlanMarginAfterLabor',
                                'PercentPlanMarginAfterLabor',
                                'LYMarginAfterLabor',
                                'MarginAfterLaborGrowth'
                            ]]
                    #print(quarterly)
                    
                    self.quarterly_totals = quarterly
                case "Yearly":
                    yearly = pd.read_sql(
                    """
                    select
                    1.0 - CAST(DATEDIFF(day, GETDATE(), '2023-06-30')/365.0 as float) as 'Annualize',
                    qtr.[Quarter] as 'Year', qtr.Dept,
                    sum(qtr.Revenue) as 'Revenue',
                    LAG(sum(qtr.Revenue), 1) OVER (partition by qtr.Dept order by qtr.[Quarter], qtr.Dept) as 'LY REVENUE',
                    ROUND((sum(qtr.Revenue)/LAG(sum(qtr.Revenue), 1) OVER (partition by qtr.Dept order by qtr.[Quarter], qtr.Dept))-1, 4) as 'REVENUE_GROWTH',
                    sum(qtr.Cost) as 'Cost',
                    LAG(sum(qtr.Cost),1) OVER (partition by qtr.Dept order by qtr.[Quarter], qtr.Dept) as 'LY COST',
                    ROUND((sum(qtr.Cost)/LAG(sum(qtr.Cost), 1) OVER (partition by qtr.Dept order by qtr.[Quarter], qtr.Dept))-1, 4) as 'COST_GROWTH',
                    sum(qtr.Revenue) - sum(qtr.Cost) as 'Profit',
                    LAG(sum(qtr.Revenue)-sum(qtr.Cost), 1) OVER (partition by qtr.Dept order by qtr.[Quarter], qtr.Dept) as 'LY PROFIT',
                    ROUND((sum(qtr.Revenue) - sum(qtr.Cost)) / LAG(sum(qtr.Revenue)-sum(qtr.Cost), 1) OVER (partition by qtr.Dept order by qtr.[Quarter], qtr.Dept) -1, 4) as 'PROFIT_GROWTH',
                    (sum(qtr.Revenue) - sum(qtr.Cost))/ sum(qtr.Revenue) as 'Margin',
                    LAG((sum(qtr.Revenue)-sum(qtr.Cost))/sum(qtr.Revenue), 1) OVER (partition by qtr.Dept order by qtr.[Quarter], qtr.Dept) as 'LY MARGIN',
                    (sum(qtr.Revenue) - sum(qtr.Cost))/ sum(qtr.Revenue) - LAG((sum(qtr.Revenue)-sum(qtr.Cost))/sum(qtr.Revenue), 1) OVER (partition by qtr.Dept order by qtr.[Quarter], qtr.Dept) as 'YoY Diff',
                    sum(qtr.Volume) as 'Volume',
                    LAG(sum(qtr.Volume), 1) OVER (partition by qtr.Dept order by qtr.[Quarter], qtr.Dept) as 'LY VOLUME',
                    ROUND((sum(qtr.Volume)/LAG(sum(qtr.Volume), 4) OVER (partition by qtr.Dept order by qtr.[Quarter], qtr.Dept))-1, 4) as 'VOLUME_GROWTH'
                    from 
                    (select CASE
                            WHEN MONTH(revenueReport.F254) BETWEEN 7 AND 9 THEN convert(char(4), YEAR(revenueReport.F254) + 1)
                            WHEN MONTH(revenueReport.F254) BETWEEN 10 AND 12 THEN convert(char(4), YEAR(revenueReport.F254) + 1)
                            WHEN MONTH(revenueReport.F254) BETWEEN 1 AND 3 THEN convert(char(4), YEAR(revenueReport.F254))
                            WHEN MONTH(revenueReport.F254) BETWEEN 4 AND 6 THEN convert(char(4), YEAR(revenueReport.F254))
                    END AS 'Quarter',
                    reportCodeTable.F1024 as 'Dept',
                    sum(revenueReport.F65) as 'Revenue',
                    costReport.F65 as 'Cost',
                    sum(revenueReport.F64) as 'Volume'
                    from
                    (select * from STORESQL.dbo.RPT_DPT 
                    where F1031 = 'M' and F1034 in (3, 3303) and F03 = %s) revenueReport
                    inner join (select * from STORESQL.dbo.RPT_DPT where F1031 = 'M' and F1034 = 8101 and F03 = %s) costReport on revenueReport.F254 = costReport.F254 and revenueReport.F03 = costReport.F03
                    inner join (select F18, F1024 from STORESQL.dbo.RPC_TAB) reportCodeTable on revenueReport.F03 = reportCodeTable.F18
                    where revenueReport.F03 not in (14, 21, 23, 97, 98, 99)
                    group by
                    CASE
                            WHEN MONTH(revenueReport.F254) BETWEEN 7 AND 9 THEN convert(char(4), YEAR(revenueReport.F254) + 1)
                            WHEN MONTH(revenueReport.F254) BETWEEN 10 AND 12 THEN convert(char(4), YEAR(revenueReport.F254) + 1)
                            WHEN MONTH(revenueReport.F254) BETWEEN 1 AND 3 THEN convert(char(4), YEAR(revenueReport.F254))
                            WHEN MONTH(revenueReport.F254) BETWEEN 4 AND 6 THEN convert(char(4), YEAR(revenueReport.F254))
                    END,
                    revenueReport.F03, costReport.F03, costReport.F65, reportCodeTable.F1024)
                    qtr
                    group by qtr.[Quarter], qtr.[Dept]
                    order by  qtr.[Quarter] desc, qtr.[Dept]
                    """ % (self.report_code, self.report_code),cnxn)

                    yearly['Revenue'] = yearly['Revenue'].astype(float)
                    yearly['Plan Yearly Sales'] = yearly['Year'].apply(lambda x: float(self.year_sales) if x == "2023" else np.nan)
                    yearly['RevPercentPlan'] = yearly.apply(lambda x: (x['Revenue']/(x['Plan Yearly Sales'] * x['Annualize'])) - 1 if x['Year'] == "2023" else np.nan, axis=1)
                    yearly['Plan Yearly COGS'] = yearly['Year'].apply(lambda x: float(self.year_COGS) if x == "2023" else np.nan)
                    yearly['COGSPercentPlan'] = yearly.apply(lambda x: (x['Revenue']/(x['Plan Yearly COGS'] * x['Annualize'])) - 1 if x['Year'] == "2023" else np.nan, axis=1)
                    yearly['Plan Profit'] = yearly['Plan Yearly Sales'] - yearly['Plan Yearly COGS']
                    yearly['ProfitPercentPlan'] = yearly.apply(lambda x: (x['Profit']/(x['Plan Profit'] * x['Annualize'])) - 1 if x['Year'] == "2023" else np.nan, axis=1)
                    yearly['Plan Margin'] = yearly['Year'].apply(lambda x: float(self.year_margin) if x == "2023" else np.nan)
                    yearly['MARGINPercentPlan'] = (yearly['Margin'] - yearly['Plan Margin'])
                    yearly['REVENUE_GROWTH'] = yearly.apply(lambda x: (x['Revenue']/(x['LY REVENUE'] * x['Annualize'])) - 1 if x['Year'] == "2023" else x['REVENUE_GROWTH'], axis=1)
                    yearly['COST_GROWTH'] = yearly.apply(lambda x: (x['Cost']/(x['LY COST'] * x['Annualize'])) - 1 if x['Year'] == "2023" else x['COST_GROWTH'], axis=1)
                    yearly['PROFIT_GROWTH'] = yearly.apply(lambda x: (x['Profit']/(x['LY PROFIT'] * x['Annualize'])) - 1 if x['Year'] == "2023" else x['PROFIT_GROWTH'], axis=1)
                    yearly['VOLUME_GROWTH'] = yearly.apply(lambda x: (x['Volume']/(x['LY VOLUME'] * x['Annualize'])) - 1 if x['Year'] == "2023" else x['VOLUME_GROWTH'], axis=1)
                    yearly.fillna('', inplace=True)

                    yearly = yearly[[
                                'Annualize','Year', 'Dept', 'Revenue', 'Plan Yearly Sales', 'RevPercentPlan', 'LY REVENUE', 'REVENUE_GROWTH',
                                'Cost', 'Plan Yearly COGS', 'COGSPercentPlan', 'LY COST', 'COST_GROWTH',
                                'Margin', 'Plan Margin', 'MARGINPercentPlan', 'LY MARGIN', 'YoY Diff',
                                'Profit', 'Plan Profit', 'ProfitPercentPlan', 'LY PROFIT', 'PROFIT_GROWTH',
                                'Volume', 'LY VOLUME', 'VOLUME_GROWTH'
                            ]]

                    if self.Name in labor.report_look_ups:
                        labor = labor.report(self.Name).groupby('Dept').agg({"Worked Wages" : sum, "Worked Hours" : sum})
                        labor['Year'] = '2023'
                        print("LABOR", labor)
                        yearly = yearly.merge(labor, how='left', left_on='Year', right_on='Year')
                        yearly['TotalGPVariance'] = ""
                        yearly['VarDuetoMargin'] = ""
                        yearly['VarDueToSalesVolume'] = ""
                        yearly['LaborCost'] = yearly['Worked Wages']
                        yearly['PlanLaborCost'] = ""
                        yearly['PercentPlanLaborCost'] = ""
                        yearly['LYLaborCost'] = ""
                        yearly['LaborCostGrowth'] = ""
                        yearly['LaborHours'] = yearly['Worked Hours']
                        yearly['PlanLaborHours'] = ""
                        yearly['PercentPlanLaborHours'] = ""
                        yearly['LYLaborHours'] = ""
                        yearly['LaborHoursGrowth'] = ""
                        yearly['SalesToLaborHours'] = yearly['LaborCost'] / yearly['LaborHours']
                        yearly['PlanSalesToLaborHours'] = ""
                        yearly['PercentPlanSalesToLaborHours'] = ""
                        yearly['LYSalesToLaborHours'] = ""
                        yearly['SalesToLaborHoursGrowth'] = ""
                        yearly['MarginAfterLabor'] = ((yearly['Revenue']-yearly['LaborCost']) - yearly['Cost'])/yearly['Revenue']
                        yearly['PlanMarginAfterLabor'] = ""
                        yearly['PercentPlanMarginAfterLabor'] = ""
                        yearly['LYMarginAfterLabor'] = ""
                        yearly['MarginAfterLaborGrowth'] = ""
                        yearly.fillna("", inplace=True)
                    else:
                        
                        yearly['Worked Wages'] = ""
                        yearly['Worked Hours'] = ""

                        yearly['TotalGPVariance'] = ""
                        yearly['VarDuetoMargin'] = ""
                        yearly['VarDueToSalesVolume'] = ""
                        yearly['LaborCost'] = ""
                        yearly['PlanLaborCost'] = ""
                        yearly['PercentPlanLaborCost'] = ""
                        yearly['LYLaborCost'] = ""
                        yearly['LaborCostGrowth'] = ""
                        yearly['LaborHours'] = ""
                        yearly['PlanLaborHours'] = ""
                        yearly['PercentPlanLaborHours'] = ""
                        yearly['LYLaborHours'] = ""
                        yearly['LaborHoursGrowth'] = ""
                        yearly['SalesToLaborHours'] = ""
                        yearly['PlanSalesToLaborHours'] = ""
                        yearly['PercentPlanSalesToLaborHours'] = ""
                        yearly['LYSalesToLaborHours'] = ""
                        yearly['SalesToLaborHoursGrowth'] = ""
                        yearly['MarginAfterLabor'] = ""
                        yearly['PlanMarginAfterLabor'] = ""
                        yearly['PercentPlanMarginAfterLabor'] = ""
                        yearly['LYMarginAfterLabor'] = ""
                        yearly['MarginAfterLaborGrowth'] = ""
                    #    yearly['Week','Worked Wages','Worked Hours','TotalGPVariance','VarDuetoMargin','VarDueToSalesVolume','LaborCost','PlanLaborCost','PercentPlanLaborCost',
                    #    'LYLaborCost','LaborCostGrowth','LaborHours','PlanLaborHours','PercentPlanLaborHours','LYLaborHours','LaborHoursGrowth','SalesToLaborHours',
                    #    'PlanSalesToLaborHours','PercentPlanSalesToLaborHours','LYSalesToLaborHours','SalesToLaborHoursGrowth','MarginAfterLabor','PlanMarginAfterLabor',
                    #    'PercentPlanMarginAfterLabor','LYMarginAfterLabor','MarginAfterLaborGrowth'] = ""

                    yearly = yearly[[
                                'Annualize','Year', 'Dept', 'Revenue', 'Plan Yearly Sales', 'RevPercentPlan', 'LY REVENUE', 'REVENUE_GROWTH',
                                'Cost', 'Plan Yearly COGS', 'COGSPercentPlan', 'LY COST', 'COST_GROWTH',
                                'Margin', 'Plan Margin', 'MARGINPercentPlan', 'LY MARGIN', 'YoY Diff',
                                'Profit', 'Plan Profit', 'ProfitPercentPlan', 'LY PROFIT', 'PROFIT_GROWTH',
                                'Volume', 'LY VOLUME', 'VOLUME_GROWTH',
                                'TotalGPVariance',
                                'VarDuetoMargin',
                                'VarDueToSalesVolume',
                                'LaborCost',
                                'PlanLaborCost',
                                'PercentPlanLaborCost',
                                'LYLaborCost',
                                'LaborCostGrowth',
                                'LaborHours',
                                'PlanLaborHours',
                                'PercentPlanLaborHours',
                                'LYLaborHours',
                                'LaborHoursGrowth',
                                'SalesToLaborHours',
                                'PlanSalesToLaborHours',
                                'PercentPlanSalesToLaborHours',
                                'LYSalesToLaborHours',
                                'SalesToLaborHoursGrowth',
                                'MarginAfterLabor',
                                'PlanMarginAfterLabor',
                                'PercentPlanMarginAfterLabor',
                                'LYMarginAfterLabor',
                                'MarginAfterLaborGrowth'
                            ]]
                    #print(yearly)
                    self.yearly_totals = yearly
        else:
            match period:
                case "Daily":
                    starting_quarter = pd.Period('2023Q1')
                    daily = pd.read_sql("""
                                select revenueReport.F254 as 'Day',
                                'Store Wide' as 'Dept',
                                sum(revenueReport.F65) as 'Revenue',
                                LAG(sum(revenueReport.F65), 7) OVER (order by revenueReport.F254) as 'LW Revenue',
                                ROUND(AVG(sum(revenueReport.F65)) OVER (order by revenueReport.F254 ROWS BETWEEN 88 PRECEDING AND 1 PRECEDING), 2) as 'Ninety_Trailing_Revenue',
                                LAG(sum(revenueReport.F65), 358) OVER (order by revenueReport.F254) as 'LY REVENUE',
                                ROUND((sum(revenueReport.F65)/(LAG(sum(revenueReport.F65), 358) OVER (order by revenueReport.F254))-1), 4) as 'REVENUE_GROWTH',
                                costReport.F65 as 'Cost',
                                LAG(costReport.F65, 7) OVER (order by revenueReport.F254) as 'LW Cost',
                                ROUND(AVG(costReport.F65) OVER (order by revenueReport.F254 ROWS BETWEEN 88 PRECEDING AND 1 PRECEDING), 2) as 'Ninety_Trailing_Cost',
                                LAG(costReport.F65, 358) OVER (order by revenueReport.F254) as 'LY COST',
                                ROUND((costReport.F65/(LAG(costReport.F65, 358) OVER (order by revenueReport.F254))-1), 4) as 'COST_GROWTH',
                                ROUND(((sum(revenueReport.F65)-costReport.F65)/sum(revenueReport.F65)),4) as 'Margin',
                                LAG(ROUND(((sum(revenueReport.F65) - costReport.F65) / sum(revenueReport.F65)), 4), 7) OVER (order by revenueReport.F254) as 'LW Margin',
                                ROUND(AVG(((sum(revenueReport.F65)-costReport.F65)/sum(revenueReport.F65))) OVER (order by revenueReport.F254 ROWS BETWEEN 88 PRECEDING AND 1 PRECEDING), 4) as 'Ninety_Trailing_Margin',
                                LAG(ROUND(((sum(revenueReport.F65) - costReport.F65) / sum(revenueReport.F65)), 4), 358) OVER (order by revenueReport.F254) as 'LY MARGIN',
                                ROUND(((sum(revenueReport.F65) - costReport.F65) / sum(revenueReport.F65)), 4) - LAG(ROUND(((sum(revenueReport.F65) - costReport.F65) / sum(revenueReport.F65)), 4), 358) OVER (order by revenueReport.F254) as 'YoY Diff',
                                (sum(revenueReport.F65)-costReport.F65) as 'Profit',
                                LAG((sum(revenueReport.F65)-costReport.F65), 7) OVER (order by revenueReport.F254) as 'LW Profit',
                                ROUND(AVG((sum(revenueReport.F65)-costReport.F65)) OVER (order by revenueReport.F254 ROWS BETWEEN 88 PRECEDING AND 1 PRECEDING), 2) as 'Ninety_Trailing_Profit',
                                LAG((sum(revenueReport.F65)-costReport.F65), 358) OVER (order by revenueReport.F254) as 'LY PROFIT',
                                ROUND((sum(revenueReport.F65)-costReport.F65)/(LAG((sum(revenueReport.F65)-costReport.F65), 358) OVER (order by revenueReport.F254))-1, 4) as 'PROFIT_GROWTH',
                                sum(revenueReport.F64) as 'Volume',
                                LAG(sum(revenueReport.F64), 7) OVER (order by revenueReport.F254) as 'LW Volume',
                                ROUND(AVG(sum(revenueReport.F64)) OVER (order by revenueReport.F254 ROWS BETWEEN 88 PRECEDING AND 1 PRECEDING), 2) as 'Ninety_Trailing_Volume',
                                LAG(sum(revenueReport.F64), 358) OVER (order by revenueReport.F254) as 'LY VOLUME',
                                ROUND((sum(revenueReport.F64)/(LAG(sum(revenueReport.F64), 358) OVER (order by revenueReport.F254))-1), 4) as 'VOLUME_GROWTH'
                                from (select * from STORESQL.dbo.RPT_FIN
                                where F1031 = 'D' and F1034 = 2) revenueReport
                                inner join (select * from STORESQL.dbo.RPT_FIN where F1031 = 'D' and F1034 = 8101) costReport on revenueReport.F254 = costReport.F254
                                group by revenueReport.F254, costReport.F65
                                order by revenueReport.F254 desc
                                """, cnxn)
                    
                    daily['Revenue'] = daily['Revenue'].astype(float)
                    daily['Q'] = daily['Day'].apply(lambda x: (pd.to_datetime(x) + pd.offsets.QuarterEnd(n=3)).to_period("Q"))
                    daily['Plan Daily Sales'] = daily['Q'].apply(lambda x: float(self.daily_sales[self.quarters.index(x)]) if x >= starting_quarter else np.nan)
                    daily['RevPercentPlan'] = (daily['Revenue']/daily['Plan Daily Sales']) - 1
                    daily['Plan Daily COGS'] = daily['Q'].apply(lambda x: float(self.daily_COGS[self.quarters.index(x)]) if x >= starting_quarter else np.nan)
                    daily['COGSPercentPlan'] = (daily['Cost']/daily['Plan Daily COGS']) - 1
                    daily['Plan Profit'] = daily['Plan Daily Sales'] - daily['Plan Daily COGS']
                    daily['ProfitPercentPlan'] = (daily['Profit']/daily['Plan Profit']) - 1
                    daily['Plan Margin'] = daily['Q'].apply(lambda x: float(self.quarterly_margin[self.quarters.index(x)]) if x >= starting_quarter else np.nan)
                    daily['MARGINPercentPlan'] = (daily['Margin'] - daily['Plan Margin'])
                    daily.fillna('', inplace=True)
                    daily = daily.astype({"Day" : str})
                    daily = daily[[
                                'Day', 'Dept', 'Revenue', 'Plan Daily Sales', 'RevPercentPlan', 'LW Revenue', 'Ninety_Trailing_Revenue', 'LY REVENUE', 'REVENUE_GROWTH',
                                'Cost', 'Plan Daily COGS', 'COGSPercentPlan', 'LW Cost', 'Ninety_Trailing_Cost', 'LY COST', 'COST_GROWTH',
                                'Margin', 'Plan Margin', 'MARGINPercentPlan', 'LW Margin', 'Ninety_Trailing_Margin', 'LY MARGIN', 'YoY Diff',
                                'Profit', 'Plan Profit', 'ProfitPercentPlan', 'LW Profit', 'Ninety_Trailing_Profit', 'LY PROFIT', 'PROFIT_GROWTH',
                                'Volume', 'LW Volume', 'Ninety_Trailing_Volume', 'LY VOLUME', 'VOLUME_GROWTH'
                            ]]
                    self.daily_totals = daily
                case "Weekly":
                    starting_quarter = pd.Period('2023Q1')
                    weekly = pd.read_sql("""
                                select
                                1.0 - CAST(DATEDIFF(day, GETDATE(), '2022-09-18')/7.0 as float) as 'Annualize',
                                revenueReport.F254 as 'Day',
                                'Store Wide' as 'Dept',
                                sum(revenueReport.F65) as 'Revenue',
                                ROUND(AVG(sum(revenueReport.F65)) OVER (order by revenueReport.F254 ROWS BETWEEN 13 PRECEDING AND 1 PRECEDING), 2) as 'Ninety_Trailing_Revenue',
                                LAG(sum(revenueReport.F65), 52) OVER (order by revenueReport.F254) as 'LY REVENUE',
                                ROUND((sum(revenueReport.F65)/(LAG(sum(revenueReport.F65), 52) OVER (order by revenueReport.F254))-1), 4) as 'REVENUE_GROWTH',
                                costReport.F65 as 'Cost',
                                ROUND(AVG(costReport.F65) OVER (order by revenueReport.F254 ROWS BETWEEN 13 PRECEDING AND 1 PRECEDING), 2) as 'Ninety_Trailing_Cost',
                                LAG(costReport.F65, 52) OVER (order by revenueReport.F254) as 'LY COST',
                                ROUND((costReport.F65/(LAG(costReport.F65, 52) OVER (order by revenueReport.F254))-1), 4) as 'COST_GROWTH',
                                ROUND(((sum(revenueReport.F65)-costReport.F65)/sum(revenueReport.F65)),4) as 'Margin',
                                ROUND(AVG(((sum(revenueReport.F65)-costReport.F65)/sum(revenueReport.F65))) OVER (order by revenueReport.F254 ROWS BETWEEN 13 PRECEDING AND 1 PRECEDING), 4) as 'Ninety_Trailing_Margin',
                                LAG(ROUND(((sum(revenueReport.F65) - costReport.F65) / sum(revenueReport.F65)), 4), 52) OVER (order by revenueReport.F254) as 'LY MARGIN',
                                ROUND(((sum(revenueReport.F65) - costReport.F65) / sum(revenueReport.F65)), 4) - LAG(ROUND(((sum(revenueReport.F65) - costReport.F65) / sum(revenueReport.F65)), 4), 52) OVER (order by revenueReport.F254) as 'YoY Diff',
                                (sum(revenueReport.F65)-costReport.F65) as 'Profit',
                                ROUND(AVG((sum(revenueReport.F65)-costReport.F65)) OVER (order by revenueReport.F254 ROWS BETWEEN 13 PRECEDING AND 1 PRECEDING), 2) as 'Ninety_Trailing_Profit',
                                LAG((sum(revenueReport.F65)-costReport.F65), 52) OVER (order by revenueReport.F254) as 'LY PROFIT',
                                ROUND((sum(revenueReport.F65)-costReport.F65)/(LAG((sum(revenueReport.F65)-costReport.F65), 52) OVER (order by revenueReport.F254))-1, 4) as 'PROFIT_GROWTH',
                                sum(revenueReport.F64) as 'Volume',
                                ROUND(AVG(sum(revenueReport.F64)) OVER (order by revenueReport.F254 ROWS BETWEEN 13 PRECEDING AND 1 PRECEDING), 2) as 'Ninety_Trailing_Volume',
                                LAG(sum(revenueReport.F64), 52) OVER (order by revenueReport.F254) as 'LY VOLUME',
                                ROUND((sum(revenueReport.F64)/(LAG(sum(revenueReport.F64), 52) OVER (order by revenueReport.F254))-1), 4) as 'VOLUME_GROWTH'
                                from (select * from STORESQL.dbo.RPT_FIN
                                where F1031 = 'W' and F1034 = 2) revenueReport
                                inner join (select * from STORESQL.dbo.RPT_FIN where F1031 = 'W' and F1034 = 8101) costReport on revenueReport.F254 = costReport.F254
                                group by revenueReport.F254, costReport.F65
                                order by revenueReport.F254 desc
                                """,cnxn)
                    
                    weekly['Revenue'] = weekly['Revenue'].astype(float)
                    weekly['DOW'] = weekly['Day'].apply(lambda x: x.date().weekday())
                    weekly['Q'] = weekly['Day'].apply(lambda x: (pd.to_datetime(x) + pd.offsets.QuarterEnd(n=3)).to_period("Q"))
                    weekly['Plan Weekly Sales'] = weekly['Q'].apply(lambda x: float(self.weekly_sales[self.quarters.index(x)]) if x >= starting_quarter else np.nan)
                    weekly['RevPercentPlan'] = weekly.apply(lambda x: (x['Revenue']/(x['Plan Weekly Sales'] * x['Annualize'])) - 1 if pd.to_datetime(x['Day']) > actual_date else (x['Revenue']/(x['Plan Weekly Sales']) - 1), axis=1)
                    weekly['Plan Weekly COGS'] = weekly['Q'].apply(lambda x: float(self.weekly_COGS[self.quarters.index(x)]) if x >= starting_quarter else np.nan)
                    weekly['COGSPercentPlan'] = weekly.apply(lambda x: (x['Cost']/(x['Plan Weekly COGS'] * x['Annualize'])) - 1 if pd.to_datetime(x['Day']) > actual_date else (x['Cost']/(x['Plan Weekly COGS']) - 1), axis=1)
                    weekly['Plan Profit'] = weekly['Plan Weekly Sales'] - weekly['Plan Weekly COGS']
                    weekly['ProfitPercentPlan'] = weekly.apply(lambda x: (x['Profit']/(x['Plan Profit'] * x['Annualize'])) - 1 if pd.to_datetime(x['Day']) > actual_date else (x['Profit']/(x['Plan Profit']) - 1), axis=1)
                    weekly['Plan Margin'] = weekly['Q'].apply(lambda x: float(self.quarterly_margin[self.quarters.index(x)]) if x >= starting_quarter else np.nan)
                    weekly['REVENUE_GROWTH'] = weekly.apply(lambda x: (x['Revenue']/(x['LY REVENUE'] * x['Annualize'])) - 1 if pd.to_datetime(x['Day']) > actual_date else x['REVENUE_GROWTH'], axis=1)
                    weekly['COST_GROWTH'] = weekly.apply(lambda x: (x['Cost']/(x['LY COST'] * x['Annualize'])) - 1 if pd.to_datetime(x['Day']) > actual_date else x['COST_GROWTH'], axis=1)
                    weekly['PROFIT_GROWTH'] = weekly.apply(lambda x: (x['Profit']/(x['LY PROFIT'] * x['Annualize'])) - 1 if pd.to_datetime(x['Day']) > actual_date else x['PROFIT_GROWTH'], axis=1)
                    weekly['VOLUME_GROWTH'] = weekly.apply(lambda x: (x['Volume']/(x['LY VOLUME'] * x['Annualize'])) - 1 if pd.to_datetime(x['Day']) > actual_date else x['VOLUME_GROWTH'], axis=1)
                    weekly['MARGINPercentPlan'] = (weekly['Margin'] - weekly['Plan Margin'])
                    weekly.fillna('', inplace=True)
                    weekly = weekly.astype({"Day" : str})
                    weekly = weekly[[
                                'Annualize','Day', 'Dept', 'Revenue', 'Plan Weekly Sales', 'RevPercentPlan', 'Ninety_Trailing_Revenue', 'LY REVENUE', 'REVENUE_GROWTH',
                                'Cost', 'Plan Weekly COGS', 'COGSPercentPlan', 'Ninety_Trailing_Cost', 'LY COST', 'COST_GROWTH',
                                'Margin', 'Plan Margin', 'MARGINPercentPlan', 'Ninety_Trailing_Margin', 'LY MARGIN', 'YoY Diff',
                                'Profit', 'Plan Profit', 'ProfitPercentPlan', 'Ninety_Trailing_Profit', 'LY PROFIT', 'PROFIT_GROWTH',
                                'Volume', 'Ninety_Trailing_Volume', 'LY VOLUME', 'VOLUME_GROWTH'
                            ]]
                    if self.Name in labor.report_look_ups:
                        weekly = weekly.merge(labor.report(self.Name)[["Week", "Worked Wages", "Worked Hours"]].astype({'Week':str}), how='left', left_on='Day', right_on='Week')
                        weekly['TotalGPVariance'] = ""
                        weekly['VarDuetoMargin'] = ""
                        weekly['VarDueToSalesVolume'] = ""
                        weekly['LaborCost'] = weekly['Worked Wages']
                        weekly['PlanLaborCost'] = ""
                        weekly['PercentPlanLaborCost'] = ""
                        weekly['LYLaborCost'] = ""
                        weekly['LaborCostGrowth'] = ""
                        weekly['LaborHours'] = weekly['Worked Hours']
                        weekly['PlanLaborHours'] = ""
                        weekly['PercentPlanLaborHours'] = ""
                        weekly['LYLaborHours'] = ""
                        weekly['LaborHoursGrowth'] = ""
                        weekly['SalesToLaborHours'] = weekly['LaborCost'] / weekly['LaborHours']
                        weekly['PlanSalesToLaborHours'] = ""
                        weekly['PercentPlanSalesToLaborHours'] = ""
                        weekly['LYSalesToLaborHours'] = ""
                        weekly['SalesToLaborHoursGrowth'] = ""
                        weekly['MarginAfterLabor'] = ((weekly['Revenue']-weekly['LaborCost']) - weekly['Cost'])/weekly['Revenue']
                        weekly['PlanMarginAfterLabor'] = ""
                        weekly['PercentPlanMarginAfterLabor'] = ""
                        weekly['LYMarginAfterLabor'] = ""
                        weekly['MarginAfterLaborGrowth'] = ""
                        weekly.fillna("", inplace=True)
                    else:
                        weekly['Week'] = ""
                        weekly['Worked Wages'] = ""
                        weekly['Worked Hours'] = ""

                        weekly['TotalGPVariance'] = ""
                        weekly['VarDuetoMargin'] = ""
                        weekly['VarDueToSalesVolume'] = ""
                        weekly['LaborCost'] = ""
                        weekly['PlanLaborCost'] = ""
                        weekly['PercentPlanLaborCost'] = ""
                        weekly['LYLaborCost'] = ""
                        weekly['LaborCostGrowth'] = ""
                        weekly['LaborHours'] = ""
                        weekly['PlanLaborHours'] = ""
                        weekly['PercentPlanLaborHours'] = ""
                        weekly['LYLaborHours'] = ""
                        weekly['LaborHoursGrowth'] = ""
                        weekly['SalesToLaborHours'] = ""
                        weekly['PlanSalesToLaborHours'] = ""
                        weekly['PercentPlanSalesToLaborHours'] = ""
                        weekly['LYSalesToLaborHours'] = ""
                        weekly['SalesToLaborHoursGrowth'] = ""
                        weekly['MarginAfterLabor'] = ""
                        weekly['PlanMarginAfterLabor'] = ""
                        weekly['PercentPlanMarginAfterLabor'] = ""
                        weekly['LYMarginAfterLabor'] = ""
                        weekly['MarginAfterLaborGrowth'] = ""
                    #    weekly['Week','Worked Wages','Worked Hours','TotalGPVariance','VarDuetoMargin','VarDueToSalesVolume','LaborCost','PlanLaborCost','PercentPlanLaborCost',
                    #    'LYLaborCost','LaborCostGrowth','LaborHours','PlanLaborHours','PercentPlanLaborHours','LYLaborHours','LaborHoursGrowth','SalesToLaborHours',
                    #    'PlanSalesToLaborHours','PercentPlanSalesToLaborHours','LYSalesToLaborHours','SalesToLaborHoursGrowth','MarginAfterLabor','PlanMarginAfterLabor',
                    #    'PercentPlanMarginAfterLabor','LYMarginAfterLabor','MarginAfterLaborGrowth'] = ""

                    weekly = weekly[[
                                'Annualize','Day', 'Dept', 'Revenue', 'Plan Weekly Sales', 'RevPercentPlan', 'Ninety_Trailing_Revenue', 'LY REVENUE', 'REVENUE_GROWTH',
                                'Cost', 'Plan Weekly COGS', 'COGSPercentPlan', 'Ninety_Trailing_Cost', 'LY COST', 'COST_GROWTH',
                                'Margin', 'Plan Margin', 'MARGINPercentPlan', 'Ninety_Trailing_Margin', 'LY MARGIN', 'YoY Diff',
                                'Profit', 'Plan Profit', 'ProfitPercentPlan', 'Ninety_Trailing_Profit', 'LY PROFIT', 'PROFIT_GROWTH',
                                'Volume', 'Ninety_Trailing_Volume', 'LY VOLUME', 'VOLUME_GROWTH',
                                'TotalGPVariance',
                                'VarDuetoMargin',
                                'VarDueToSalesVolume',
                                'LaborCost',
                                'PlanLaborCost',
                                'PercentPlanLaborCost',
                                'LYLaborCost',
                                'LaborCostGrowth',
                                'LaborHours',
                                'PlanLaborHours',
                                'PercentPlanLaborHours',
                                'LYLaborHours',
                                'LaborHoursGrowth',
                                'SalesToLaborHours',
                                'PlanSalesToLaborHours',
                                'PercentPlanSalesToLaborHours',
                                'LYSalesToLaborHours',
                                'SalesToLaborHoursGrowth',
                                'MarginAfterLabor',
                                'PlanMarginAfterLabor',
                                'PercentPlanMarginAfterLabor',
                                'LYMarginAfterLabor',
                                'MarginAfterLaborGrowth'
                            ]]
                    #print(weekly)
                    #print(weekly)
                    self.weekly_totals = weekly
                case "Monthly":
                    starting_quarter = pd.Period('2023Q1')
                    starting_month = pd.Period('2023-01')
                    monthly = pd.read_sql("""
                        select
                        1.0 - CAST(DATEDIFF(day, GETDATE(), '2022-09-30')/30.0 as float) as 'Annualize',
                        revenueReport.F254 as 'Day',
                        'Store Wide' as 'Dept',
                        sum(revenueReport.F65) as 'Revenue',
                        ROUND(AVG(sum(revenueReport.F65)) OVER (order by revenueReport.F254 ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) as 'Ninety_Trailing_Revenue',
                        LAG(sum(revenueReport.F65), 12) OVER (order by revenueReport.F254) as 'LY REVENUE',
                        ROUND((sum(revenueReport.F65)/(LAG(sum(revenueReport.F65), 12) OVER (order by revenueReport.F254))-1), 4) as 'REVENUE_GROWTH',
                        costReport.F65 as 'Cost',
                        ROUND(AVG(costReport.F65) OVER (order by revenueReport.F254 ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) as 'Ninety_Trailing_Cost',
                        LAG(costReport.F65, 12) OVER (order by revenueReport.F254) as 'LY COST',
                        ROUND((costReport.F65/(LAG(costReport.F65, 12) OVER (order by revenueReport.F254))-1), 4) as 'COST_GROWTH',
                        ROUND(((sum(revenueReport.F65)-costReport.F65)/sum(revenueReport.F65)),4) as 'Margin',
                        ROUND(AVG(((sum(revenueReport.F65)-costReport.F65)/sum(revenueReport.F65))) OVER (order by revenueReport.F254 ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 4) as 'Ninety_Trailing_Margin',
                        LAG(ROUND(((sum(revenueReport.F65) - costReport.F65) / sum(revenueReport.F65)), 4), 12) OVER (order by revenueReport.F254) as 'LY MARGIN',
                        ROUND(((sum(revenueReport.F65) - costReport.F65) / sum(revenueReport.F65)), 4) - LAG(ROUND(((sum(revenueReport.F65) - costReport.F65) / sum(revenueReport.F65)), 4), 12) OVER (order by revenueReport.F254) as 'YoY Diff',
                        (sum(revenueReport.F65)-costReport.F65) as 'Profit',
                        ROUND(AVG((sum(revenueReport.F65)-costReport.F65)) OVER (order by revenueReport.F254 ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) as 'Ninety_Trailing_Profit',
                        LAG((sum(revenueReport.F65)-costReport.F65), 12) OVER (order by revenueReport.F254) as 'LY PROFIT',
                        ROUND((sum(revenueReport.F65)-costReport.F65)/(LAG((sum(revenueReport.F65)-costReport.F65), 12) OVER (order by revenueReport.F254))-1, 4) as 'PROFIT_GROWTH',
                        sum(revenueReport.F64) as 'Volume',
                        ROUND(AVG(sum(revenueReport.F64)) OVER (order by revenueReport.F254 ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) as 'Ninety_Trailing_Volume',
                        LAG(sum(revenueReport.F64), 12) OVER (order by revenueReport.F254) as 'LY VOLUME',
                        ROUND((sum(revenueReport.F64)/(LAG(sum(revenueReport.F64), 12) OVER (order by revenueReport.F254))-1), 4) as 'VOLUME_GROWTH'
                        from (select * from STORESQL.dbo.RPT_FIN
                        where F1031 = 'M' and F1034 = 2) revenueReport
                        inner join (select * from STORESQL.dbo.RPT_FIN where F1031 = 'M' and F1034 = 8101) costReport on revenueReport.F254 = costReport.F254
                        group by revenueReport.F254, costReport.F65
                        order by revenueReport.F254 desc
                        """,cnxn)
                   
                    monthly['Revenue'] = monthly['Revenue'].astype(float)
                    monthly['DOM'] = monthly['Day'].apply(lambda x: x.date())
                    monthly['Q'] = monthly['Day'].apply(lambda x: (pd.to_datetime(x) + pd.offsets.QuarterEnd(n=3)).to_period("Q"))
                    monthly['M'] = monthly['Day'].apply(lambda x: (pd.to_datetime(x) + pd.offsets.MonthEnd(n=6)).to_period("M"))
                    monthly['Plan Monthly Sales'] = monthly['M'].apply(lambda x: float(self.monthly_sales[self.months.index(x)]) if x >= starting_month else np.nan)
                    monthly['RevPercentPlan'] = monthly.apply(lambda x: (x['Revenue']/(x['Plan Monthly Sales'] * x['Annualize'])) - 1 if pd.to_datetime(x['Day']) > actual_date else (x['Revenue']/(x['Plan Monthly Sales']) - 1), axis=1)
                    monthly['Plan Monthly COGS'] = monthly['M'].apply(lambda x: float(self.monthly_COGS[self.months.index(x)]) if x >= starting_month else np.nan)
                    monthly['COGSPercentPlan'] = monthly.apply(lambda x: (x['Cost']/(x['Plan Monthly COGS'] * x['Annualize'])) - 1 if pd.to_datetime(x['Day']) > actual_date else (x['Cost']/(x['Plan Monthly COGS']) - 1), axis=1)
                    monthly['Plan Profit'] = monthly['Plan Monthly Sales'] - monthly['Plan Monthly COGS']
                    monthly['ProfitPercentPlan'] = monthly.apply(lambda x: (x['Profit']/(x['Plan Profit'] * x['Annualize'])) - 1 if pd.to_datetime(x['Day']) > actual_date else (x['Profit']/(x['Plan Profit']) - 1), axis=1)
                    monthly['Plan Margin'] = monthly['Q'].apply(lambda x: float(self.quarterly_margin[self.quarters.index(x)]) if x >= starting_quarter else np.nan)
                    monthly['REVENUE_GROWTH'] = monthly.apply(lambda x: (x['Revenue']/(x['LY REVENUE'] * x['Annualize'])) - 1 if pd.to_datetime(x['Day']) > actual_date else x['REVENUE_GROWTH'], axis=1)
                    monthly['COST_GROWTH'] = monthly.apply(lambda x: (x['Cost']/(x['LY COST'] * x['Annualize'])) - 1 if pd.to_datetime(x['Day']) > actual_date else x['COST_GROWTH'], axis=1)
                    monthly['PROFIT_GROWTH'] = monthly.apply(lambda x: (x['Profit']/(x['LY PROFIT'] * x['Annualize'])) - 1 if pd.to_datetime(x['Day']) > actual_date else x['PROFIT_GROWTH'], axis=1)
                    monthly['VOLUME_GROWTH'] = monthly.apply(lambda x: (x['Volume']/(x['LY VOLUME'] * x['Annualize'])) - 1 if pd.to_datetime(x['Day']) > actual_date else x['VOLUME_GROWTH'], axis=1)
                    monthly['MARGINPercentPlan'] = (monthly['Margin'] - monthly['Plan Margin'])
                    monthly.fillna('', inplace=True)
                    monthly = monthly.astype({"Day" : str})
                    monthly = monthly[[
                                'Annualize','Day', 'Dept', 'Revenue', 'Plan Monthly Sales', 'RevPercentPlan', 'Ninety_Trailing_Revenue', 'LY REVENUE', 'REVENUE_GROWTH',
                                'Cost', 'Plan Monthly COGS', 'COGSPercentPlan', 'Ninety_Trailing_Cost', 'LY COST', 'COST_GROWTH',
                                'Margin', 'Plan Margin', 'MARGINPercentPlan', 'Ninety_Trailing_Margin', 'LY MARGIN', 'YoY Diff',
                                'Profit', 'Plan Profit', 'ProfitPercentPlan', 'Ninety_Trailing_Profit', 'LY PROFIT', 'PROFIT_GROWTH',
                                'Volume', 'Ninety_Trailing_Volume', 'LY VOLUME', 'VOLUME_GROWTH'
                            ]]
                    self.monthly_totals = monthly
                case "Quarterly":
                    starting_quarter = pd.Period('2023Q1')
                    # quarterly = pd.read_sql("""
                    # select
                    # 1.0 - CAST(DATEDIFF(day, GETDATE(), '2022-09-30')/92.0 as float) as 'Annualize',
                    # qtr.[Begin], qtr.[End], DATEDIFF(dd, qtr.[Begin], qtr.[End]) as 'QtrLength',
                    # qtr.[Quarter], qtr.Dept,
                    # sum(qtr.Revenue) as 'Revenue',
                    # LAG(sum(qtr.Revenue), 4) OVER (partition by qtr.Dept order by qtr.[Quarter], qtr.Dept) as 'LY REVENUE',
                    # ROUND((sum(qtr.Revenue)/LAG(sum(qtr.Revenue), 4) OVER (partition by qtr.Dept order by qtr.[Quarter], qtr.Dept))-1, 4) as 'REVENUE_GROWTH',
                    # sum(qtr.Cost) as 'Cost',
                    # LAG(sum(qtr.Cost), 4) OVER (partition by qtr.Dept order by qtr.[Quarter], qtr.Dept) as 'LY COST',
                    # ROUND((sum(qtr.Cost)/LAG(sum(qtr.Cost), 4) OVER (partition by qtr.Dept order by qtr.[Quarter], qtr.Dept))-1, 4) as 'COST_GROWTH',
                    # sum(qtr.Revenue) - sum(qtr.Cost) as 'Profit',
                    # LAG(sum(qtr.Revenue)-sum(qtr.Cost), 4) OVER (partition by qtr.Dept order by qtr.[Quarter], qtr.Dept) as 'LY PROFIT',
                    # ROUND((sum(qtr.Revenue) - sum(qtr.Cost)) / LAG(sum(qtr.Revenue)-sum(qtr.Cost), 4) OVER (partition by qtr.Dept order by qtr.[Quarter], qtr.Dept) -1, 4) as 'PROFIT_GROWTH',
                    # (sum(qtr.Revenue) - sum(qtr.Cost))/ sum(qtr.Revenue) as 'Margin',
                    # LAG((sum(qtr.Revenue)-sum(qtr.Cost))/sum(qtr.Revenue), 4) OVER (partition by qtr.Dept order by qtr.[Quarter], qtr.Dept) as 'LY MARGIN',
                    # (sum(qtr.Revenue) - sum(qtr.Cost))/ sum(qtr.Revenue) - LAG((sum(qtr.Revenue)-sum(qtr.Cost))/sum(qtr.Revenue), 4) OVER (partition by qtr.Dept order by qtr.[Quarter], qtr.Dept) as 'YoY Diff',
                    # sum(qtr.Volume) as 'Volume',
                    # LAG(sum(qtr.Volume), 4) OVER (partition by qtr.Dept order by qtr.[Quarter], qtr.Dept) as 'LY VOLUME',
                    # ROUND((sum(qtr.Volume)/LAG(sum(qtr.Volume), 4) OVER (partition by qtr.Dept order by qtr.[Quarter], qtr.Dept))-1, 4) as 'VOLUME_GROWTH'
                    # from 
                    # (select 
                    # DATEADD(qq, DATEDIFF(qq, 0, revenueReport.F254), 0) as 'Begin',
                    # DATEADD(d, -1, DATEADD(q, DATEDIFF(q, 0, revenueReport.F254) + 1, 0)) as 'End',
                    # CASE
                    #         WHEN MONTH(revenueReport.F254) BETWEEN 7 AND 9 THEN convert(char(4), YEAR(revenueReport.F254) + 1) + 'Q1'
                    #         WHEN MONTH(revenueReport.F254) BETWEEN 10 AND 12 THEN convert(char(4), YEAR(revenueReport.F254) + 1) + 'Q2'
                    #         WHEN MONTH(revenueReport.F254) BETWEEN 1 AND 3 THEN convert(char(4), YEAR(revenueReport.F254)) + 'Q3'
                    #         WHEN MONTH(revenueReport.F254) BETWEEN 4 AND 6 THEN convert(char(4), YEAR(revenueReport.F254)) + 'Q4'
                    # END AS 'Quarter',
                    # reportCodeTable.F1024 as 'Dept',
                    # sum(revenueReport.F65) as 'Revenue',
                    # costReport.F65 as 'Cost',
                    # sum(revenueReport.F64) as 'Volume'
                    # from
                    # (select * from STORESQL.dbo.RPT_DPT 
                    # where F1031 = 'M' and F1034 in (3, 3303) and F03 = %s) revenueReport
                    # inner join (select * from STORESQL.dbo.RPT_DPT where F1031 = 'M' and F1034 = 8101 and F03 = %s) costReport on revenueReport.F254 = costReport.F254 and revenueReport.F03 = costReport.F03
                    # inner join (select F18, F1024 from STORESQL.dbo.RPC_TAB) reportCodeTable on revenueReport.F03 = reportCodeTable.F18
                    # where revenueReport.F03 not in (14, 21, 23, 97, 98, 99)
                    # group by
                    # DATEADD(qq, DATEDIFF(qq, 0, revenueReport.F254), 0),
                    # DATEADD(d, -1, DATEADD(q, DATEDIFF(q, 0, revenueReport.F254) + 1, 0)),
                    # CASE
                    #         WHEN MONTH(revenueReport.F254) BETWEEN 7 AND 9 THEN convert(char(4), YEAR(revenueReport.F254) + 1) + 'Q1'
                    #         WHEN MONTH(revenueReport.F254) BETWEEN 10 AND 12 THEN convert(char(4), YEAR(revenueReport.F254) + 1) + 'Q2'
                    #         WHEN MONTH(revenueReport.F254) BETWEEN 1 AND 3 THEN convert(char(4), YEAR(revenueReport.F254)) + 'Q3'
                    #         WHEN MONTH(revenueReport.F254) BETWEEN 4 AND 6 THEN convert(char(4), YEAR(revenueReport.F254)) + 'Q4'
                    # END,
                    # revenueReport.F03, costReport.F03, costReport.F65, reportCodeTable.F1024)
                    # qtr
                    # group by qtr.[Begin], qtr.[End], qtr.[Quarter], qtr.[Dept]
                    # order by qtr.[Dept], qtr.[Quarter] desc
                    # """ % (self.report_code, self.report_code),cnxn)
                    quarterly = pd.read_sql("""
                        select
                        1.0 - CAST(DATEDIFF(day, GETDATE(), '2022-09-30')/92.0 as float) as 'Annualize',
                        qtr.[Quarter] as 'Quarter',
                        qtr.[Begin] as 'Begin', qtr.[End] as 'End',
                        'Store Wide' as 'Dept',
                        sum(qtr.[Revenue]) as 'Revenue',
                        LAG(sum(qtr.[Revenue]), 4) OVER (order by qtr.[Quarter]) as 'LY REVENUE',
                        ROUND((sum(qtr.[Revenue])/(LAG(sum(qtr.[Revenue]), 4) OVER (order by qtr.[Quarter]))-1), 4) as 'REVENUE_GROWTH',
                        qtr.[Cost] as 'Cost',
                        LAG(qtr.[Cost], 4) OVER (order by qtr.[Quarter]) as 'LY COST',
                        ROUND((qtr.[Cost]/(LAG(qtr.[Cost], 4) OVER (order by qtr.[Quarter]))-1), 4) as 'COST_GROWTH',
                        ROUND(((sum(qtr.[Revenue])-qtr.[Cost])/sum(qtr.[Revenue])),4) as 'Margin',
                        LAG(ROUND(((sum(qtr.[Revenue]) - qtr.[Cost]) / sum(qtr.[Revenue])), 4), 4) OVER (order by qtr.[Quarter]) as 'LY MARGIN',
                        ROUND(((sum(qtr.[Revenue]) - qtr.[Cost]) / sum(qtr.[Revenue])), 4) - LAG(ROUND(((sum(qtr.[Revenue]) - qtr.[Cost]) / sum(qtr.[Revenue])), 4), 4) OVER (order by qtr.[Quarter]) as 'YoY Diff',
                        (sum(qtr.[Revenue])-qtr.[Cost]) as 'Profit',
                        LAG((sum(qtr.[Revenue])-qtr.[Cost]), 4) OVER (order by qtr.[Quarter]) as 'LY PROFIT',
                        ROUND((sum(qtr.[Revenue])-qtr.[Cost])/(LAG((sum(qtr.[Revenue])-qtr.[Cost]), 4) OVER (order by qtr.[Quarter]))-1, 4) as 'PROFIT_GROWTH',
                        sum(qtr.[Volume]) as 'Volume',
                        LAG(sum(qtr.[Volume]), 4) OVER (order by qtr.[Quarter]) as 'LY VOLUME',
                        ROUND((sum(qtr.[Volume])/(LAG(sum(qtr.[Volume]), 4) OVER (order by qtr.[Quarter]))-1), 4) as 'VOLUME_GROWTH'
                        from
                        (select 
                        DATEADD(qq, DATEDIFF(qq, 0, revenueReport.F254), 0) as 'Begin',
                        DATEADD(d, -1, DATEADD(q, DATEDIFF(q, 0, revenueReport.F254) + 1, 0)) as 'End',
                        CASE
                        WHEN MONTH(revenueReport.F254) BETWEEN 7 AND 9 THEN convert(char(4), YEAR(revenueReport.F254) + 1) + 'Q1'
                        WHEN MONTH(revenueReport.F254) BETWEEN 10 AND 12 THEN convert(char(4), YEAR(revenueReport.F254) + 1) + 'Q2'
                        WHEN MONTH(revenueReport.F254) BETWEEN 1 AND 3 THEN convert(char(4), YEAR(revenueReport.F254)) + 'Q3'
                        WHEN MONTH(revenueReport.F254) BETWEEN 4 AND 6 THEN convert(char(4), YEAR(revenueReport.F254)) + 'Q4'
                        END AS 'Quarter',
                        sum(revenueReport.F65) as 'Revenue',
                        sum(costReport.F65) as 'Cost',
                        sum(revenueReport.F64) as 'Volume'
                        from
                        (select * from STORESQL.dbo.RPT_FIN
                        where F1031 = 'M' and F1034 = 2) revenueReport
                        inner join (select * from STORESQL.dbo.RPT_FIN where F1031 = 'M' and F1034 = 8101) costReport on revenueReport.F254 = costReport.F254
                        group by 
                        DATEADD(qq, DATEDIFF(qq, 0, revenueReport.F254), 0),
                        DATEADD(d, -1, DATEADD(q, DATEDIFF(q, 0, revenueReport.F254) + 1, 0)),
                        CASE
                        WHEN MONTH(revenueReport.F254) BETWEEN 7 AND 9 THEN convert(char(4), YEAR(revenueReport.F254) + 1) + 'Q1'
                        WHEN MONTH(revenueReport.F254) BETWEEN 10 AND 12 THEN convert(char(4), YEAR(revenueReport.F254) + 1) + 'Q2'
                        WHEN MONTH(revenueReport.F254) BETWEEN 1 AND 3 THEN convert(char(4), YEAR(revenueReport.F254)) + 'Q3'
                        WHEN MONTH(revenueReport.F254) BETWEEN 4 AND 6 THEN convert(char(4), YEAR(revenueReport.F254)) + 'Q4'
                        END) qtr
                        group by qtr.[Quarter],qtr.[Begin], qtr.[End], qtr.[Cost], qtr.[Revenue], qtr.[Volume]
                        order by qtr.[Quarter] desc
                    """, cnxn)
                    
                    quarterly['Revenue'] = quarterly['Revenue'].astype(float)
                    quarterly['Q'] = quarterly['Begin'].apply(lambda x: (pd.to_datetime(x) + pd.offsets.QuarterEnd(n=3)).to_period("Q"))
                    quarterly['Plan Quarterly Sales'] = quarterly['Q'].apply(lambda x: float(self.quarterly_sales[self.quarters.index(x)]) if x >= starting_quarter else np.nan)
                    quarterly['RevPercentPlan'] = quarterly.apply(lambda x: (x['Revenue']/(x['Plan Quarterly Sales'] * (x['Annualize']))) - 1 if pd.to_datetime(x['End']) > actual_date else (x['Revenue']/(x['Plan Quarterly Sales']) - 1), axis=1)
                    quarterly['Plan Quarterly COGS'] = quarterly['Q'].apply(lambda x: float(self.quarterly_COGS[self.quarters.index(x)]) if x >= starting_quarter else np.nan)
                    quarterly['COGSPercentPlan'] = quarterly.apply(lambda x: (x['Cost']/(x['Plan Quarterly COGS'] * (x['Annualize']))) - 1 if pd.to_datetime(x['End']) > actual_date else (x['Cost']/(x['Plan Quarterly COGS']) - 1), axis=1)
                    quarterly['Plan Profit'] = quarterly['Plan Quarterly Sales'] - quarterly['Plan Quarterly COGS']
                    quarterly['ProfitPercentPlan'] = quarterly.apply(lambda x: (x['Profit']/(x['Plan Profit'] * (x['Annualize']))) - 1 if pd.to_datetime(x['End']) > actual_date else (x['Profit']/(x['Plan Profit']) - 1), axis=1)
                    quarterly['Plan Margin'] = quarterly['Q'].apply(lambda x: float(self.quarterly_margin[self.quarters.index(x)]) if x >= starting_quarter else np.nan)
                    quarterly['MARGINPercentPlan'] = (quarterly['Margin'] - quarterly['Plan Margin'])
                    quarterly.fillna('', inplace=True)

                    quarterly = quarterly[[
                                'Annualize','End','Quarter', 'Dept', 'Revenue', 'Plan Quarterly Sales', 'RevPercentPlan', 'LY REVENUE', 'REVENUE_GROWTH',
                                'Cost', 'Plan Quarterly COGS', 'COGSPercentPlan', 'LY COST', 'COST_GROWTH',
                                'Margin', 'Plan Margin', 'MARGINPercentPlan', 'LY MARGIN', 'YoY Diff',
                                'Profit', 'Plan Profit', 'ProfitPercentPlan', 'LY PROFIT', 'PROFIT_GROWTH',
                                'Volume', 'LY VOLUME', 'VOLUME_GROWTH'
                            ]]
                    if self.Name in labor.report_look_ups:
                        
                        labor = labor.report(self.Name).groupby('Dept').agg({"Worked Wages" : sum, "Worked Hours" : sum})
                        labor['Quarter'] = '2023Q1'
                        print("LABOR", labor)
                        quarterly = quarterly.merge(labor, how='left', left_on='Quarter', right_on='Quarter')
                        quarterly['TotalGPVariance'] = ""
                        quarterly['VarDuetoMargin'] = ""
                        quarterly['VarDueToSalesVolume'] = ""
                        quarterly['LaborCost'] = quarterly['Worked Wages']
                        quarterly['PlanLaborCost'] = ""
                        quarterly['PercentPlanLaborCost'] = ""
                        quarterly['LYLaborCost'] = ""
                        quarterly['LaborCostGrowth'] = ""
                        quarterly['LaborHours'] = quarterly['Worked Hours']
                        quarterly['PlanLaborHours'] = ""
                        quarterly['PercentPlanLaborHours'] = ""
                        quarterly['LYLaborHours'] = ""
                        quarterly['LaborHoursGrowth'] = ""
                        quarterly['SalesToLaborHours'] = quarterly['LaborCost'] / quarterly['LaborHours']
                        quarterly['PlanSalesToLaborHours'] = ""
                        quarterly['PercentPlanSalesToLaborHours'] = ""
                        quarterly['LYSalesToLaborHours'] = ""
                        quarterly['SalesToLaborHoursGrowth'] = ""
                        quarterly['MarginAfterLabor'] = ((quarterly['Revenue']-quarterly['LaborCost']) - quarterly['Cost'])/quarterly['Revenue']
                        quarterly['PlanMarginAfterLabor'] = ""
                        quarterly['PercentPlanMarginAfterLabor'] = ""
                        quarterly['LYMarginAfterLabor'] = ""
                        quarterly['MarginAfterLaborGrowth'] = ""
                        quarterly.fillna("", inplace=True)
                    else:
                        
                        quarterly['Worked Wages'] = ""
                        quarterly['Worked Hours'] = ""

                        quarterly['TotalGPVariance'] = ""
                        quarterly['VarDuetoMargin'] = ""
                        quarterly['VarDueToSalesVolume'] = ""
                        quarterly['LaborCost'] = ""
                        quarterly['PlanLaborCost'] = ""
                        quarterly['PercentPlanLaborCost'] = ""
                        quarterly['LYLaborCost'] = ""
                        quarterly['LaborCostGrowth'] = ""
                        quarterly['LaborHours'] = ""
                        quarterly['PlanLaborHours'] = ""
                        quarterly['PercentPlanLaborHours'] = ""
                        quarterly['LYLaborHours'] = ""
                        quarterly['LaborHoursGrowth'] = ""
                        quarterly['SalesToLaborHours'] = ""
                        quarterly['PlanSalesToLaborHours'] = ""
                        quarterly['PercentPlanSalesToLaborHours'] = ""
                        quarterly['LYSalesToLaborHours'] = ""
                        quarterly['SalesToLaborHoursGrowth'] = ""
                        quarterly['MarginAfterLabor'] = ""
                        quarterly['PlanMarginAfterLabor'] = ""
                        quarterly['PercentPlanMarginAfterLabor'] = ""
                        quarterly['LYMarginAfterLabor'] = ""
                        quarterly['MarginAfterLaborGrowth'] = ""
                    #    quarterly['Week','Worked Wages','Worked Hours','TotalGPVariance','VarDuetoMargin','VarDueToSalesVolume','LaborCost','PlanLaborCost','PercentPlanLaborCost',
                    #    'LYLaborCost','LaborCostGrowth','LaborHours','PlanLaborHours','PercentPlanLaborHours','LYLaborHours','LaborHoursGrowth','SalesToLaborHours',
                    #    'PlanSalesToLaborHours','PercentPlanSalesToLaborHours','LYSalesToLaborHours','SalesToLaborHoursGrowth','MarginAfterLabor','PlanMarginAfterLabor',
                    #    'PercentPlanMarginAfterLabor','LYMarginAfterLabor','MarginAfterLaborGrowth'] = ""

                    quarterly = quarterly[[
                                'End', 'Annualize','Quarter', 'Dept', 'Revenue', 'Plan Quarterly Sales', 'RevPercentPlan', 'LY REVENUE', 'REVENUE_GROWTH',
                                'Cost', 'Plan Quarterly COGS', 'COGSPercentPlan', 'LY COST', 'COST_GROWTH',
                                'Margin', 'Plan Margin', 'MARGINPercentPlan', 'LY MARGIN', 'YoY Diff',
                                'Profit', 'Plan Profit', 'ProfitPercentPlan', 'LY PROFIT', 'PROFIT_GROWTH',
                                'Volume', 'LY VOLUME', 'VOLUME_GROWTH',
                                'TotalGPVariance',
                                'VarDuetoMargin',
                                'VarDueToSalesVolume',
                                'LaborCost',
                                'PlanLaborCost',
                                'PercentPlanLaborCost',
                                'LYLaborCost',
                                'LaborCostGrowth',
                                'LaborHours',
                                'PlanLaborHours',
                                'PercentPlanLaborHours',
                                'LYLaborHours',
                                'LaborHoursGrowth',
                                'SalesToLaborHours',
                                'PlanSalesToLaborHours',
                                'PercentPlanSalesToLaborHours',
                                'LYSalesToLaborHours',
                                'SalesToLaborHoursGrowth',
                                'MarginAfterLabor',
                                'PlanMarginAfterLabor',
                                'PercentPlanMarginAfterLabor',
                                'LYMarginAfterLabor',
                                'MarginAfterLaborGrowth'
                            ]]
                    self.quarterly_totals = quarterly

                case "Yearly":
                    starting_quarter = pd.Period('2023Q1')
                    yearly = pd.read_sql(
                    # """
                    # select
                    # 1.0 - CAST(DATEDIFF(day, GETDATE(), '2023-06-30')/365.0 as float) as 'Annualize',
                    # qtr.[Quarter] as 'Year', qtr.Dept,
                    # sum(qtr.Revenue) as 'Revenue',
                    # LAG(sum(qtr.Revenue), 1) OVER (partition by qtr.Dept order by qtr.[Quarter], qtr.Dept) as 'LY REVENUE',
                    # ROUND((sum(qtr.Revenue)/LAG(sum(qtr.Revenue), 1) OVER (partition by qtr.Dept order by qtr.[Quarter], qtr.Dept))-1, 4) as 'REVENUE_GROWTH',
                    # sum(qtr.Cost) as 'Cost',
                    # LAG(sum(qtr.Cost),1) OVER (partition by qtr.Dept order by qtr.[Quarter], qtr.Dept) as 'LY COST',
                    # ROUND((sum(qtr.Cost)/LAG(sum(qtr.Cost), 1) OVER (partition by qtr.Dept order by qtr.[Quarter], qtr.Dept))-1, 4) as 'COST_GROWTH',
                    # sum(qtr.Revenue) - sum(qtr.Cost) as 'Profit',
                    # LAG(sum(qtr.Revenue)-sum(qtr.Cost), 1) OVER (partition by qtr.Dept order by qtr.[Quarter], qtr.Dept) as 'LY PROFIT',
                    # ROUND((sum(qtr.Revenue) - sum(qtr.Cost)) / LAG(sum(qtr.Revenue)-sum(qtr.Cost), 1) OVER (partition by qtr.Dept order by qtr.[Quarter], qtr.Dept) -1, 4) as 'PROFIT_GROWTH',
                    # (sum(qtr.Revenue) - sum(qtr.Cost))/ sum(qtr.Revenue) as 'Margin',
                    # LAG((sum(qtr.Revenue)-sum(qtr.Cost))/sum(qtr.Revenue), 1) OVER (partition by qtr.Dept order by qtr.[Quarter], qtr.Dept) as 'LY MARGIN',
                    # (sum(qtr.Revenue) - sum(qtr.Cost))/ sum(qtr.Revenue) - LAG((sum(qtr.Revenue)-sum(qtr.Cost))/sum(qtr.Revenue), 1) OVER (partition by qtr.Dept order by qtr.[Quarter], qtr.Dept) as 'YoY Diff',
                    # sum(qtr.Volume) as 'Volume',
                    # LAG(sum(qtr.Volume), 1) OVER (partition by qtr.Dept order by qtr.[Quarter], qtr.Dept) as 'LY VOLUME',
                    # ROUND((sum(qtr.Volume)/LAG(sum(qtr.Volume), 4) OVER (partition by qtr.Dept order by qtr.[Quarter], qtr.Dept))-1, 4) as 'VOLUME_GROWTH'
                    # from 
                    # (select CASE
                    #         WHEN MONTH(revenueReport.F254) BETWEEN 7 AND 9 THEN convert(char(4), YEAR(revenueReport.F254) + 1)
                    #         WHEN MONTH(revenueReport.F254) BETWEEN 10 AND 12 THEN convert(char(4), YEAR(revenueReport.F254) + 1)
                    #         WHEN MONTH(revenueReport.F254) BETWEEN 1 AND 3 THEN convert(char(4), YEAR(revenueReport.F254))
                    #         WHEN MONTH(revenueReport.F254) BETWEEN 4 AND 6 THEN convert(char(4), YEAR(revenueReport.F254))
                    # END AS 'Quarter',
                    # reportCodeTable.F1024 as 'Dept',
                    # sum(revenueReport.F65) as 'Revenue',
                    # costReport.F65 as 'Cost',
                    # sum(revenueReport.F64) as 'Volume'
                    # from
                    # (select * from STORESQL.dbo.RPT_DPT 
                    # where F1031 = 'M' and F1034 in (3, 3303) and F03 = %s) revenueReport
                    # inner join (select * from STORESQL.dbo.RPT_DPT where F1031 = 'M' and F1034 = 8101 and F03 = %s) costReport on revenueReport.F254 = costReport.F254 and revenueReport.F03 = costReport.F03
                    # inner join (select F18, F1024 from STORESQL.dbo.RPC_TAB) reportCodeTable on revenueReport.F03 = reportCodeTable.F18
                    # where revenueReport.F03 not in (14, 21, 23, 97, 98, 99)
                    # group by
                    # CASE
                    #         WHEN MONTH(revenueReport.F254) BETWEEN 7 AND 9 THEN convert(char(4), YEAR(revenueReport.F254) + 1)
                    #         WHEN MONTH(revenueReport.F254) BETWEEN 10 AND 12 THEN convert(char(4), YEAR(revenueReport.F254) + 1)
                    #         WHEN MONTH(revenueReport.F254) BETWEEN 1 AND 3 THEN convert(char(4), YEAR(revenueReport.F254))
                    #         WHEN MONTH(revenueReport.F254) BETWEEN 4 AND 6 THEN convert(char(4), YEAR(revenueReport.F254))
                    # END,
                    # revenueReport.F03, costReport.F03, costReport.F65, reportCodeTable.F1024)
                    # qtr
                    # group by qtr.[Quarter], qtr.[Dept]
                    # order by  qtr.[Quarter] desc, qtr.[Dept]
                    """
                    select
                    1.0 - CAST(DATEDIFF(day, GETDATE(), '2023-06-30')/365.0 as float) as 'Annualize',
                    qtr.[Quarter] as 'Year',
                    'Store Wide' as 'Dept',
                    sum(qtr.[Revenue]) as 'Revenue',
                    LAG(sum(qtr.[Revenue]), 1) OVER (order by qtr.[Quarter]) as 'LY REVENUE',
                    ROUND((sum(qtr.[Revenue])/(LAG(sum(qtr.[Revenue]), 1) OVER (order by qtr.[Quarter]))-1), 4) as 'REVENUE_GROWTH',
                    qtr.[Cost] as 'Cost',
                    LAG(qtr.[Cost], 1) OVER (order by qtr.[Quarter]) as 'LY COST',
                    ROUND((qtr.[Cost]/(LAG(qtr.[Cost], 1) OVER (order by qtr.[Quarter]))-1), 4) as 'COST_GROWTH',
                    ROUND(((sum(qtr.[Revenue])-qtr.[Cost])/sum(qtr.[Revenue])),4) as 'Margin',
                    LAG(ROUND(((sum(qtr.[Revenue]) - qtr.[Cost]) / sum(qtr.[Revenue])), 4), 1) OVER (order by qtr.[Quarter]) as 'LY MARGIN',
                    ROUND(((sum(qtr.[Revenue]) - qtr.[Cost]) / sum(qtr.[Revenue])), 4) - LAG(ROUND(((sum(qtr.[Revenue]) - qtr.[Cost]) / sum(qtr.[Revenue])), 4), 1) OVER (order by qtr.[Quarter]) as 'YoY Diff',
                    (sum(qtr.[Revenue])-qtr.[Cost]) as 'Profit',
                    LAG((sum(qtr.[Revenue])-qtr.[Cost]), 1) OVER (order by qtr.[Quarter]) as 'LY PROFIT',
                    ROUND((sum(qtr.[Revenue])-qtr.[Cost])/(LAG((sum(qtr.[Revenue])-qtr.[Cost]), 4) OVER (order by qtr.[Quarter]))-1, 1) as 'PROFIT_GROWTH',
                    sum(qtr.[Volume]) as 'Volume',
                    LAG(sum(qtr.[Volume]), 1) OVER (order by qtr.[Quarter]) as 'LY VOLUME',
                    ROUND((sum(qtr.[Volume])/(LAG(sum(qtr.[Volume]), 4) OVER (order by qtr.[Quarter]))-1), 1) as 'VOLUME_GROWTH'
                    from
                    (select 
                    CASE
                    WHEN MONTH(revenueReport.F254) BETWEEN 7 AND 9 THEN convert(char(4), YEAR(revenueReport.F254) + 1)
                    WHEN MONTH(revenueReport.F254) BETWEEN 10 AND 12 THEN convert(char(4), YEAR(revenueReport.F254) + 1)
                    WHEN MONTH(revenueReport.F254) BETWEEN 1 AND 3 THEN convert(char(4), YEAR(revenueReport.F254))
                    WHEN MONTH(revenueReport.F254) BETWEEN 4 AND 6 THEN convert(char(4), YEAR(revenueReport.F254))
                    END AS 'Quarter',
                    sum(revenueReport.F65) as 'Revenue',
                    sum(costReport.F65) as 'Cost',
                    sum(revenueReport.F64) as 'Volume'
                    from
                    (select * from STORESQL.dbo.RPT_FIN
                    where F1031 = 'M' and F1034 = 2) revenueReport
                    inner join (select * from STORESQL.dbo.RPT_FIN where F1031 = 'M' and F1034 = 8101) costReport on revenueReport.F254 = costReport.F254
                    group by 
                    CASE
                    WHEN MONTH(revenueReport.F254) BETWEEN 7 AND 9 THEN convert(char(4), YEAR(revenueReport.F254) + 1)
                    WHEN MONTH(revenueReport.F254) BETWEEN 10 AND 12 THEN convert(char(4), YEAR(revenueReport.F254) + 1)
                    WHEN MONTH(revenueReport.F254) BETWEEN 1 AND 3 THEN convert(char(4), YEAR(revenueReport.F254))
                    WHEN MONTH(revenueReport.F254) BETWEEN 4 AND 6 THEN convert(char(4), YEAR(revenueReport.F254))
                    END) qtr
                    group by qtr.[Quarter],qtr.[Cost], qtr.[Revenue], qtr.[Volume]
                    order by qtr.[Quarter] desc
                    """, cnxn)

                    yearly['Revenue'] = yearly['Revenue'].astype(float)
                    #yearly['Q'] = yearly['Begin'].apply(lambda x: (pd.to_datetime(x) + pd.offsets.QuarterEnd(n=3)).to_period("Q"))
                    yearly['Plan Yearly Sales'] = yearly['Year'].apply(lambda x: float(self.year_sales) if x == "2023" else np.nan)
                    yearly['RevPercentPlan'] = yearly.apply(lambda x: (x['Revenue']/(x['Plan Yearly Sales'] * x['Annualize'])) - 1 if x['Year'] == "2023" else np.nan, axis=1)
                    yearly['Plan Yearly COGS'] = yearly['Year'].apply(lambda x: float(self.year_COGS) if x == "2023" else np.nan)
                    yearly['COGSPercentPlan'] = yearly.apply(lambda x: (x['Revenue']/(x['Plan Yearly COGS'] * x['Annualize'])) - 1 if x['Year'] == "2023" else np.nan, axis=1)
                    yearly['Plan Profit'] = yearly['Plan Yearly Sales'] - yearly['Plan Yearly COGS']
                    yearly['ProfitPercentPlan'] = yearly.apply(lambda x: (x['Profit']/(x['Plan Profit'] * x['Annualize'])) - 1 if x['Year'] == "2023" else np.nan, axis=1)
                    yearly['Plan Margin'] = yearly['Year'].apply(lambda x: float(self.year_margin) if x == "2023" else np.nan)
                    yearly['MARGINPercentPlan'] = (yearly['Margin'] - yearly['Plan Margin'])
                    yearly.fillna('', inplace=True)

                    yearly = yearly[[
                                'Annualize','Year', 'Dept', 'Revenue', 'Plan Yearly Sales', 'RevPercentPlan', 'LY REVENUE', 'REVENUE_GROWTH',
                                'Cost', 'Plan Yearly COGS', 'COGSPercentPlan', 'LY COST', 'COST_GROWTH',
                                'Margin', 'Plan Margin', 'MARGINPercentPlan', 'LY MARGIN', 'YoY Diff',
                                'Profit', 'Plan Profit', 'ProfitPercentPlan', 'LY PROFIT', 'PROFIT_GROWTH',
                                'Volume', 'LY VOLUME', 'VOLUME_GROWTH'
                            ]]

                    if self.Name in labor.report_look_ups:
                        labor = labor.report(self.Name).groupby('Dept').agg({"Worked Wages" : sum, "Worked Hours" : sum})
                        labor['Year'] = '2023'
                        print("LABOR", labor)
                        yearly = yearly.merge(labor, how='left', left_on='Year', right_on='Year')
                        yearly['TotalGPVariance'] = ""
                        yearly['VarDuetoMargin'] = ""
                        yearly['VarDueToSalesVolume'] = ""
                        yearly['LaborCost'] = yearly['Worked Wages']
                        yearly['PlanLaborCost'] = ""
                        yearly['PercentPlanLaborCost'] = ""
                        yearly['LYLaborCost'] = ""
                        yearly['LaborCostGrowth'] = ""
                        yearly['LaborHours'] = yearly['Worked Hours']
                        yearly['PlanLaborHours'] = ""
                        yearly['PercentPlanLaborHours'] = ""
                        yearly['LYLaborHours'] = ""
                        yearly['LaborHoursGrowth'] = ""
                        yearly['SalesToLaborHours'] = yearly['LaborCost'] / yearly['LaborHours']
                        yearly['PlanSalesToLaborHours'] = ""
                        yearly['PercentPlanSalesToLaborHours'] = ""
                        yearly['LYSalesToLaborHours'] = ""
                        yearly['SalesToLaborHoursGrowth'] = ""
                        yearly['MarginAfterLabor'] = ((yearly['Revenue']-yearly['LaborCost']) - yearly['Cost'])/yearly['Revenue']
                        yearly['PlanMarginAfterLabor'] = ""
                        yearly['PercentPlanMarginAfterLabor'] = ""
                        yearly['LYMarginAfterLabor'] = ""
                        yearly['MarginAfterLaborGrowth'] = ""
                        yearly.fillna("", inplace=True)
                    else:
                        
                        yearly['Worked Wages'] = ""
                        yearly['Worked Hours'] = ""

                        yearly['TotalGPVariance'] = ""
                        yearly['VarDuetoMargin'] = ""
                        yearly['VarDueToSalesVolume'] = ""
                        yearly['LaborCost'] = ""
                        yearly['PlanLaborCost'] = ""
                        yearly['PercentPlanLaborCost'] = ""
                        yearly['LYLaborCost'] = ""
                        yearly['LaborCostGrowth'] = ""
                        yearly['LaborHours'] = ""
                        yearly['PlanLaborHours'] = ""
                        yearly['PercentPlanLaborHours'] = ""
                        yearly['LYLaborHours'] = ""
                        yearly['LaborHoursGrowth'] = ""
                        yearly['SalesToLaborHours'] = ""
                        yearly['PlanSalesToLaborHours'] = ""
                        yearly['PercentPlanSalesToLaborHours'] = ""
                        yearly['LYSalesToLaborHours'] = ""
                        yearly['SalesToLaborHoursGrowth'] = ""
                        yearly['MarginAfterLabor'] = ""
                        yearly['PlanMarginAfterLabor'] = ""
                        yearly['PercentPlanMarginAfterLabor'] = ""
                        yearly['LYMarginAfterLabor'] = ""
                        yearly['MarginAfterLaborGrowth'] = ""
                    #    yearly['Week','Worked Wages','Worked Hours','TotalGPVariance','VarDuetoMargin','VarDueToSalesVolume','LaborCost','PlanLaborCost','PercentPlanLaborCost',
                    #    'LYLaborCost','LaborCostGrowth','LaborHours','PlanLaborHours','PercentPlanLaborHours','LYLaborHours','LaborHoursGrowth','SalesToLaborHours',
                    #    'PlanSalesToLaborHours','PercentPlanSalesToLaborHours','LYSalesToLaborHours','SalesToLaborHoursGrowth','MarginAfterLabor','PlanMarginAfterLabor',
                    #    'PercentPlanMarginAfterLabor','LYMarginAfterLabor','MarginAfterLaborGrowth'] = ""

                    yearly = yearly[[
                                'Annualize','Year', 'Dept', 'Revenue', 'Plan Yearly Sales', 'RevPercentPlan', 'LY REVENUE', 'REVENUE_GROWTH',
                                'Cost', 'Plan Yearly COGS', 'COGSPercentPlan', 'LY COST', 'COST_GROWTH',
                                'Margin', 'Plan Margin', 'MARGINPercentPlan', 'LY MARGIN', 'YoY Diff',
                                'Profit', 'Plan Profit', 'ProfitPercentPlan', 'LY PROFIT', 'PROFIT_GROWTH',
                                'Volume', 'LY VOLUME', 'VOLUME_GROWTH',
                                'TotalGPVariance',
                                'VarDuetoMargin',
                                'VarDueToSalesVolume',
                                'LaborCost',
                                'PlanLaborCost',
                                'PercentPlanLaborCost',
                                'LYLaborCost',
                                'LaborCostGrowth',
                                'LaborHours',
                                'PlanLaborHours',
                                'PercentPlanLaborHours',
                                'LYLaborHours',
                                'LaborHoursGrowth',
                                'SalesToLaborHours',
                                'PlanSalesToLaborHours',
                                'PercentPlanSalesToLaborHours',
                                'LYSalesToLaborHours',
                                'SalesToLaborHoursGrowth',
                                'MarginAfterLabor',
                                'PlanMarginAfterLabor',
                                'PercentPlanMarginAfterLabor',
                                'LYMarginAfterLabor',
                                'MarginAfterLaborGrowth'
                            ]]
                    #print(yearly)
                    #print(yearly.columns.tolist())
                    self.yearly_totals = yearly


class ReportGroup:
    def __init__(self, name, dept_reports):
        self.Name = name
        self.report_codes = [x.report_code for x in dept_reports]
        self.reports = dept_reports
        self.departments = [x for x in dept_reports]
        self.year_sales = sum(float(x.year_sales) for x in dept_reports)
        self.year_COGS = sum(float(x.year_COGS) for x in dept_reports)
        self.year_margin = (self.year_sales - self.year_COGS) / self.year_sales
        self.quarterly_sales = [sum([float(m) for m in list(s)]) for s in zip(*[x.quarterly_sales for x in dept_reports])]
        self.quarterly_COGS = [sum([float(m) for m in list(s)]) for s in zip(*[x.quarterly_COGS for x in dept_reports])]
        self.quarterly_margin = [((s-c)/s) for s, c in zip(self.quarterly_sales, self.quarterly_COGS)]
        self.weekly_sales = [sum([float(m) for m in list(s)]) for s in zip(*[x.weekly_sales for x in dept_reports])]
        self.weekly_COGS = [sum([float(m) for m in list(s)]) for s in zip(*[x.weekly_COGS for x in dept_reports])]
        self.daily_sales = [sum([float(m) for m in list(s)]) for s in zip(*[x.daily_sales for x in dept_reports])]
        self.daily_COGS = [sum([float(m) for m in list(s)]) for s in zip(*[x.daily_COGS for x in dept_reports])]
        self.monthly_sales = [sum([float(m) for m in list(s)]) for s in zip(*[x.monthly_sales for x in dept_reports])]
        self.monthly_COGS = [sum([float(m) for m in list(s)]) for s in zip(*[x.monthly_COGS for x in dept_reports])]
        self.daily_totals = (pd.concat([x.daily_totals for x in dept_reports], axis=0, ignore_index=True)).sort_values(['Day', 'Dept'], ascending=[False, True])
        self.weekly_totals = (pd.concat([x.weekly_totals for x in dept_reports], axis=0, ignore_index=True)).sort_values(['Day', 'Dept'], ascending=[False, True])
        self.monthly_totals = (pd.concat([x.monthly_totals for x in dept_reports], axis=0, ignore_index=True)).sort_values(['Day', 'Dept'], ascending=[False, True])
        self.quarterly_totals = (pd.concat([x.quarterly_totals for x in dept_reports], axis=0, ignore_index=True)).sort_values(['Quarter', 'Dept'], ascending=[False, True])
        self.yearly_totals =(pd.concat([x.yearly_totals for x in dept_reports], axis=0, ignore_index=True)).sort_values(['Year', 'Dept'], ascending=[False, True])
        self.workbook_id = ""
        self.daily_sh_id = ""
        self.weekly_sh_id = ""
        self.monthly_sh_id = ""
        self.yearly_sh_id = ""
        self.quarters = [
            pd.Period('2023Q1'),pd.Period('2023Q2'),pd.Period('2023Q3'),pd.Period('2023Q4'),
            pd.Period('2024Q1'),pd.Period('2024Q2'),pd.Period('2024Q3'),pd.Period('2024Q4'),
            pd.Period('2025Q1'),pd.Period('2025Q2'),pd.Period('2025Q3'),pd.Period('2025Q4'),
            pd.Period('2026Q1'),pd.Period('2026Q2'),pd.Period('2026Q3'),pd.Period('2026Q4')
        ]
        self.months = [
            pd.Period('2023-01'),pd.Period('2023-02'),pd.Period('2023-03'),pd.Period('2023-04'),pd.Period('2023-05'),pd.Period('2023-06'),pd.Period('2023-07'),pd.Period('2023-08'),pd.Period('2023-09'),pd.Period('2023-10'),pd.Period('2023-11'),pd.Period('2023-12'),
            pd.Period('2024-01'),pd.Period('2024-02'),pd.Period('2024-03'),pd.Period('2024-04'),pd.Period('2024-05'),pd.Period('2024-06'),pd.Period('2024-07'),pd.Period('2024-08'),pd.Period('2024-09'),pd.Period('2024-10'),pd.Period('2024-11'),pd.Period('2024-12'),
            pd.Period('2025-01'),pd.Period('2025-02'),pd.Period('2025-03'),pd.Period('2025-04'),pd.Period('2025-05'),pd.Period('2025-06'),pd.Period('2025-07'),pd.Period('2025-08'),pd.Period('2025-09'),pd.Period('2025-10'),pd.Period('2025-11'),pd.Period('2025-12'),
            pd.Period('2026-01'),pd.Period('2026-02'),pd.Period('2026-03'),pd.Period('2026-04'),pd.Period('2026-05'),pd.Period('2026-06'),pd.Period('2026-07'),pd.Period('2026-08'),pd.Period('2026-09'),pd.Period('2026-10'),pd.Period('2026-11'),pd.Period('2026-12')
        ]
    
    def recalc_totals(self, period):
        starting_quarter = pd.Period('2023Q1')
        starting_month = pd.Period('2023-01')
        actual_date = dt.datetime.today()
        match period:
            case "Daily":
                df = self.daily_totals.replace('', np.nan, regex=True)
                df = df[df['Dept'] != 'Store Wide']
                df['Day'] = pd.to_datetime(df['Day'])
                df = df.astype({'Revenue' : float, 'Plan Daily Sales' : float, 'RevPercentPlan' : float, 'LW Revenue' : float, 'Ninety_Trailing_Revenue' : float, 'LY REVENUE' : float, 'REVENUE_GROWTH' : float,
                                'Cost' : float, 'Plan Daily COGS' : float, 'COGSPercentPlan' : float, 'LW Cost' : float, 'Ninety_Trailing_Cost' : float, 'LY COST' : float, 'COST_GROWTH' : float,
                                'Margin' : float, 'Plan Margin' : float, 'MARGINPercentPlan' : float, 'LW Margin' : float, 'Ninety_Trailing_Margin' : float, 'LY MARGIN' : float, 'YoY Diff' : float,
                                'Profit' : float, 'Plan Profit' : float, 'ProfitPercentPlan' : float, 'LW Profit' : float, 'Ninety_Trailing_Profit' : float, 'LY PROFIT' : float, 'PROFIT_GROWTH' : float,
                                'Volume' : float, 'LW Volume' : float, 'Ninety_Trailing_Volume' : float, 'LY VOLUME' : float, 'VOLUME_GROWTH': float})
                df = df.groupby("Day").agg({
                    'Revenue' : sum, 'Plan Daily Sales': sum, 'LW Revenue' : sum, 'Ninety_Trailing_Revenue' : sum, 'LY REVENUE' : sum,
                    'Cost' : sum, 'Plan Daily COGS' : sum, 'LW Cost' : sum, 'Ninety_Trailing_Cost' : sum, 'LY COST' : sum,
                    'Profit' : sum, 'Plan Profit' : sum, 'LW Profit' : sum, 'Ninety_Trailing_Profit' : sum, 'LY PROFIT' : sum,
                    'Volume' : sum, 'LW Volume' : sum, 'Ninety_Trailing_Volume' : sum, 'LY VOLUME' : sum}
                )
                
                df['Dept'] = "Total"
                df = df.reset_index()
                df['RevPercentPlan'] = df.apply(lambda x: ((x['Revenue']/x['Plan Daily Sales']) - 1) if x['Plan Daily Sales'] > 0.0 else np.nan, axis=1)
                df['COGSPercentPlan'] = df.apply(lambda x: (x['Cost']/x['Plan Daily COGS']) - 1 if x['Plan Daily COGS'] > 0.0 else np.nan, axis=1)
                df['ProfitPercentPlan'] = df.apply(lambda x: (x['Profit']/x['Plan Profit']) - 1 if x['Plan Profit'] > 0.0 else np.nan, axis=1)
                df['Margin'] = df.apply(lambda x: x['Profit']/x['Revenue'], axis=1)
                df['Plan Margin'] = df.apply(lambda x: x['Plan Profit'] / x['Plan Daily Sales'] if x['Plan Daily Sales'] > 0.0 else np.nan, axis=1)
                df['MARGINPercentPlan'] = df.apply(lambda x: x['Plan Margin'] - x['Margin'] if x['Plan Daily Sales'] > 0.0 else np.nan, axis=1)
                df['Ninety_Trailing_Margin'] = df.apply(lambda x: x['Ninety_Trailing_Profit'] / x['Ninety_Trailing_Revenue'] if x['Ninety_Trailing_Revenue'] > 0.0 else np.nan, axis=1)
                df['REVENUE_GROWTH'] = df.apply(lambda x: ((x['Revenue']/x['LY REVENUE']) - 1) if x['LY REVENUE'] > 0.0 else np.nan, axis=1)
                df['COST_GROWTH'] = df.apply(lambda x: ((x['Cost']/x['LY COST']) - 1) if x['LY COST'] > 0.0 else np.nan, axis=1)
                df['LY MARGIN'] = df.apply(lambda x: (x['LY PROFIT'] / x['LY REVENUE']) if x['LY REVENUE'] > 0.0 else np.nan, axis=1)
                df['LW Margin'] = df.apply(lambda x: (x['LW Profit'] / x['LW Revenue']) if x['LW Revenue'] > 0.0 else np.nan, axis=1)
                df['YoY Diff'] = df.apply(lambda x: (x['LY MARGIN'] - x['Margin']) if x['LY MARGIN'] > 0.0 else np.nan, axis=1)
                df['PROFIT_GROWTH'] = df.apply(lambda x: ((x['Profit']/x['LY PROFIT']) - 1) if x['LY PROFIT'] > 0.0 else np.nan, axis=1)
                df['VOLUME_GROWTH'] = df.apply(lambda x: ((x['Volume']/x['LY VOLUME']) - 1) if x['LY VOLUME'] > 0.0 else np.nan, axis=1)
                df = df[[
                                'Day', 'Dept', 'Revenue', 'Plan Daily Sales', 'RevPercentPlan', 'LW Revenue', 'Ninety_Trailing_Revenue', 'LY REVENUE', 'REVENUE_GROWTH',
                                'Cost', 'Plan Daily COGS', 'COGSPercentPlan', 'LW Cost', 'Ninety_Trailing_Cost', 'LY COST', 'COST_GROWTH',
                                'Margin', 'Plan Margin', 'MARGINPercentPlan', 'LW Margin', 'Ninety_Trailing_Margin', 'LY MARGIN', 'YoY Diff',
                                'Profit', 'Plan Profit', 'ProfitPercentPlan', 'LW Profit', 'Ninety_Trailing_Profit', 'LY PROFIT', 'PROFIT_GROWTH',
                                'Volume', 'LW Volume', 'Ninety_Trailing_Volume', 'LY VOLUME', 'VOLUME_GROWTH'
                            ]]
                df = df.astype({
                                'Day' : str, 'Dept' : str, 'Revenue' : str, 'Plan Daily Sales' : str, 'RevPercentPlan' : str, 'LW Revenue' : str, 'Ninety_Trailing_Revenue' : str, 'LY REVENUE' : str, 'REVENUE_GROWTH' : str,
                                'Cost' : str, 'Plan Daily COGS' : str, 'COGSPercentPlan' : str, 'LW Cost' : str, 'Ninety_Trailing_Cost' : str, 'LY COST' : str, 'COST_GROWTH' : str,
                                'Margin' : str, 'Plan Margin' : str, 'MARGINPercentPlan' : str, 'LW Margin' : str, 'Ninety_Trailing_Margin' : str, 'LY MARGIN' : str, 'YoY Diff' : str,
                                'Profit' : str, 'Plan Profit' : str, 'ProfitPercentPlan' : str, 'LW Profit' : str, 'Ninety_Trailing_Profit' : str, 'LY PROFIT' : str, 'PROFIT_GROWTH' : str,
                                'Volume' : str, 'LW Volume' : str, 'Ninety_Trailing_Volume' : str, 'LY VOLUME' : str, 'VOLUME_GROWTH' : str
                })
                df = df.replace(['0', '0.0', 'nan', "NaN"], '')
                self.daily_totals = pd.concat([self.daily_totals, df], ignore_index=True, axis=0)
                self.daily_totals['Dept'] = pd.Categorical(self.daily_totals['Dept'], categories=dept_order, ordered=True)
                self.daily_totals.sort_values(['Day', 'Dept'], ascending=[False, True], inplace=True, axis=0)
                
                #self.daily_totals.to_csv("agged_day.csv")
            case "Weekly":
                df = self.weekly_totals.replace('', np.nan, regex=True)
                df = df[df['Dept'] != 'Store Wide']
                df['Day'] = pd.to_datetime(df['Day'])
                df = df.astype({'Annualize' : float, 'Revenue' : float, 'Plan Weekly Sales' : float, 'RevPercentPlan' : float, 'Ninety_Trailing_Revenue' : float, 'LY REVENUE' : float, 'REVENUE_GROWTH' : float,
                                'Cost' : float, 'Plan Weekly COGS' : float, 'COGSPercentPlan' : float, 'Ninety_Trailing_Cost' : float, 'LY COST' : float, 'COST_GROWTH' : float,
                                'Margin' : float, 'Plan Margin' : float, 'MARGINPercentPlan' : float, 'Ninety_Trailing_Margin' : float, 'LY MARGIN' : float, 'YoY Diff' : float,
                                'Profit' : float, 'Plan Profit' : float, 'ProfitPercentPlan' : float, 'Ninety_Trailing_Profit' : float, 'LY PROFIT' : float, 'PROFIT_GROWTH' : float,
                                'Volume' : float, 'Ninety_Trailing_Volume' : float, 'LY VOLUME' : float, 'VOLUME_GROWTH': float,
                                'TotalGPVariance' : float,
                                'VarDuetoMargin' : float,
                                'VarDueToSalesVolume' : float,
                                'LaborCost' : float,
                                'PlanLaborCost' : float,
                                'PercentPlanLaborCost' : float,
                                'LYLaborCost' : float,
                                'LaborCostGrowth' : float,
                                'LaborHours' : float,
                                'PlanLaborHours' : float,
                                'PercentPlanLaborHours' : float,
                                'LYLaborHours' : float,
                                'LaborHoursGrowth' : float,
                                'SalesToLaborHours' : float,
                                'PlanSalesToLaborHours' : float,
                                'PercentPlanSalesToLaborHours' : float,
                                'LYSalesToLaborHours' : float,
                                'SalesToLaborHoursGrowth' : float,
                                'MarginAfterLabor' : float,
                                'PlanMarginAfterLabor' : float,
                                'PercentPlanMarginAfterLabor' : float,
                                'LYMarginAfterLabor' : float,
                                'MarginAfterLaborGrowth' : float
                                })
                df = df.groupby("Day").agg({
                    'Annualize' : 'first', 'Revenue' : sum, 'Plan Weekly Sales': sum, 'Ninety_Trailing_Revenue' : sum, 'LY REVENUE' : sum,
                    'Cost' : sum, 'Plan Weekly COGS' : sum, 'Ninety_Trailing_Cost' : sum, 'LY COST' : sum,
                    'Profit' : sum, 'Plan Profit' : sum, 'Ninety_Trailing_Profit' : sum, 'LY PROFIT' : sum,
                    'Volume' : sum, 'Ninety_Trailing_Volume' : sum, 'LY VOLUME' : sum,
                    'TotalGPVariance' : sum,
                    'VarDuetoMargin' : sum,
                    'VarDueToSalesVolume' : sum,
                    'LaborCost' : sum,
                    'PlanLaborCost' : sum,
                    'PercentPlanLaborCost' : sum,
                    'LYLaborCost' : sum,
                    'LaborCostGrowth' : sum,
                    'LaborHours' : sum,
                    'PlanLaborHours' : sum,
                    'PercentPlanLaborHours' : sum,
                    'LYLaborHours' : sum,
                    'LaborHoursGrowth' : sum,
                    'SalesToLaborHours' : sum,
                    'PlanSalesToLaborHours' : sum,
                    'PercentPlanSalesToLaborHours' : sum,
                    'LYSalesToLaborHours' : sum,
                    'SalesToLaborHoursGrowth' : sum,
                    'MarginAfterLabor' : sum,
                    'PlanMarginAfterLabor' : sum,
                    'PercentPlanMarginAfterLabor' : sum,
                    'LYMarginAfterLabor' : sum,
                    'MarginAfterLaborGrowth' : sum
                    }
                )
                
                df['Dept'] = "Total"
                df = df.reset_index()

                df['SalesToLaborHours'] = df.apply(lambda x: x['Revenue'] / x['LaborHours'] if x['LaborCost'] > 0 else np.nan, axis=1)
                df['MarginAfterLabor'] = df.apply(lambda x: (x['Profit'] - x['LaborCost']) / x['Revenue'] if x['LaborCost'] > 0 else np.nan, axis=1)


                df['RevPercentPlan'] = df.apply(lambda x: ((x['Revenue']/x['Plan Weekly Sales']) - 1) if x['Plan Weekly Sales'] > 0.0 else np.nan, axis=1)
                df['COGSPercentPlan'] = df.apply(lambda x: (x['Cost']/x['Plan Weekly COGS']) - 1 if x['Plan Weekly COGS'] > 0.0 else np.nan, axis=1)
                df['ProfitPercentPlan'] = df.apply(lambda x: (x['Profit']/x['Plan Profit']) - 1 if x['Plan Profit'] > 0.0 else np.nan, axis=1)
                df['Margin'] = df.apply(lambda x: x['Profit']/x['Revenue'], axis=1)
                df['Plan Margin'] = df.apply(lambda x: x['Plan Profit'] / x['Plan Weekly Sales'] if x['Plan Weekly Sales'] > 0.0 else np.nan, axis=1)
                df['MARGINPercentPlan'] = df.apply(lambda x: x['Plan Margin'] - x['Margin'] if x['Plan Weekly Sales'] > 0.0 else np.nan, axis=1)
                df['Ninety_Trailing_Margin'] = df.apply(lambda x: x['Ninety_Trailing_Profit'] / x['Ninety_Trailing_Revenue'] if x['Ninety_Trailing_Revenue'] > 0.0 else np.nan, axis=1)
                df['REVENUE_GROWTH'] = df.apply(lambda x: ((x['Revenue']/x['LY REVENUE']) - 1) if x['LY REVENUE'] > 0.0 else np.nan, axis=1)
                df['COST_GROWTH'] = df.apply(lambda x: ((x['Cost']/x['LY COST']) - 1) if x['LY COST'] > 0.0 else np.nan, axis=1)
                df['LY MARGIN'] = df.apply(lambda x: (x['LY PROFIT'] / x['LY REVENUE']) if x['LY REVENUE'] > 0.0 else np.nan, axis=1)
                df['YoY Diff'] = df.apply(lambda x: (x['LY MARGIN'] - x['Margin']) if x['LY MARGIN'] > 0.0 else np.nan, axis=1)
                df['PROFIT_GROWTH'] = df.apply(lambda x: ((x['Profit']/x['LY PROFIT']) - 1) if x['LY PROFIT'] > 0.0 else np.nan, axis=1)
                df['VOLUME_GROWTH'] = df.apply(lambda x: ((x['Volume']/x['LY VOLUME']) - 1) if x['LY VOLUME'] > 0.0 else np.nan, axis=1)
                #Annualizations
                df['RevPercentPlan'] = df.apply(lambda x: (x['Revenue']/(x['Plan Weekly Sales'] * x['Annualize'])) - 1 if pd.to_datetime(x['Day']) > actual_date else x['RevPercentPlan'], axis=1)
                df['COGSPercentPlan'] = df.apply(lambda x: (x['Cost']/(x['Plan Weekly COGS'] * x['Annualize'])) - 1 if pd.to_datetime(x['Day']) > actual_date else x['COGSPercentPlan'], axis=1)
                df['ProfitPercentPlan'] = df.apply(lambda x: (x['Profit']/(x['Plan Profit'] * x['Annualize'])) - 1 if pd.to_datetime(x['Day']) > actual_date else x['ProfitPercentPlan'], axis=1)
                df['REVENUE_GROWTH'] = df.apply(lambda x: (x['Revenue']/(x['LY REVENUE'] * x['Annualize'])) - 1 if pd.to_datetime(x['Day']) > actual_date else x['REVENUE_GROWTH'], axis=1)
                df['COST_GROWTH'] = df.apply(lambda x: (x['Cost']/(x['LY COST'] * x['Annualize'])) - 1 if pd.to_datetime(x['Day']) > actual_date else x['COST_GROWTH'], axis=1)
                df['PROFIT_GROWTH'] = df.apply(lambda x: (x['Profit']/(x['LY PROFIT'] * x['Annualize'])) - 1 if pd.to_datetime(x['Day']) > actual_date else x['PROFIT_GROWTH'], axis=1)
                df['VOLUME_GROWTH'] = df.apply(lambda x: (x['Volume']/(x['LY VOLUME'] * x['Annualize'])) - 1 if pd.to_datetime(x['Day']) > actual_date else x['VOLUME_GROWTH'], axis=1)
                df['Ninety_Trailing_Revenue'] = df.apply(lambda x: (x['Ninety_Trailing_Revenue'] * x['Annualize']) if pd.to_datetime(x['Day']) > actual_date else x['Ninety_Trailing_Revenue'], axis=1)
                df['Ninety_Trailing_Cost'] = df.apply(lambda x: (x['Ninety_Trailing_Cost'] * x['Annualize']) if pd.to_datetime(x['Day']) > actual_date else x['Ninety_Trailing_Cost'], axis=1)
                df['Ninety_Trailing_Profit'] = df.apply(lambda x: (x['Ninety_Trailing_Profit'] * x['Annualize']) if pd.to_datetime(x['Day']) > actual_date else x['Ninety_Trailing_Profit'], axis=1)
                df['Ninety_Trailing_Volume'] = df.apply(lambda x: (x['Ninety_Trailing_Volume'] * x['Annualize']) if pd.to_datetime(x['Day']) > actual_date else x['Ninety_Trailing_Volume'], axis=1)
                
                
                df = df[[
                                'Day', 'Dept', 'Revenue', 'Plan Weekly Sales', 'RevPercentPlan', 'Ninety_Trailing_Revenue', 'LY REVENUE', 'REVENUE_GROWTH',
                                'Cost', 'Plan Weekly COGS', 'COGSPercentPlan', 'Ninety_Trailing_Cost', 'LY COST', 'COST_GROWTH',
                                'Margin', 'Plan Margin', 'MARGINPercentPlan', 'Ninety_Trailing_Margin', 'LY MARGIN', 'YoY Diff',
                                'Profit', 'Plan Profit', 'ProfitPercentPlan', 'Ninety_Trailing_Profit', 'LY PROFIT', 'PROFIT_GROWTH',
                                'Volume', 'Ninety_Trailing_Volume', 'LY VOLUME', 'VOLUME_GROWTH',
                                'TotalGPVariance',
                                'VarDuetoMargin',
                                'VarDueToSalesVolume',
                                'LaborCost',
                                'PlanLaborCost',
                                'PercentPlanLaborCost',
                                'LYLaborCost',
                                'LaborCostGrowth',
                                'LaborHours',
                                'PlanLaborHours',
                                'PercentPlanLaborHours',
                                'LYLaborHours',
                                'LaborHoursGrowth',
                                'SalesToLaborHours',
                                'PlanSalesToLaborHours',
                                'PercentPlanSalesToLaborHours',
                                'LYSalesToLaborHours',
                                'SalesToLaborHoursGrowth',
                                'MarginAfterLabor',
                                'PlanMarginAfterLabor',
                                'PercentPlanMarginAfterLabor',
                                'LYMarginAfterLabor',
                                'MarginAfterLaborGrowth'
                            ]]
                self.weekly_totals = self.weekly_totals[[
                                'Day', 'Dept', 'Revenue', 'Plan Weekly Sales', 'RevPercentPlan', 'Ninety_Trailing_Revenue', 'LY REVENUE', 'REVENUE_GROWTH',
                                'Cost', 'Plan Weekly COGS', 'COGSPercentPlan', 'Ninety_Trailing_Cost', 'LY COST', 'COST_GROWTH',
                                'Margin', 'Plan Margin', 'MARGINPercentPlan', 'Ninety_Trailing_Margin', 'LY MARGIN', 'YoY Diff',
                                'Profit', 'Plan Profit', 'ProfitPercentPlan', 'Ninety_Trailing_Profit', 'LY PROFIT', 'PROFIT_GROWTH',
                                'Volume', 'Ninety_Trailing_Volume', 'LY VOLUME', 'VOLUME_GROWTH',
                                'TotalGPVariance','VarDuetoMargin','VarDueToSalesVolume',
                                'LaborCost','PlanLaborCost','PercentPlanLaborCost','LYLaborCost','LaborCostGrowth',
                                'LaborHours','PlanLaborHours','PercentPlanLaborHours','LYLaborHours','LaborHoursGrowth',
                                'SalesToLaborHours',
                                'PlanSalesToLaborHours',
                                'PercentPlanSalesToLaborHours',
                                'LYSalesToLaborHours',
                                'SalesToLaborHoursGrowth',
                                'MarginAfterLabor',
                                'PlanMarginAfterLabor',
                                'PercentPlanMarginAfterLabor',
                                'LYMarginAfterLabor',
                                'MarginAfterLaborGrowth'
                            ]]
                df = df.astype({
                                'Day' : str, 'Dept' : str, 'Revenue' : str, 'Plan Weekly Sales' : str, 'RevPercentPlan' : str, 'Ninety_Trailing_Revenue' : str, 'LY REVENUE' : str, 'REVENUE_GROWTH' : str,
                                'Cost' : str, 'Plan Weekly COGS' : str, 'COGSPercentPlan' : str, 'Ninety_Trailing_Cost' : str, 'LY COST' : str, 'COST_GROWTH' : str,
                                'Margin' : str, 'Plan Margin' : str, 'MARGINPercentPlan' : str, 'Ninety_Trailing_Margin' : str, 'LY MARGIN' : str, 'YoY Diff' : str,
                                'Profit' : str, 'Plan Profit' : str, 'ProfitPercentPlan' : str, 'Ninety_Trailing_Profit' : str, 'LY PROFIT' : str, 'PROFIT_GROWTH' : str,
                                'Volume' : str, 'Ninety_Trailing_Volume' : str, 'LY VOLUME' : str, 'VOLUME_GROWTH' : str,
                                'TotalGPVariance' : str,
                                'VarDuetoMargin' : str,
                                'VarDueToSalesVolume' : str,
                                'LaborCost' : str,
                                'PlanLaborCost' : str,
                                'PercentPlanLaborCost' : str,
                                'LYLaborCost' : str,
                                'LaborCostGrowth' : str,
                                'LaborHours' : str,
                                'PlanLaborHours' : str,
                                'PercentPlanLaborHours' : str,
                                'LYLaborHours' : str,
                                'LaborHoursGrowth' : str,
                                'SalesToLaborHours' : str,
                                'PlanSalesToLaborHours' : str,
                                'PercentPlanSalesToLaborHours' : str,
                                'LYSalesToLaborHours' : str,
                                'SalesToLaborHoursGrowth' : str,
                                'MarginAfterLabor' : str,
                                'PlanMarginAfterLabor' : str,
                                'PercentPlanMarginAfterLabor' : str,
                                'LYMarginAfterLabor' : str,
                                'MarginAfterLaborGrowth' : str
                })
                df = df.replace(['0', '0.0', 'nan', "NaN"], '')
                self.weekly_totals = pd.concat([self.weekly_totals, df], ignore_index=True, axis=0)
                self.weekly_totals['Dept'] = pd.Categorical(self.weekly_totals['Dept'], categories=dept_order, ordered=True)
                self.weekly_totals.sort_values(['Day', 'Dept'], ascending=[False, True], inplace=True, axis=0)
                
                #self.weekly_totals.to_csv("agged_week.csv")
            case "Monthly":
                df = self.monthly_totals.replace('', np.nan, regex=True)
                df = df[df['Dept'] != 'Store Wide']
                df['Day'] = pd.to_datetime(df['Day'])
                df = df.astype({'Annualize' : float, 'Revenue' : float, 'Plan Monthly Sales' : float, 'RevPercentPlan' : float, 'Ninety_Trailing_Revenue' : float, 'LY REVENUE' : float, 'REVENUE_GROWTH' : float,
                                'Cost' : float, 'Plan Monthly COGS' : float, 'COGSPercentPlan' : float, 'Ninety_Trailing_Cost' : float, 'LY COST' : float, 'COST_GROWTH' : float,
                                'Margin' : float, 'Plan Margin' : float, 'MARGINPercentPlan' : float, 'Ninety_Trailing_Margin' : float, 'LY MARGIN' : float, 'YoY Diff' : float,
                                'Profit' : float, 'Plan Profit' : float, 'ProfitPercentPlan' : float, 'Ninety_Trailing_Profit' : float, 'LY PROFIT' : float, 'PROFIT_GROWTH' : float,
                                'Volume' : float, 'Ninety_Trailing_Volume' : float, 'LY VOLUME' : float, 'VOLUME_GROWTH': float})
                df = df.groupby("Day").agg({
                    'Annualize' : 'first', 'Revenue' : sum, 'Plan Monthly Sales': sum, 'Ninety_Trailing_Revenue' : sum, 'LY REVENUE' : sum,
                    'Cost' : sum, 'Plan Monthly COGS' : sum, 'Ninety_Trailing_Cost' : sum, 'LY COST' : sum,
                    'Profit' : sum, 'Plan Profit' : sum, 'Ninety_Trailing_Profit' : sum, 'LY PROFIT' : sum,
                    'Volume' : sum, 'Ninety_Trailing_Volume' : sum, 'LY VOLUME' : sum}
                )
                
                df['Dept'] = "Total"
                df = df.reset_index()
                df['RevPercentPlan'] = df.apply(lambda x: ((x['Revenue']/x['Plan Monthly Sales']) - 1) if x['Plan Monthly Sales'] > 0.0 else np.nan, axis=1)
                df['COGSPercentPlan'] = df.apply(lambda x: (x['Cost']/x['Plan Monthly COGS']) - 1 if x['Plan Monthly COGS'] > 0.0 else np.nan, axis=1)
                df['ProfitPercentPlan'] = df.apply(lambda x: (x['Profit']/x['Plan Profit']) - 1 if x['Plan Profit'] > 0.0 else np.nan, axis=1)
                df['Margin'] = df.apply(lambda x: x['Profit']/x['Revenue'], axis=1)
                df['Plan Margin'] = df.apply(lambda x: x['Plan Profit'] / x['Plan Monthly Sales'] if x['Plan Monthly Sales'] > 0.0 else np.nan, axis=1)
                df['MARGINPercentPlan'] = df.apply(lambda x: x['Plan Margin'] - x['Margin'] if x['Plan Monthly Sales'] > 0.0 else np.nan, axis=1)
                df['Ninety_Trailing_Margin'] = df.apply(lambda x: x['Ninety_Trailing_Profit'] / x['Ninety_Trailing_Revenue'] if x['Ninety_Trailing_Revenue'] > 0.0 else np.nan, axis=1)
                df['REVENUE_GROWTH'] = df.apply(lambda x: ((x['Revenue']/x['LY REVENUE']) - 1) if x['LY REVENUE'] > 0.0 else np.nan, axis=1)
                df['COST_GROWTH'] = df.apply(lambda x: ((x['Cost']/x['LY COST']) - 1) if x['LY COST'] > 0.0 else np.nan, axis=1)
                df['LY MARGIN'] = df.apply(lambda x: (x['LY PROFIT'] / x['LY REVENUE']) if x['LY REVENUE'] > 0.0 else np.nan, axis=1)
                df['YoY Diff'] = df.apply(lambda x: (x['LY MARGIN'] - x['Margin']) if x['LY MARGIN'] > 0.0 else np.nan, axis=1)
                df['PROFIT_GROWTH'] = df.apply(lambda x: ((x['Profit']/x['LY PROFIT']) - 1) if x['LY PROFIT'] > 0.0 else np.nan, axis=1)
                df['VOLUME_GROWTH'] = df.apply(lambda x: ((x['Volume']/x['LY VOLUME']) - 1) if x['LY VOLUME'] > 0.0 else np.nan, axis=1)

                #Annualizations
                df['RevPercentPlan'] = df.apply(lambda x: (x['Revenue']/(x['Plan Monthly Sales'] * x['Annualize'])) - 1 if pd.to_datetime(x['Day']) > actual_date else x['RevPercentPlan'], axis=1)
                df['COGSPercentPlan'] = df.apply(lambda x: (x['Cost']/(x['Plan Monthly COGS'] * x['Annualize'])) - 1 if pd.to_datetime(x['Day']) > actual_date else x['COGSPercentPlan'], axis=1)
                df['ProfitPercentPlan'] = df.apply(lambda x: (x['Profit']/(x['Plan Profit'] * x['Annualize'])) - 1 if pd.to_datetime(x['Day']) > actual_date else x['ProfitPercentPlan'], axis=1)
                df['REVENUE_GROWTH'] = df.apply(lambda x: (x['Revenue']/(x['LY REVENUE'] * x['Annualize'])) - 1 if pd.to_datetime(x['Day']) > actual_date else x['REVENUE_GROWTH'], axis=1)
                df['COST_GROWTH'] = df.apply(lambda x: (x['Cost']/(x['LY COST'] * x['Annualize'])) - 1 if pd.to_datetime(x['Day']) > actual_date else x['COST_GROWTH'], axis=1)
                df['PROFIT_GROWTH'] = df.apply(lambda x: (x['Profit']/(x['LY PROFIT'] * x['Annualize'])) - 1 if pd.to_datetime(x['Day']) > actual_date else x['PROFIT_GROWTH'], axis=1)
                df['VOLUME_GROWTH'] = df.apply(lambda x: (x['Volume']/(x['LY VOLUME'] * x['Annualize'])) - 1 if pd.to_datetime(x['Day']) > actual_date else x['VOLUME_GROWTH'], axis=1)
                df['Ninety_Trailing_Revenue'] = df.apply(lambda x: (x['Ninety_Trailing_Revenue'] * x['Annualize']) if pd.to_datetime(x['Day']) > actual_date else x['Ninety_Trailing_Revenue'], axis=1)
                df['Ninety_Trailing_Cost'] = df.apply(lambda x: (x['Ninety_Trailing_Cost'] * x['Annualize']) if pd.to_datetime(x['Day']) > actual_date else x['Ninety_Trailing_Cost'], axis=1)
                df['Ninety_Trailing_Profit'] = df.apply(lambda x: (x['Ninety_Trailing_Profit'] * x['Annualize']) if pd.to_datetime(x['Day']) > actual_date else x['Ninety_Trailing_Profit'], axis=1)
                df['Ninety_Trailing_Volume'] = df.apply(lambda x: (x['Ninety_Trailing_Volume'] * x['Annualize']) if pd.to_datetime(x['Day']) > actual_date else x['Ninety_Trailing_Volume'], axis=1)


                df = df[[
                                'Day', 'Dept', 'Revenue', 'Plan Monthly Sales', 'RevPercentPlan', 'Ninety_Trailing_Revenue', 'LY REVENUE', 'REVENUE_GROWTH',
                                'Cost', 'Plan Monthly COGS', 'COGSPercentPlan', 'Ninety_Trailing_Cost', 'LY COST', 'COST_GROWTH',
                                'Margin', 'Plan Margin', 'MARGINPercentPlan', 'Ninety_Trailing_Margin', 'LY MARGIN', 'YoY Diff',
                                'Profit', 'Plan Profit', 'ProfitPercentPlan', 'Ninety_Trailing_Profit', 'LY PROFIT', 'PROFIT_GROWTH',
                                'Volume', 'Ninety_Trailing_Volume', 'LY VOLUME', 'VOLUME_GROWTH'
                            ]]
                self.monthly_totals = self.monthly_totals[[
                                'Day', 'Dept', 'Revenue', 'Plan Monthly Sales', 'RevPercentPlan', 'Ninety_Trailing_Revenue', 'LY REVENUE', 'REVENUE_GROWTH',
                                'Cost', 'Plan Monthly COGS', 'COGSPercentPlan', 'Ninety_Trailing_Cost', 'LY COST', 'COST_GROWTH',
                                'Margin', 'Plan Margin', 'MARGINPercentPlan', 'Ninety_Trailing_Margin', 'LY MARGIN', 'YoY Diff',
                                'Profit', 'Plan Profit', 'ProfitPercentPlan', 'Ninety_Trailing_Profit', 'LY PROFIT', 'PROFIT_GROWTH',
                                'Volume', 'Ninety_Trailing_Volume', 'LY VOLUME', 'VOLUME_GROWTH'
                            ]]
                df = df.astype({
                                'Day' : str, 'Dept' : str, 'Revenue' : str, 'Plan Monthly Sales' : str, 'RevPercentPlan' : str, 'Ninety_Trailing_Revenue' : str, 'LY REVENUE' : str, 'REVENUE_GROWTH' : str,
                                'Cost' : str, 'Plan Monthly COGS' : str, 'COGSPercentPlan' : str, 'Ninety_Trailing_Cost' : str, 'LY COST' : str, 'COST_GROWTH' : str,
                                'Margin' : str, 'Plan Margin' : str, 'MARGINPercentPlan' : str, 'Ninety_Trailing_Margin' : str, 'LY MARGIN' : str, 'YoY Diff' : str,
                                'Profit' : str, 'Plan Profit' : str, 'ProfitPercentPlan' : str, 'Ninety_Trailing_Profit' : str, 'LY PROFIT' : str, 'PROFIT_GROWTH' : str,
                                'Volume' : str, 'Ninety_Trailing_Volume' : str, 'LY VOLUME' : str, 'VOLUME_GROWTH' : str
                })
                df = df.replace(['0', '0.0', 'nan', "NaN"], '')
                self.monthly_totals = pd.concat([self.monthly_totals, df], ignore_index=True, axis=0)
                self.monthly_totals['Dept'] = pd.Categorical(self.monthly_totals['Dept'], categories=dept_order, ordered=True)
                self.monthly_totals.sort_values(['Day', 'Dept'], ascending=[False, True], inplace=True, axis=0)
                
                #self.monthly_totals.to_csv("agged_month.csv")
            case "Quarterly":
                df = self.quarterly_totals.replace('', np.nan, regex=True)
                df = df[df['Dept'] != 'Store Wide']
                df = df.astype({'Annualize' : float, 'Revenue' : float, 'Plan Quarterly Sales' : float, 'RevPercentPlan' : float, 'LY REVENUE' : float, 'REVENUE_GROWTH' : float,
                                'Cost' : float, 'Plan Quarterly COGS' : float, 'COGSPercentPlan' : float, 'LY COST' : float, 'COST_GROWTH' : float,
                                'Margin' : float, 'Plan Margin' : float, 'MARGINPercentPlan' : float, 'LY MARGIN' : float, 'YoY Diff' : float,
                                'Profit' : float, 'Plan Profit' : float, 'ProfitPercentPlan' : float, 'LY PROFIT' : float, 'PROFIT_GROWTH' : float,
                                'Volume' : float, 'LY VOLUME' : float, 'VOLUME_GROWTH': float,
                                'TotalGPVariance' : float,
                                'VarDuetoMargin' : float,
                                'VarDueToSalesVolume' : float,
                                'LaborCost' : float,
                                'PlanLaborCost' : float,
                                'PercentPlanLaborCost' : float,
                                'LYLaborCost' : float,
                                'LaborCostGrowth' : float,
                                'LaborHours' : float,
                                'PlanLaborHours' : float,
                                'PercentPlanLaborHours' : float,
                                'LYLaborHours' : float,
                                'LaborHoursGrowth' : float,
                                'SalesToLaborHours' : float,
                                'PlanSalesToLaborHours' : float,
                                'PercentPlanSalesToLaborHours' : float,
                                'LYSalesToLaborHours' : float,
                                'SalesToLaborHoursGrowth' : float,
                                'MarginAfterLabor' : float,
                                'PlanMarginAfterLabor' : float,
                                'PercentPlanMarginAfterLabor' : float,
                                'LYMarginAfterLabor' : float,
                                'MarginAfterLaborGrowth' : float})
                df = df.groupby("Quarter").agg({ 'End' : 'median',
                    'Annualize' : 'first', 'Revenue' : sum, 'Plan Quarterly Sales': sum, 'LY REVENUE' : sum,
                    'Cost' : sum, 'Plan Quarterly COGS' : sum, 'LY COST' : sum,
                    'Profit' : sum, 'Plan Profit' : sum, 'LY PROFIT' : sum,
                    'Volume' : sum, 'LY VOLUME' : sum,
                    'TotalGPVariance' : sum,
                    'VarDuetoMargin' : sum,
                    'VarDueToSalesVolume' : sum,
                    'LaborCost' : sum,
                    'PlanLaborCost' : sum,
                    'PercentPlanLaborCost' : sum,
                    'LYLaborCost' : sum,
                    'LaborCostGrowth' : sum,
                    'LaborHours' : sum,
                    'PlanLaborHours' : sum,
                    'PercentPlanLaborHours' : sum,
                    'LYLaborHours' : sum,
                    'LaborHoursGrowth' : sum,
                    'SalesToLaborHours' : sum,
                    'PlanSalesToLaborHours' : sum,
                    'PercentPlanSalesToLaborHours' : sum,
                    'LYSalesToLaborHours' : sum,
                    'SalesToLaborHoursGrowth' : sum,
                    'MarginAfterLabor' : sum,
                    'PlanMarginAfterLabor' : sum,
                    'PercentPlanMarginAfterLabor' : sum,
                    'LYMarginAfterLabor' : sum,
                    'MarginAfterLaborGrowth' : sum
                })
                
                df['Dept'] = "Total"
                df = df.reset_index()
                df['RevPercentPlan'] = df.apply(lambda x: ((x['Revenue']/(x['Plan Quarterly Sales']*x['Annualize'])) - 1) if x['Plan Quarterly Sales'] > 0.0 else np.nan, axis=1)
                df['COGSPercentPlan'] = df.apply(lambda x: (x['Cost']/(x['Plan Quarterly COGS']*x['Annualize'])) - 1 if x['Plan Quarterly COGS'] > 0.0 else np.nan, axis=1)
                df['ProfitPercentPlan'] = df.apply(lambda x: (x['Profit']/(x['Plan Profit']*x['Annualize'])) - 1 if x['Plan Profit'] > 0.0 else np.nan, axis=1)
                df['Margin'] = df.apply(lambda x: x['Profit']/x['Revenue'], axis=1)
                df['Plan Margin'] = df.apply(lambda x: x['Plan Profit'] / x['Plan Quarterly Sales'] if x['Plan Quarterly Sales'] > 0.0 else np.nan, axis=1)
                df['MARGINPercentPlan'] = df.apply(lambda x: x['Plan Margin'] - x['Margin'] if x['Plan Quarterly Sales'] > 0.0 else np.nan, axis=1)
                df['REVENUE_GROWTH'] = df.apply(lambda x: ((x['Revenue']/x['LY REVENUE']) - 1) if x['LY REVENUE'] > 0.0 else np.nan, axis=1)
                df['COST_GROWTH'] = df.apply(lambda x: ((x['Cost']/x['LY COST']) - 1) if x['LY COST'] > 0.0 else np.nan, axis=1)
                df['LY MARGIN'] = df.apply(lambda x: (x['LY PROFIT'] / x['LY REVENUE']) if x['LY REVENUE'] > 0.0 else np.nan, axis=1)
                df['YoY Diff'] = df.apply(lambda x: (x['LY MARGIN'] - x['Margin']) if x['LY MARGIN'] > 0.0 else np.nan, axis=1)
                df['PROFIT_GROWTH'] = df.apply(lambda x: ((x['Profit']/x['LY PROFIT']) - 1) if x['LY PROFIT'] > 0.0 else np.nan, axis=1)
                df['VOLUME_GROWTH'] = df.apply(lambda x: ((x['Volume']/x['LY VOLUME']) - 1) if x['LY VOLUME'] > 0.0 else np.nan, axis=1)
                #Annualizations
                df['RevPercentPlan'] = df.apply(lambda x: (x['Revenue']/(x['Plan Quarterly Sales'] * x['Annualize'])) - 1 if pd.to_datetime(x['End']) > actual_date else x['RevPercentPlan'], axis=1)
                df['COGSPercentPlan'] = df.apply(lambda x: (x['Cost']/(x['Plan Quarterly COGS'] * x['Annualize'])) - 1 if pd.to_datetime(x['End']) > actual_date else x['COGSPercentPlan'], axis=1)
                df['ProfitPercentPlan'] = df.apply(lambda x: (x['Profit']/(x['Plan Profit'] * x['Annualize'])) - 1 if pd.to_datetime(x['End']) > actual_date else x['ProfitPercentPlan'], axis=1)
                df['REVENUE_GROWTH'] = df.apply(lambda x: (x['Revenue']/(x['LY REVENUE'] * x['Annualize'])) - 1 if pd.to_datetime(x['End']) > actual_date else x['REVENUE_GROWTH'], axis=1)
                df['COST_GROWTH'] = df.apply(lambda x: (x['Cost']/(x['LY COST'] * x['Annualize'])) - 1 if pd.to_datetime(x['End']) > actual_date else x['COST_GROWTH'], axis=1)
                df['PROFIT_GROWTH'] = df.apply(lambda x: (x['Profit']/(x['LY PROFIT'] * x['Annualize'])) - 1 if pd.to_datetime(x['End']) > actual_date else x['PROFIT_GROWTH'], axis=1)
                df['VOLUME_GROWTH'] = df.apply(lambda x: (x['Volume']/(x['LY VOLUME'] * x['Annualize'])) - 1 if pd.to_datetime(x['End']) > actual_date else x['VOLUME_GROWTH'], axis=1)
                
                df = df[[
                                'Quarter', 'Dept', 'Revenue', 'Plan Quarterly Sales', 'RevPercentPlan', 'LY REVENUE', 'REVENUE_GROWTH',
                                'Cost', 'Plan Quarterly COGS', 'COGSPercentPlan', 'LY COST', 'COST_GROWTH',
                                'Margin', 'Plan Margin', 'MARGINPercentPlan', 'LY MARGIN', 'YoY Diff',
                                'Profit', 'Plan Profit', 'ProfitPercentPlan', 'LY PROFIT', 'PROFIT_GROWTH',
                                'Volume', 'LY VOLUME', 'VOLUME_GROWTH',
                                'TotalGPVariance',
                                'VarDuetoMargin',
                                'VarDueToSalesVolume',
                                'LaborCost',
                                'PlanLaborCost',
                                'PercentPlanLaborCost',
                                'LYLaborCost',
                                'LaborCostGrowth',
                                'LaborHours',
                                'PlanLaborHours',
                                'PercentPlanLaborHours',
                                'LYLaborHours',
                                'LaborHoursGrowth',
                                'SalesToLaborHours',
                                'PlanSalesToLaborHours',
                                'PercentPlanSalesToLaborHours',
                                'LYSalesToLaborHours',
                                'SalesToLaborHoursGrowth',
                                'MarginAfterLabor',
                                'PlanMarginAfterLabor',
                                'PercentPlanMarginAfterLabor',
                                'LYMarginAfterLabor',
                                'MarginAfterLaborGrowth'
                            ]]
                self.quarterly_totals = self.quarterly_totals[[
                                'Quarter', 'Dept', 'Revenue', 'Plan Quarterly Sales', 'RevPercentPlan', 'LY REVENUE', 'REVENUE_GROWTH',
                                'Cost', 'Plan Quarterly COGS', 'COGSPercentPlan', 'LY COST', 'COST_GROWTH',
                                'Margin', 'Plan Margin', 'MARGINPercentPlan', 'LY MARGIN', 'YoY Diff',
                                'Profit', 'Plan Profit', 'ProfitPercentPlan', 'LY PROFIT', 'PROFIT_GROWTH',
                                'Volume', 'LY VOLUME', 'VOLUME_GROWTH',
                                'TotalGPVariance',
                                'VarDuetoMargin',
                                'VarDueToSalesVolume',
                                'LaborCost',
                                'PlanLaborCost',
                                'PercentPlanLaborCost',
                                'LYLaborCost',
                                'LaborCostGrowth',
                                'LaborHours',
                                'PlanLaborHours',
                                'PercentPlanLaborHours',
                                'LYLaborHours',
                                'LaborHoursGrowth',
                                'SalesToLaborHours',
                                'PlanSalesToLaborHours',
                                'PercentPlanSalesToLaborHours',
                                'LYSalesToLaborHours',
                                'SalesToLaborHoursGrowth',
                                'MarginAfterLabor',
                                'PlanMarginAfterLabor',
                                'PercentPlanMarginAfterLabor',
                                'LYMarginAfterLabor',
                                'MarginAfterLaborGrowth'
                            ]]
                df = df.astype({
                                'Quarter' : str, 'Dept' : str, 'Revenue' : str, 'Plan Quarterly Sales' : str, 'RevPercentPlan' : str, 'LY REVENUE' : str, 'REVENUE_GROWTH' : str,
                                'Cost' : str, 'Plan Quarterly COGS' : str, 'COGSPercentPlan' : str, 'LY COST' : str, 'COST_GROWTH' : str,
                                'Margin' : str, 'Plan Margin' : str, 'MARGINPercentPlan' : str, 'LY MARGIN' : str, 'YoY Diff' : str,
                                'Profit' : str, 'Plan Profit' : str, 'ProfitPercentPlan' : str, 'LY PROFIT' : str, 'PROFIT_GROWTH' : str,
                                'Volume' : str, 'LY VOLUME' : str, 'VOLUME_GROWTH' : str,
                                'TotalGPVariance' : str,
                                'VarDuetoMargin' : str,
                                'VarDueToSalesVolume' : str,
                                'LaborCost' : str,
                                'PlanLaborCost' : str,
                                'PercentPlanLaborCost' : str,
                                'LYLaborCost' : str,
                                'LaborCostGrowth' : str,
                                'LaborHours' : str,
                                'PlanLaborHours' : str,
                                'PercentPlanLaborHours' : str,
                                'LYLaborHours' : str,
                                'LaborHoursGrowth' : str,
                                'SalesToLaborHours' : str,
                                'PlanSalesToLaborHours' : str,
                                'PercentPlanSalesToLaborHours' : str,
                                'LYSalesToLaborHours' : str,
                                'SalesToLaborHoursGrowth' : str,
                                'MarginAfterLabor' : str,
                                'PlanMarginAfterLabor' : str,
                                'PercentPlanMarginAfterLabor' : str,
                                'LYMarginAfterLabor' : str,
                                'MarginAfterLaborGrowth' : str
                })
                df = df.replace(['0', '0.0', 'nan', "NaN"], '')
                self.quarterly_totals = pd.concat([self.quarterly_totals, df], ignore_index=True, axis=0)
                self.quarterly_totals['Dept'] = pd.Categorical(self.quarterly_totals['Dept'], categories=dept_order, ordered=True)
                
                
                self.quarterly_totals.sort_values(['Quarter', 'Dept'], ascending=[False, True],inplace=True,  axis=0)
                
                #print(self.quarterly_totals)
                #self.quarterly_totals.to_csv("agged_month.csv")
            case "Yearly":
                df = self.yearly_totals.replace('', np.nan, regex=True)
                
                self.yearly_totals = self.yearly_totals[[
                    'Year', 'Dept', 'Revenue', 'Plan Yearly Sales', 'RevPercentPlan', 'LY REVENUE', 'REVENUE_GROWTH',
                    'Cost', 'Plan Yearly COGS', 'COGSPercentPlan', 'LY COST', 'COST_GROWTH',
                    'Margin', 'Plan Margin', 'MARGINPercentPlan', 'LY MARGIN', 'YoY Diff',
                    'Profit', 'Plan Profit', 'ProfitPercentPlan', 'LY PROFIT', 'PROFIT_GROWTH',
                    'Volume', 'LY VOLUME', 'VOLUME_GROWTH',
                    'TotalGPVariance',
                    'VarDuetoMargin',
                    'VarDueToSalesVolume',
                    'LaborCost',
                    'PlanLaborCost',
                    'PercentPlanLaborCost',
                    'LYLaborCost',
                    'LaborCostGrowth',
                    'LaborHours',
                    'PlanLaborHours',
                    'PercentPlanLaborHours',
                    'LYLaborHours',
                    'LaborHoursGrowth',
                    'SalesToLaborHours',
                    'PlanSalesToLaborHours',
                    'PercentPlanSalesToLaborHours',
                    'LYSalesToLaborHours',
                    'SalesToLaborHoursGrowth',
                    'MarginAfterLabor',
                    'PlanMarginAfterLabor',
                    'PercentPlanMarginAfterLabor',
                    'LYMarginAfterLabor',
                    'MarginAfterLaborGrowth'
                ]]
                df = df[df['Dept'] != 'Store Wide']
                df = df.astype({'Annualize' : float, 'Revenue' : float, 'Plan Yearly Sales' : float, 'RevPercentPlan' : float, 'LY REVENUE' : float, 'REVENUE_GROWTH' : float,
                                'Cost' : float, 'Plan Yearly COGS' : float, 'COGSPercentPlan' : float, 'LY COST' : float, 'COST_GROWTH' : float,
                                'Margin' : float, 'Plan Margin' : float, 'MARGINPercentPlan' : float, 'LY MARGIN' : float, 'YoY Diff' : float,
                                'Profit' : float, 'Plan Profit' : float, 'ProfitPercentPlan' : float, 'LY PROFIT' : float, 'PROFIT_GROWTH' : float,
                                'Volume' : float, 'LY VOLUME' : float, 'VOLUME_GROWTH': float,
                                'TotalGPVariance' : float,
                                'VarDuetoMargin' : float,
                                'VarDueToSalesVolume' : float,
                                'LaborCost' : float,
                                'PlanLaborCost' : float,
                                'PercentPlanLaborCost' : float,
                                'LYLaborCost' : float,
                                'LaborCostGrowth' : float,
                                'LaborHours' : float,
                                'PlanLaborHours' : float,
                                'PercentPlanLaborHours' : float,
                                'LYLaborHours' : float,
                                'LaborHoursGrowth' : float,
                                'SalesToLaborHours' : float,
                                'PlanSalesToLaborHours' : float,
                                'PercentPlanSalesToLaborHours' : float,
                                'LYSalesToLaborHours' : float,
                                'SalesToLaborHoursGrowth' : float,
                                'MarginAfterLabor' : float,
                                'PlanMarginAfterLabor' : float,
                                'PercentPlanMarginAfterLabor' : float,
                                'LYMarginAfterLabor' : float,
                                'MarginAfterLaborGrowth' : float
                                })
                df = df.groupby("Year").agg({
                    'Annualize' : 'first', 'Revenue' : sum, 'Plan Yearly Sales': sum, 'LY REVENUE' : sum,
                    'Cost' : sum, 'Plan Yearly COGS' : sum, 'LY COST' : sum,
                    'Profit' : sum, 'Plan Profit' : sum, 'LY PROFIT' : sum,
                    'Volume' : sum, 'LY VOLUME' : sum,
                    'TotalGPVariance' : sum,
                    'VarDuetoMargin' : sum,
                    'VarDueToSalesVolume' : sum,
                    'LaborCost' : sum,
                    'PlanLaborCost' : sum,
                    'PercentPlanLaborCost' : sum,
                    'LYLaborCost' : sum,
                    'LaborCostGrowth' : sum,
                    'LaborHours' : sum,
                    'PlanLaborHours' : sum,
                    'PercentPlanLaborHours' : sum,
                    'LYLaborHours' : sum,
                    'LaborHoursGrowth' : sum,
                    'SalesToLaborHours' : sum,
                    'PlanSalesToLaborHours' : sum,
                    'PercentPlanSalesToLaborHours' : sum,
                    'LYSalesToLaborHours' : sum,
                    'SalesToLaborHoursGrowth' : sum,
                    'MarginAfterLabor' : sum,
                    'PlanMarginAfterLabor' : sum,
                    'PercentPlanMarginAfterLabor' : sum,
                    'LYMarginAfterLabor' : sum,
                    'MarginAfterLaborGrowth' : sum    
                })
                
                df['Dept'] = "Total"
                df = df.reset_index()
                df['RevPercentPlan'] = df.apply(lambda x: ((x['Revenue']/(x['Plan Yearly Sales']*x['Annualize'])) - 1) if x['Plan Yearly Sales'] > 0.0 else np.nan, axis=1)
                df['COGSPercentPlan'] = df.apply(lambda x: (x['Cost']/(x['Plan Yearly COGS']*x['Annualize'])) - 1 if x['Plan Yearly COGS'] > 0.0 else np.nan, axis=1)
                df['ProfitPercentPlan'] = df.apply(lambda x: (x['Profit']/(x['Plan Profit']*x['Annualize'])) - 1 if x['Plan Profit'] > 0.0 else np.nan, axis=1)
                df['Margin'] = df.apply(lambda x: x['Profit']/x['Revenue'], axis=1)
                df['Plan Margin'] = df.apply(lambda x: x['Plan Profit'] / x['Plan Yearly Sales'] if x['Plan Yearly Sales'] > 0.0 else np.nan, axis=1)
                df['MARGINPercentPlan'] = df.apply(lambda x: x['Plan Margin'] - x['Margin'] if x['Plan Yearly Sales'] > 0.0 else np.nan, axis=1)
                df['REVENUE_GROWTH'] = df.apply(lambda x: ((x['Revenue']/x['LY REVENUE']) - 1) if x['LY REVENUE'] > 0.0 else np.nan, axis=1)
                df['COST_GROWTH'] = df.apply(lambda x: ((x['Cost']/x['LY COST']) - 1) if x['LY COST'] > 0.0 else np.nan, axis=1)
                df['LY MARGIN'] = df.apply(lambda x: (x['LY PROFIT'] / x['LY REVENUE']) if x['LY REVENUE'] > 0.0 else np.nan, axis=1)
                df['YoY Diff'] = df.apply(lambda x: (x['LY MARGIN'] - x['Margin']) if x['LY MARGIN'] > 0.0 else np.nan, axis=1)
                df['PROFIT_GROWTH'] = df.apply(lambda x: ((x['Profit']/x['LY PROFIT']) - 1) if x['LY PROFIT'] > 0.0 else np.nan, axis=1)
                df['VOLUME_GROWTH'] = df.apply(lambda x: ((x['Volume']/x['LY VOLUME']) - 1) if x['LY VOLUME'] > 0.0 else np.nan, axis=1)
                #Annualizations
                df['RevPercentPlan'] = df.apply(lambda x: (x['Revenue']/(x['Plan Yearly Sales'] * x['Annualize'])) - 1 if x['Year'] == "2023" else x['RevPercentPlan'], axis=1)
                df['COGSPercentPlan'] = df.apply(lambda x: (x['Cost']/(x['Plan Yearly COGS'] * x['Annualize'])) - 1 if x['Year'] == "2023" else x['COGSPercentPlan'], axis=1)
                df['ProfitPercentPlan'] = df.apply(lambda x: (x['Profit']/(x['Plan Profit'] * x['Annualize'])) - 1 if x['Year'] == "2023" else x['ProfitPercentPlan'], axis=1)
                df['REVENUE_GROWTH'] = df.apply(lambda x: (x['Revenue']/(x['LY REVENUE'] * x['Annualize'])) - 1 if x['Year'] == "2023" else x['REVENUE_GROWTH'], axis=1)
                df['COST_GROWTH'] = df.apply(lambda x: (x['Cost']/(x['LY COST'] * x['Annualize'])) - 1 if x['Year'] == "2023" else x['COST_GROWTH'], axis=1)
                df['PROFIT_GROWTH'] = df.apply(lambda x: (x['Profit']/(x['LY PROFIT'] * x['Annualize'])) - 1 if x['Year'] == "2023" else x['PROFIT_GROWTH'], axis=1)
                df['VOLUME_GROWTH'] = df.apply(lambda x: (x['Volume']/(x['LY VOLUME'] * x['Annualize'])) - 1 if x['Year'] == "2023" else x['VOLUME_GROWTH'], axis=1)
                df = df[[
                                'Year', 'Dept', 'Revenue', 'Plan Yearly Sales', 'RevPercentPlan', 'LY REVENUE', 'REVENUE_GROWTH',
                                'Cost', 'Plan Yearly COGS', 'COGSPercentPlan', 'LY COST', 'COST_GROWTH',
                                'Margin', 'Plan Margin', 'MARGINPercentPlan', 'LY MARGIN', 'YoY Diff',
                                'Profit', 'Plan Profit', 'ProfitPercentPlan', 'LY PROFIT', 'PROFIT_GROWTH',
                                'Volume', 'LY VOLUME', 'VOLUME_GROWTH',
                                'TotalGPVariance',
                                'VarDuetoMargin',
                                'VarDueToSalesVolume',
                                'LaborCost',
                                'PlanLaborCost',
                                'PercentPlanLaborCost',
                                'LYLaborCost',
                                'LaborCostGrowth',
                                'LaborHours',
                                'PlanLaborHours',
                                'PercentPlanLaborHours',
                                'LYLaborHours',
                                'LaborHoursGrowth',
                                'SalesToLaborHours',
                                'PlanSalesToLaborHours',
                                'PercentPlanSalesToLaborHours',
                                'LYSalesToLaborHours',
                                'SalesToLaborHoursGrowth',
                                'MarginAfterLabor',
                                'PlanMarginAfterLabor',
                                'PercentPlanMarginAfterLabor',
                                'LYMarginAfterLabor',
                                'MarginAfterLaborGrowth'
                            ]]
                df = df.astype({
                                'Year' : str, 'Dept' : str, 'Revenue' : str, 'Plan Yearly Sales' : str, 'RevPercentPlan' : str, 'LY REVENUE' : str, 'REVENUE_GROWTH' : str,
                                'Cost' : str, 'Plan Yearly COGS' : str, 'COGSPercentPlan' : str, 'LY COST' : str, 'COST_GROWTH' : str,
                                'Margin' : str, 'Plan Margin' : str, 'MARGINPercentPlan' : str, 'LY MARGIN' : str, 'YoY Diff' : str,
                                'Profit' : str, 'Plan Profit' : str, 'ProfitPercentPlan' : str, 'LY PROFIT' : str, 'PROFIT_GROWTH' : str,
                                'Volume' : str, 'LY VOLUME' : str, 'VOLUME_GROWTH' : str,
                                'TotalGPVariance' : str,
                                'VarDuetoMargin' : str,
                                'VarDueToSalesVolume' : str,
                                'LaborCost' : str,
                                'PlanLaborCost' : str,
                                'PercentPlanLaborCost' : str,
                                'LYLaborCost' : str,
                                'LaborCostGrowth' : str,
                                'LaborHours' : str,
                                'PlanLaborHours' : str,
                                'PercentPlanLaborHours' : str,
                                'LYLaborHours' : str,
                                'LaborHoursGrowth' : str,
                                'SalesToLaborHours' : str,
                                'PlanSalesToLaborHours' : str,
                                'PercentPlanSalesToLaborHours' : str,
                                'LYSalesToLaborHours' : str,
                                'SalesToLaborHoursGrowth' : str,
                                'MarginAfterLabor' : str,
                                'PlanMarginAfterLabor' : str,
                                'PercentPlanMarginAfterLabor' : str,
                                'LYMarginAfterLabor' : str,
                                'MarginAfterLaborGrowth' : str
                })
                df = df.replace(['0', '0.0', 'nan', "NaN"], '')
                #self.yearly_totals = pd.concat([self.yearly_totals, df], ignore_index=True, axis=0).sort_values(['Year', 'Dept'], ascending=[False, True], axis=0)
                self.yearly_totals = pd.concat([self.yearly_totals, df], ignore_index=True, axis=0)
                self.yearly_totals['Dept'] = pd.Categorical(self.yearly_totals['Dept'], categories=dept_order, ordered=True)
                self.yearly_totals.sort_values(['Year', 'Dept'], ascending=[False, True], inplace=True, axis=0)
                #print(self.yearly_totals)
                #self.yearly_totals.to_csv("agged_month.csv")

    def build_book(self, folder_id, driveClient):
        output_workbook = createBook(self.Name + " Dashboard", folder_id, 'application/vnd.google-apps.spreadsheet', driveClient)
        
        self.workbook_id = output_workbook.get('id', '')
        tabs = {"Daily" : 57, "Weekly" : 52, "Monthly" : 52, "Quarterly" : 47, "Yearly" : 47}
        for tab_name in tabs:
            col = 2 if len(self.report_codes) > 1 else 1
            response = batchUpdate(self.workbook_id, body=addSheet(sheetProp(title=tab_name, gridProperties=gridProp(column=tabs[tab_name], frozenRow=2, frozenColumn=col))))
            
            match tab_name:
                case "Daily": self.daily_sh_id = response.get('replies', '')[0].get('addSheet', '').get('properties', '').get('sheetId', '')
                case "Weekly": self.weekly_sh_id = response.get('replies', '')[0].get('addSheet', '').get('properties', '').get('sheetId', '')
                case "Monthly": self.monthly_sh_id = response.get('replies', '')[0].get('addSheet', '').get('properties', '').get('sheetId', '')
                case "Quarterly": self.quarterly_sh_id = response.get('replies', '')[0].get('addSheet', '').get('properties', '').get('sheetId', '')
                case "Yearly": self.yearly_sh_id = response.get('replies', '')[0].get('addSheet', '').get('properties', '').get('sheetId', '')
        self.build_plan_sheet()
        batchUpdate(self.workbook_id, deleteSheet(0))

    def build_sheet(self, period):
        if self.workbook_id != "":
            self.sheet_template(period)
        else:
            print("No workbook associated with this department yet.")
    def build_plan_sheet(self):
        if self.workbook_id != "":
            row_length = 6 if len(self.reports) == 1 else 6 + ((len(self.reports))*5)
            response = batchUpdate(self.workbook_id, body=addSheet(sheetProp(title="Plan Figures FY23", gridProperties=gridProp(row=row_length, column=4))))
            
            sheet_id = response.get('replies', '')[0].get('addSheet', '').get('properties', '').get('sheetId', '')

            append(self.workbook_id, "Plan Figures FY23!A1:B1", "ROWS", [['Period', 'Margin', 'Sales', 'COGS']])
            update_values(self.workbook_id, "Plan Figures FY23!A1:D1", 'USER_ENTERED', [['Period', 'Margin', 'Sales', 'COGS']])
            update_values(self.workbook_id, "Plan Figures FY23!A2:A", 'USER_ENTERED', [['Yearly'], ['Q1'], ['Q2'], ['Q3'], ['Q4']])
            update_values(self.workbook_id, "Plan Figures FY23!B2:B", 'USER_ENTERED', [[el] for el in [self.year_margin] + self.quarterly_margin])
            update_values(self.workbook_id, "Plan Figures FY23!C2:C", 'USER_ENTERED', [[el] for el in [self.year_sales] + self.quarterly_sales])
            update_values(self.workbook_id, "Plan Figures FY23!D2:D", 'USER_ENTERED', [[el] for el in [self.year_COGS] + self.quarterly_COGS])

            if len(self.reports) > 1:
                for report in self.reports:
                    append(self.workbook_id, "Plan Figures FY23!A1:B1", "ROWS",
                    [[report.Name + " Yearly", report.year_margin, report.year_sales, report.year_COGS],
                    [report.Name + " Q1", report.quarterly_margin[0], report.quarterly_sales[0], report.quarterly_COGS[0]],
                    [report.Name + " Q2", report.quarterly_margin[1], report.quarterly_sales[1], report.quarterly_COGS[1]],
                    [report.Name + " Q3", report.quarterly_margin[2], report.quarterly_sales[2], report.quarterly_COGS[2]],
                    [report.Name + " Q4", report.quarterly_margin[3], report.quarterly_sales[3], report.quarterly_COGS[3]]], valueInput="USER_ENTERED")
            
            format_body = [
                repeatCell( gridRange(sheet_id, 0, 1, 0, 4),
                            cellData(userEnteredFormat=cellFormat(
                                                            borders=borders(*all_thick),
                                                            horizontalAlignment="CENTER",
                                                            verticalAlignment="MIDDLE",
                                                            textFormat=textFormat(fontFamily="Arial", fontSize=14, bold=True),
                                                            backgroundColor=getColor(25,225,100,0.01)
                                                        )
                                )
                    ),
                repeatCell(gridRange(sheet_id, 1, row_length, 0, 6), cellData(userEnteredFormat=cellFormat(borders=borders(*all_solid), textFormat=textFormat(fontFamily="Arial", fontSize=12)))),
                repeatCell(gridRange(sheet_id, 1, row_length, 1, 2), cellData(userEnteredFormat=cellFormat(numberFormat=percent))),
                repeatCell(gridRange(sheet_id, 1, row_length, 2, 4), cellData(userEnteredFormat=cellFormat(numberFormat=currency))),
                autoSize(sheet_id, "COLUMNS")
            ]

            if len(self.reports) > 1:
                for i in range(0, len(self.reports)):
                    format_body.append(repeatCell(gridRange(sheet_id, 5+i*5, 6+i*5, 0, 4), cellData(userEnteredFormat=cellFormat(borders=borders(bottom=thick)))))
            
            batchUpdate(self.workbook_id, body=format_body)
        else:
            print("No workbook built for this department yet.")

    def sheet_template(self, period):
        multireport = len(self.report_codes) > 1
        match period:
            case "Daily":
                if multireport:
                    self.recalc_totals(period)
                    d = self.daily_totals
                    d['Day'] = d.apply(lambda x: x['Day'] if x['Dept'] == 'Total' else '', axis=1)
                    df = d
                else:
                    print(self.daily_totals.columns)
                    df = self.daily_totals.drop("Dept", axis=1)
                    print(df)
                heads = totalDailyHeaders if multireport else mergeDailyHeaders
                merge_list = grand_daily_merge_range if multireport else daily_merge_ranges
                sheet_id = self.daily_sh_id
                ninety_day_offset = [4, 2]
            case "Weekly":
                print(self.weekly_totals)
                if multireport:
                    self.recalc_totals(period)
                    d = self.weekly_totals
                    d['Day'] = d.apply(lambda x: x['Day'] if x['Dept'] == 'Total' else '', axis=1)
                    df = d
                    print("AFTER",df)
                else:
                    df = self.weekly_totals.drop(["Dept", 'Annualize'], axis=1)
                heads = totalWeeklyMonthlyHeaders if multireport else mergeWeeklyMonthlyHeaders
                merge_list = grand_weekly_monthly_merge_range if multireport else weekly_monthly_merge_ranges
                sheet_id = self.weekly_sh_id
                ninety_day_offset = [3, 1]
            case "Monthly":
                if multireport:
                    self.recalc_totals(period)
                    d = self.monthly_totals
                    d['Day'] = d.apply(lambda x: x['Day'] if x['Dept'] == 'Total' else '', axis=1)
                    df = d
                else:
                    df = self.monthly_totals.drop(["Dept", 'Annualize'], axis=1)
                heads = totalWeeklyMonthlyHeaders if multireport else mergeWeeklyMonthlyHeaders
                merge_list = grand_weekly_monthly_merge_range if multireport else weekly_monthly_merge_ranges
                sheet_id = self.monthly_sh_id
                ninety_day_offset = [3, 1]
            case "Quarterly":
                if multireport:
                    self.recalc_totals(period)
                    d = self.quarterly_totals
                    d['Quarter'] = d.apply(lambda x: x['Quarter'] if x['Dept'] == 'Total' else '', axis=1)
                    df = d
                else:
                    df = self.quarterly_totals.drop(["Dept", 'Annualize'], axis=1)
                heads = totalQuarterlyHeaders if multireport else mergeQuarterlyHeaders
                merge_list = grand_quarterly_merge_range if multireport else quarterly_merge_ranges
                sheet_id = self.quarterly_sh_id
                ninety_day_offset = [0, 0]
            case "Yearly":
                if multireport:
                    self.recalc_totals(period)
                    d = self.yearly_totals
                    d['Year'] = d.apply(lambda x: x['Year'] if x['Dept'] == 'Total' else '', axis=1)
                    df = d
                    print("report codes", self.report_codes)
                else:
                    df = self.yearly_totals.drop(["Dept", 'Annualize'], axis=1)
                heads = totalYearlyHeaders if multireport else mergeYearlyHeaders
                merge_list = grand_yearly_merge_range if multireport else yearly_merge_ranges
                sheet_id = self.yearly_sh_id
                ninety_day_offset = [0, 0]
        append(self.workbook_id, period+"!A1:B1", "ROWS", heads, "USER_ENTERED")
        append(self.workbook_id, period+"!A3:B3", "ROWS", df.values.tolist(), "USER_ENTERED")
        if period not in ["Quarterly", "Yearly"]:
            format_updates = [repeatCell(
                                gridRange(sheet_id, 2, df.shape[0]+2, 0, 1),
                                cellData(userEnteredFormat=cellFormat(
                                                            numberFormat=numberFormat("DATE", "m/d/yy"),
                                                            textFormat=textFormat(fontFamily="Arial", fontSize=12, bold=True)
                                                        )))
                        ]
        else:
            format_updates = [repeatCell(
                                gridRange(sheet_id, 2, df.shape[0]+2, 0, 1),
                                cellData(userEnteredFormat=cellFormat(
                                                            textFormat=textFormat(fontFamily="Arial", fontSize=12, bold=True)
                                                        )))
                        ]
        if multireport:
            # iterator_length = (len(self.report_codes) + 1) if 0 not in self.report_codes else len(self.report_codes)
            # starting_position = len(self.report_codes) + 2 if 0 not in self.report_codes else len(self.report_codes) + 1
            #print("iter len", iterator_length)
            for i in range(len(self.report_codes) + 2, df.shape[0]+2, len(self.report_codes) + 1):
                format_updates.append(
                    repeatCell(
                                gridRange(sheet_id, i, i+1, 0, merge_list[9][1]),
                                cellData(userEnteredFormat=cellFormat(
                                                            borders=borders(bottom=thick, top=solid)
                                                        )))
                )
            #Border on the left side of the Dept listing if there's a multi report.
            format_updates.append(repeatCell(gridRange(sheet_id, 2, df.shape[0]+2, 0, 1),cellData(userEnteredFormat=cellFormat(borders=borders(right=thick)))))
        first_format = []
        #second row of headers                                    
        first_format.append(repeatCell(gridRange(sheet_id, 1, 2, 1, merge_list[9][1]),
                                        cellData(userEnteredFormat=cellFormat(
                                            borders=borders(*all_thick),
                                            horizontalAlignment="CENTER",
                                            verticalAlignment="MIDDLE",
                                            textFormat=textFormat(fontFamily="Arial", fontSize=12, bold=True),
                                            backgroundColor=getColor(239, 175, 0, 0.1)
                                        ))))
        #broad data formatting
        first_format.append(repeatCell(gridRange(sheet_id, 2, df.shape[0]+2, 1 if multireport else 0, merge_list[9][1]),
                                        cellData(userEnteredFormat=cellFormat(
                                            borders=borders(*all_solid),
                                            textFormat=textFormat(fontFamily="Arial", fontSize=12)
                                        ))))
        batchUpdate(self.workbook_id, first_format)
        
        k = 0
        for i in merge_list:
            firstRow = gridRange(sheet_id, 0, 1, i[0], i[1])
            
            format_updates.append(mergeCells(firstRow, "MERGE_ALL"))
            format_updates.append(repeatCell(firstRow, cellData(userEnteredFormat=cellFormat(
                                                borders=borders(*all_thick),
                                                horizontalAlignment="CENTER",
                                                verticalAlignment="MIDDLE",
                                                textFormat=textFormat(fontFamily="Arial", fontSize=14, bold=True),
                                                backgroundColor=getColor(239, 175, 0, 0.1)
                                            ))))
            format_updates.append(repeatCell(gridRange(sheet_id, 2, df.shape[0]+2, i[1]-1, i[1]), cellData(userEnteredFormat=cellFormat(borders=borders(right=thick)))))
            #Sales, COGS, Profit
            if k in [0, 1, 3]:
                format_updates.append(repeatCell(gridRange(sheet_id, 2, df.shape[0]+2, i[0], i[0]+2), cellData(userEnteredFormat=cellFormat(numberFormat=currency))))
                format_updates.append(repeatCell(gridRange(sheet_id, 2, df.shape[0]+2, i[0]+2, i[0]+3), cellData(userEnteredFormat=cellFormat(numberFormat=percent))))
                format_updates.append(repeatCell(gridRange(sheet_id, 2, df.shape[0]+2, i[0]+3, i[1]-1), cellData(userEnteredFormat=cellFormat(numberFormat=currency))))
                format_updates.append(repeatCell(gridRange(sheet_id, 2, df.shape[0]+2, i[1]-1, i[1]), cellData(userEnteredFormat=cellFormat(numberFormat=percent))))
                #https://stackoverflow.com/questions/58129090/how-to-append-a-relative-formula-into-a-sheet-using-the-google-sheet-api
                if ninety_day_offset[0] > 0:
                    format_updates.append(addConditional(rule(gridRange(sheet_id, 2, df.shape[0]+2, i[1]-3, i[1]-2), boolRule(boolCond("CUSTOM_FORMULA", [condVal("=INDIRECT(\"R[0]C[-%s]\", FALSE)>INDIRECT(\"R[0]C[0]\", FALSE)" % ninety_day_offset[0])]), cellFormat(backgroundColor=greenVal))), 0))
                    format_updates.append(addConditional(rule(gridRange(sheet_id, 2, df.shape[0]+2, i[1]-3, i[1]-2), boolRule(boolCond("CUSTOM_FORMULA", [condVal("=INDIRECT(\"R[0]C[-%s]\", FALSE)<INDIRECT(\"R[0]C[0]\", FALSE)" % ninety_day_offset[0])]), cellFormat(backgroundColor=redVal))), 0))
                format_updates.append(addConditional(rule(gridRange(sheet_id, 2, df.shape[0]+2, i[0]+2, i[0]+3), boolRule(boolCond("NUMBER_GREATER", [condVal("0")]), cellFormat(backgroundColor=greenVal))), 0))
                format_updates.append(addConditional(rule(gridRange(sheet_id, 2, df.shape[0]+2, i[0]+2, i[0]+3), boolRule(boolCond("NUMBER_LESS", [condVal("0")]), cellFormat(backgroundColor=redVal))), 0))
                format_updates.append(addConditional(rule(gridRange(sheet_id, 2, df.shape[0]+2, i[1]-1, i[1]), boolRule(boolCond("NUMBER_GREATER", [condVal("0")]), cellFormat(backgroundColor=greenVal))), 0))
                format_updates.append(addConditional(rule(gridRange(sheet_id, 2, df.shape[0]+2, i[1]-1, i[1]), boolRule(boolCond("NUMBER_LESS", [condVal("0")]), cellFormat(backgroundColor=redVal))), 0))
            #Margin
            elif k == 2:
                format_updates.append(repeatCell(gridRange(sheet_id, 2, df.shape[0]+2, i[0], i[1]+1), cellData(userEnteredFormat=cellFormat(numberFormat=percent))))
                if ninety_day_offset[0] > 0:
                    format_updates.append(addConditional(rule(gridRange(sheet_id, 2, df.shape[0]+2, i[1]-3, i[1]-2), boolRule(boolCond("CUSTOM_FORMULA", [condVal("=INDIRECT(\"R[0]C[-%s]\", FALSE)>INDIRECT(\"R[0]C[0]\", FALSE)" % ninety_day_offset[0])]), cellFormat(backgroundColor=greenVal))), 0))
                    format_updates.append(addConditional(rule(gridRange(sheet_id, 2, df.shape[0]+2, i[1]-3, i[1]-2), boolRule(boolCond("CUSTOM_FORMULA", [condVal("=INDIRECT(\"R[0]C[-%s]\", FALSE)<INDIRECT(\"R[0]C[0]\", FALSE)" % ninety_day_offset[0])]), cellFormat(backgroundColor=redVal))), 0))
                format_updates.append(addConditional(rule(gridRange(sheet_id, 2, df.shape[0]+2, i[0]+2, i[0]+3), boolRule(boolCond("NUMBER_GREATER", [condVal("0")]), cellFormat(backgroundColor=greenVal))), 0))
                format_updates.append(addConditional(rule(gridRange(sheet_id, 2, df.shape[0]+2, i[0]+2, i[0]+3), boolRule(boolCond("NUMBER_LESS", [condVal("0")]), cellFormat(backgroundColor=redVal))), 0))
                format_updates.append(addConditional(rule(gridRange(sheet_id, 2, df.shape[0]+2, i[1]-1, i[1]), boolRule(boolCond("NUMBER_GREATER", [condVal("0")]), cellFormat(backgroundColor=greenVal))), 0))
                format_updates.append(addConditional(rule(gridRange(sheet_id, 2, df.shape[0]+2, i[1]-1, i[1]), boolRule(boolCond("NUMBER_LESS", [condVal("0")]), cellFormat(backgroundColor=redVal))), 0))
            #Volume
            elif k == 4:
                format_updates.append(repeatCell(gridRange(sheet_id, 2, df.shape[0]+2, i[0], i[1]-1), cellData(userEnteredFormat=cellFormat(numberFormat=nmbr))))
                format_updates.append(repeatCell(gridRange(sheet_id, 2, df.shape[0]+2, i[1]-1, i[1]), cellData(userEnteredFormat=cellFormat(numberFormat=percent))))
                if ninety_day_offset[0] > 0:
                    format_updates.append(addConditional(rule(gridRange(sheet_id, 2, df.shape[0]+2, i[1]-3, i[1]-2), boolRule(boolCond("CUSTOM_FORMULA", [condVal("=INDIRECT(\"R[0]C[-%s]\", FALSE)>INDIRECT(\"R[0]C[0]\", FALSE)" % ninety_day_offset[1])]), cellFormat(backgroundColor=greenVal))), 0))
                    format_updates.append(addConditional(rule(gridRange(sheet_id, 2, df.shape[0]+2, i[1]-3, i[1]-2), boolRule(boolCond("CUSTOM_FORMULA", [condVal("=INDIRECT(\"R[0]C[-%s]\", FALSE)<INDIRECT(\"R[0]C[0]\", FALSE)" % ninety_day_offset[1])]), cellFormat(backgroundColor=redVal))), 0))
                format_updates.append(addConditional(rule(gridRange(sheet_id, 2, df.shape[0]+2, i[1]-1, i[1]), boolRule(boolCond("NUMBER_GREATER", [condVal("0")]), cellFormat(backgroundColor=greenVal))), 0))
                format_updates.append(addConditional(rule(gridRange(sheet_id, 2, df.shape[0]+2, i[1]-1, i[1]), boolRule(boolCond("NUMBER_LESS", [condVal("0")]), cellFormat(backgroundColor=redVal))), 0))
            
            k+=1
        format_updates.append(veryBasicFilter(gridRange(sheet_id, 1, df.shape[0]+2, 1, df.shape[1])))
        format_updates.append(autoSize(sheet_id, "COLUMNS"))
        batchUpdate(self.workbook_id, body=format_updates)
   
    def return_Group(self):
        print(
            self.Name,
            self.report_codes,
            self.year_sales,
            self.year_COGS,
            self.year_margin,
            self.quarterly_margin,
            self.quarterly_sales,
            self.quarterly_COGS,
            self.weekly_sales,
            self.weekly_COGS,
            self.daily_sales,
            self.daily_COGS,
            self.monthly_sales,
            self.monthly_COGS,
            self.daily_totals,
            self.weekly_totals,
            self.monthly_totals,
            self.quarterly_totals,
            self.yearly_totals
        )

plan_figs = get_values(spreadsheet_id=plan_figure_file_id, range_name="Collected!A1:BD20")
for x in plan_figs:
    print(x)            

lr = LaborReport()

ls = []
for k in dept_list.keys():
    ls.append(Department(dept_list[k], k))

# #for l in [x for x in ls if x.Name == 'Produce' or x.Name == 'Floral']:
for l in ls:
    l.populate_plan_data(plan_figs)
    l.populate_single_period("Daily", cnxn, lr)
    l.populate_single_period("Weekly", cnxn, lr)
    l.populate_single_period("Monthly", cnxn, lr)
    l.populate_single_period("Quarterly", cnxn, lr)
    l.populate_single_period("Yearly", cnxn, lr)
    if l.Name in ['Produce', 'Floral']: print(l.weekly_totals)

    # print(l.Name)
    # print(l.daily_totals)
    # print(l.weekly_totals)
    # l.build_book(dashboards_folder_id, drive)
    # l.build_sheet("Daily")
    # l.build_sheet("Weekly")
    # l.build_sheet("Monthly")
    # l.build_sheet("Quarterly")

rg = []
for g in report_groups.keys():
    #print([x for x in ls if x.report_code in report_groups[g]])
    rp = ReportGroup(g, [x for x in ls if x.report_code in report_groups[g]])
    #print(rp.Name, rp.year_sales)
    rg.append(rp)
    if rp.Name in ['Produce', "Total"]:
        rp.build_book(dashboards_folder_id, drive)
        rp.build_sheet("Daily")
        time.sleep(20)
        rp.build_sheet("Weekly")
        rp.build_sheet("Monthly")
        time.sleep(20)
        rp.build_sheet("Quarterly")
        rp.build_sheet("Yearly")
    #time.sleep(30)
    #rp.return_Group()
#print([x.return_Group() for x in rg])

total = ReportGroup("Store", ls)
#print(total.yearly_totals)
total.build_book(dashboards_folder_id, drive)
total.build_sheet("Daily")
total.build_sheet("Weekly")
total.build_sheet("Monthly")
total.build_sheet("Quarterly")
total.build_sheet("Yearly")
print("FINISHED")