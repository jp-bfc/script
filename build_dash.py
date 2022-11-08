import configparser
import pyodbc
import pandas as pd
from sqlite3 import converters
import xlrd
#https://docs.python.org/3/library/tkinter.html
import tkinter as tk
from tkinter import filedialog
from pathlib import Path
import datetime as dt
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
import time
import numpy as np
import datetime

tic = time.perf_counter()

config = configparser.ConfigParser()
config.read('script_configs.ini')
username = config['DEFAULT']['user']
password = config['DEFAULT']['password']
server = config['DEFAULT']['server']
port = config['DEFAULT']['port']
database = config['DEFAULT']['database']
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';PORT='+port+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()

report_code_dict = pd.DataFrame(np.array(
    #Probably best to make these alphabetical, maybe a different data structure as well, think about it in the AM.

    #Report Code
    [1,2,3,4,5,6,7,8,9,10,11,13,15,16,17,18,19,21,22],
    #Name
    ["Grocery", "Produce", "Dairy", "Beer", "Bulk", "Haba", "Taxable Grocery",	"Deli", "Meat", "Cheese", "Supplements", "Wine", "Frozen", "Housewares", "TCH", "Floral", "CBM", "Marketing", "Seafood"],
    #Year Sales
    [],
    #Year COGS
    [],
    #Year Margin
    []
    ),
    columns=['Report Code', 'Name', 'Plan Year Sales', 'Plan Year COGS', 'Plan Year Margin']
)


def get_department_period_report(report_code, period, db_connection):
    dash_name = ""
    if report_code_dict[report_code] != "Taxable Grocery":
        dash_name = report_code_dict[report_code].split()[0] + "Dashboard"
    else:
        dash_name = "Taxable Dashboard"
    
    match period:
        case "Daily":
            query = """select revenueReport.F254 as 'Day',
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
            where revenueReport.F03 = %s 
            group by revenueReport.F254, revenueReport.F03, costReport.F254, costReport.F03, costReport.F65, reportCodeTable.F1024
            order by revenueReport.F254 desc, reportCodeTable.F1024
            """ % str(report_code)
            print(period)
            return pd.read_sql(query, db_connection)
        case "Weekly":
            query = """select revenueReport.F254 as 'Day',
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
            where revenueReport.F03 = %s 
            group by revenueReport.F254, revenueReport.F03, costReport.F254, costReport.F03, costReport.F65, reportCodeTable.F1024
            order by revenueReport.F254 desc, reportCodeTable.F1024
            """ % str(report_code)
            print(period)
            return pd.read_sql(query, db_connection)
        case "Monthly":
            query = """
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
            where revenueReport.F03 = %s 
            group by revenueReport.F254, revenueReport.F03, costReport.F254, costReport.F03, costReport.F65, reportCodeTable.F1024
            order by revenueReport.F254 desc, reportCodeTable.F1024
            """ % str(report_code)
            print(period)
            return pd.read_sql(query, db_connection)
        case "Quarterly":
            query = """
            select qtr.[Quarter], qtr.Dept,
            sum(qtr.Revenue) as 'Revenue',
            LAG(sum(qtr.Revenue), 4) OVER (partition by qtr.Dept order by qtr.[Quarter], qtr.Dept) as 'LY_REVENUE',
            ROUND((sum(qtr.Revenue)/LAG(sum(qtr.Revenue), 4) OVER (partition by qtr.Dept order by qtr.[Quarter], qtr.Dept))-1, 4) as 'REVENUE_GROWTH',
            sum(qtr.Cost) as 'Cost',
            LAG(sum(qtr.Cost), 4) OVER (partition by qtr.Dept order by qtr.[Quarter], qtr.Dept) as 'LY_COST',
            ROUND((sum(qtr.Cost)/LAG(sum(qtr.Cost), 4) OVER (partition by qtr.Dept order by qtr.[Quarter], qtr.Dept))-1, 4) as 'COST_GROWTH',
            sum(qtr.Revenue) - sum(qtr.Cost) as 'Profit',
            LAG(sum(qtr.Revenue)-sum(qtr.Cost), 4) OVER (partition by qtr.Dept order by qtr.[Quarter], qtr.Dept) as 'LY_Profit',
            ROUND((sum(qtr.Revenue) - sum(qtr.Cost)) / LAG(sum(qtr.Revenue)-sum(qtr.Cost), 4) OVER (partition by qtr.Dept order by qtr.[Quarter], qtr.Dept) -1, 4) as 'PROFIT_GROWTH',
            (sum(qtr.Revenue) - sum(qtr.Cost))/ sum(qtr.Revenue) as 'Margin',
            LAG((sum(qtr.Revenue)-sum(qtr.Cost))/sum(qtr.Revenue), 4) OVER (partition by qtr.Dept order by qtr.[Quarter], qtr.Dept) as 'LY_Margin',
            (sum(qtr.Revenue) - sum(qtr.Cost))/ sum(qtr.Revenue) - LAG((sum(qtr.Revenue)-sum(qtr.Cost))/sum(qtr.Revenue), 4) OVER (partition by qtr.Dept order by qtr.[Quarter], qtr.Dept) as 'YoY Diff',
            sum(qtr.Volume) as 'Volume',
            LAG(sum(qtr.Volume), 4) OVER (partition by qtr.Dept order by qtr.[Quarter], qtr.Dept) as 'LY_VOLUME',
            ROUND((sum(qtr.Volume)/LAG(sum(qtr.Volume), 4) OVER (partition by qtr.Dept order by qtr.[Quarter], qtr.Dept))-1, 4) as 'VOL_GROWTH'
            from 
            (select 
            CASE
                    WHEN MONTH(revenueReport.F254) BETWEEN 7 AND 9 THEN convert(char(4), YEAR(revenueReport.F254) + 1) + ' Q1'
                    WHEN MONTH(revenueReport.F254) BETWEEN 10 AND 12 THEN convert(char(4), YEAR(revenueReport.F254) + 1) + ' Q2'
                    WHEN MONTH(revenueReport.F254) BETWEEN 1 AND 3 THEN convert(char(4), YEAR(revenueReport.F254)) + ' Q3'
                    WHEN MONTH(revenueReport.F254) BETWEEN 4 AND 6 THEN convert(char(4), YEAR(revenueReport.F254)) + ' Q4'
            END AS 'Quarter',
            reportCodeTable.F1024 as 'Dept',
            sum(revenueReport.F65) as 'Revenue',
            costReport.F65 as 'Cost',
            sum(revenueReport.F64) as 'Volume'
            from
            (select * from STORESQL.dbo.RPT_DPT 
            where F1031 = 'M' and F1034 = 3) revenueReport
            inner join (select * from STORESQL.dbo.RPT_DPT where F1031 = 'M' and F1034 = 8101) costReport on revenueReport.F254 = costReport.F254 and revenueReport.F03 = costReport.F03
            inner join (select F18, F1024 from STORESQL.dbo.RPC_TAB) reportCodeTable on revenueReport.F03 = reportCodeTable.F18
            where revenueReport.F03 = %s
            group by
            datepart(year, revenueReport.F254),
            CASE
                    WHEN MONTH(revenueReport.F254) BETWEEN 7 AND 9 THEN convert(char(4), YEAR(revenueReport.F254) + 1) + ' Q1'
                    WHEN MONTH(revenueReport.F254) BETWEEN 10 AND 12 THEN convert(char(4), YEAR(revenueReport.F254) + 1) + ' Q2'
                    WHEN MONTH(revenueReport.F254) BETWEEN 1 AND 3 THEN convert(char(4), YEAR(revenueReport.F254)) + ' Q3'
                    WHEN MONTH(revenueReport.F254) BETWEEN 4 AND 6 THEN convert(char(4), YEAR(revenueReport.F254)) + ' Q4'
            END,
            revenueReport.F03, costReport.F03, costReport.F65, reportCodeTable.F1024)
            qtr
            group by qtr.[Quarter], qtr.[Dept]
            order by qtr.[Dept], qtr.[Quarter] desc
            """ % str(report_code)
            print(period)
            return pd.read_sql(query, db_connection)
        case "Yearly":
            query = """
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
            where revenueReport.F03 = %s 
            group by revenueReport.F254, revenueReport.F03, costReport.F254, costReport.F03, costReport.F65, reportCodeTable.F1024
            order by revenueReport.F254 desc, reportCodeTable.F1024
            """ % str(report_code)
            print(period)
            return pd.read_sql(query, db_connection)
for code in report_code_dict:
    for p in ['Daily', 'Weekly', 'Monthly', 'Quarterly', 'Yearly']:
        df = get_department_period_report(code, p, cnxn)
        print(df)