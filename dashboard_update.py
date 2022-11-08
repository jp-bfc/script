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
import logging

#https://stackoverflow.com/questions/1508467/log-exception-with-traceback-in-python
LOG_FILENAME = 'dash_err_logs.txt'
logging.basicConfig(filename=LOG_FILENAME, level=logging.ERROR)

try:
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



    dept_daily_query="""
    select
    revenueReport.F254 as 'Day',
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
    select top(36)
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
    where F1031 = 'W' and F1034 = 3) revenueReport
    inner join (select * from STORESQL.dbo.RPT_DPT where F1031 = 'W' and F1034 = 8101) costReport on revenueReport.F254 = costReport.F254 and revenueReport.F03 = costReport.F03
    inner join (select F18, F1024 from STORESQL.dbo.RPC_TAB) reportCodeTable on revenueReport.F03 = reportCodeTable.F18
    where revenueReport.F03 not in (14, 21, 23, 97, 98, 99) 
    group by revenueReport.F254, revenueReport.F03, costReport.F254, costReport.F03, costReport.F65, reportCodeTable.F1024
    order by revenueReport.F254 desc, reportCodeTable.F1024
    """

    dept_monthly_query = """
    select top(36)
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
    where F1031 = 'M' and F1034 = 3) revenueReport
    inner join (select * from STORESQL.dbo.RPT_DPT where F1031 = 'M' and F1034 = 8101) costReport on revenueReport.F254 = costReport.F254 and revenueReport.F03 = costReport.F03
    inner join (select F18, F1024 from STORESQL.dbo.RPC_TAB) reportCodeTable on revenueReport.F03 = reportCodeTable.F18
    where revenueReport.F03 not in (14, 21, 23, 97, 98, 99) 
    group by revenueReport.F254, revenueReport.F03, costReport.F254, costReport.F03, costReport.F65, reportCodeTable.F1024
    order by revenueReport.F254 desc, reportCodeTable.F1024
    """

    dept_yearly_query = """
    select top(36)
    revenueReport.F254 as 'Day',
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
    select
    revenueReport.F254 as 'Day',
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
    select top(2)
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
    """


    grand_monthly_query = """
    select top(2)
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
    """


    grand_yearly_query = """
    select top(2)
    revenueReport.F254 as 'Day',
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

    dept_dashboard_list = ['Beer Dashboard',
    'Bulk Dashboard',
    'CBM Dashboard',
    'Cheese Dashboard',
    'Dairy Dashboard',
    'Deli Dashboard',
    'Floral Dashboard',
    'Frozen Dashboard',
    'Grocery Dashboard',
    'Haba Dashboard',
    'Housewares Dashboard',
    'Meat Dashboard',
    'Produce Dashboard',
    'Seafood Dashboard',
    'Supplements Dashboard',
    'Taxable Dashboard',
    'TCH Dashboard',
    'Wine Dashboard',
    'Store Wide Dashboard']


    dept_list = ['Beer Department', 'Bulk Department', 'CBM Department', 'Cheese Department', 'Dairy Department',
                'Deli Department', 'Floral Department', 'Frozen Department', 'Grocery Department', 'Haba Department',
                'Housewares Department', 'Meat Department', 'Produce Department', 'Seafood Department', 'Supplements Department',
                'Taxable Grocery', 'TCH Department', 'Wine Department', 'Store Wide']


    dct = {v: i for i, v in enumerate(dept_list)}

    all_daily = pd.read_sql(dept_daily_query,cnxn)
    all_daily = pd.concat([all_daily, pd.read_sql(grand_daily_query,cnxn)], ignore_index=True)
    all_weekly = pd.read_sql(dept_weekly_query,cnxn)
    all_weekly = pd.concat([all_weekly, pd.read_sql(grand_weekly_query,cnxn)], ignore_index=True)
    all_monthly = pd.read_sql(dept_monthly_query,cnxn)
    all_monthly = pd.concat([all_monthly, pd.read_sql(grand_monthly_query,cnxn)], ignore_index=True)
    all_yearly = pd.read_sql(dept_yearly_query,cnxn)
    all_yearly = pd.concat([all_yearly, pd.read_sql(grand_yearly_query,cnxn)], ignore_index=True)

    all_daily.fillna('', inplace=True)
    all_weekly.fillna('', inplace=True)
    all_monthly.fillna('', inplace=True)
    all_yearly.fillna('', inplace=True)

    all_daily['Day'] = pd.Categorical(all_daily['Day'], categories=all_daily['Day'].unique().sort(), ordered=True)
    all_daily['Dept'] = pd.Categorical(all_daily['Dept'], categories=dct, ordered=True)
    all_daily = all_daily.sort_values(['Day', 'Dept'], ascending=[False, True])
    all_weekly['Day'] = pd.Categorical(all_weekly['Day'], categories=all_weekly['Day'].unique().sort(), ordered=True)
    all_weekly['Dept'] = pd.Categorical(all_weekly['Dept'], categories=dct, ordered=True)
    all_weekly = all_weekly.sort_values(['Day', 'Dept'], ascending=[False, True])
    all_monthly['Day'] = pd.Categorical(all_monthly['Day'], categories=all_monthly['Day'].unique().sort(), ordered=True)
    all_monthly['Dept'] = pd.Categorical(all_monthly['Dept'], categories=dct, ordered=True)
    all_monthly = all_monthly.sort_values(['Day', 'Dept'], ascending=[False, True])
    all_yearly['Day'] = pd.Categorical(all_yearly['Day'], categories=all_yearly['Day'].unique().sort(), ordered=True)
    all_yearly['Dept'] = pd.Categorical(all_yearly['Dept'], categories=dct, ordered=True)
    all_yearly = all_yearly.sort_values(['Day', 'Dept'], ascending=[False, True])

    all_daily = all_daily[all_daily['Day'].isin([all_daily['Day'].cat.categories.tolist()[-1], all_daily['Day'].cat.categories.tolist()[-2]])]
    all_weekly = all_weekly[all_weekly['Day'].isin([all_weekly['Day'].cat.categories.tolist()[-1], all_weekly['Day'].cat.categories.tolist()[-2]])]
    all_monthly = all_monthly[all_monthly['Day'].isin([all_monthly['Day'].cat.categories.tolist()[-1], all_monthly['Day'].cat.categories.tolist()[-2]])]
    all_yearly = all_yearly[all_yearly['Day'].isin([all_yearly['Day'].cat.categories.tolist()[-1], all_yearly['Day'].cat.categories.tolist()[-2]])]

    all_daily = all_daily.astype({"Day" : str})
    all_weekly = all_weekly.astype({"Day" : str})
    all_monthly = all_monthly.astype({"Day" : str})
    all_yearly = all_yearly.astype({"Day" : str})

    all_daily.name = 'Daily'
    all_weekly.name = 'Weekly'
    all_monthly.name = 'Monthly'
    all_yearly.name = 'Yearly'

    SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    dashboards_folder_id = config['DEFAULT']['dashboard_update_folder']
    sheet_creds = service_account.Credentials.from_service_account_file(
                        'sheet_credentials.json', scopes=SCOPES)
    drive_creds = service_account.Credentials.from_service_account_file(
        'drive_credentials.json', scopes=SCOPES)

    #https://googleapis.github.io/google-api-python-client/docs/dyn/drive_v3.html
    drive = build('drive', 'v3', credentials=drive_creds)
    #https://googleapis.github.io/google-api-python-client/docs/dyn/sheets_v4.html
    sheets = build('sheets', 'v4', credentials=sheet_creds)

    def batchUpdate(workbookId, bodyList):
        return sheets.spreadsheets().batchUpdate(
            spreadsheetId = workbookId,
            body = {
                "requests" : bodyList
            }
        ).execute()

    # def userEnteredFormat():


    # def repeat_cell(sheetId, startRow, endRow, startColumn, endColumn):
    #     return {
    #         "repeatCell" :{
    #                 "range" : {
    #                     "sheetId" : sheetId,
    #                     "startRowIndex" : startRow,
    #                     "endRowIndex" : endRow,
    #                     "startColumnIndex" : startColumn,
    #                     "endColumnIndex" : endColumn
    #                 },
    #         }
            
    #     }

    def delete_range(sheetId, startRow, endRow, startColumn, endColumn, dimensionEnum):
        return {
            "deleteRange" : {
                "range" : {
                    "sheetId" : sheetId,
                    "startRowIndex" : startRow,
                    "endRowIndex" : endRow,
                    "startColumnIndex" : startColumn,
                    "endColumnIndex" : endColumn,
                },
                "shiftDimension" : dimensionEnum
            }
        }

    def insert_row_or_column_body(sheetId, dimension, start, end, inherit=False):
        return {
            "insertDimension" : {
                "range" : {
                    "sheetId" : sheetId,
                    "dimension" : dimension,
                    "startIndex" : start,
                    "endIndex" : end
                },
                "inheritFromBefore" : inherit
            }
        }

    #https://developers.google.com/sheets/api/guides/values
    def get_values(spreadsheet_id, range_name):
        """
        Creates the batch_update the user has access to.
        Load pre-authorized user credentials from the environment.
        TODO(developer) - See https://developers.google.com/identity
        for guides on implementing OAuth2 for the application.\n"
            """
        
        # pylint: disable=maybe-no-member
        try:
            result = sheets.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id, range=range_name).execute()
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
        for guides on implementing OAuth2 for the application.\n"
            """
        # pylint: disable=maybe-no-member
        try:
            
            body = {
                'values': _values
            }
            result = sheets.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id, range=range_name,
                valueInputOption=value_input_option, body=body).execute()
            return result
        except HttpError as error:
            print(f"An error occurred: {error}")
            return error

    def append_values(spreadsheet_id, range_name, dim, value_input_option, _values):
        try:
            
            body = {
                "majorDimension" : dim,
                'values': _values
            }
            result = sheets.spreadsheets().values().append(
                spreadsheetId=spreadsheet_id, range=range_name,
                valueInputOption=value_input_option, body=body).execute()
            return result
        except HttpError as error:
            print(f"An error occurred: {error}")
            return error

    response = drive.files().list(q="parents in '%s'" % dashboards_folder_id,
                                    spaces='drive',
                                    fields='nextPageToken, files(id, name)',
                                    pageToken=None).execute()



    # response = drive.files().list(q="parents in '%s'" % dashboards_folder_id,
    #                                 spaces='drive',
    #                                 fields='nextPageToken, files(id, name)',
    #                                 pageToken=None).execute()

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

    quarterly_merge_ranges = [
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
    grand_yearly_merge_range = [[x+1 for x in l] for l in yearly_merge_ranges]

    for dept in dept_dashboard_list:
        if dept != "Store Wide Dashboard":
            workbook = [x for x in response['files'] if x['name'] == dept][0]['id']
            for period in [all_daily, all_weekly, all_monthly, all_yearly]:
                print(dept + " " + period.name)        
                if dept != 'Taxable Dashboard' and dept != "Store Wide Dashboard":
                    dept_data = period[period['Dept'] == (dept.split(' ')[0] + " Department")]
                elif dept == "Taxable Dashboard":
                    dept_data = period[period['Dept'] == "Taxable Grocery"]
                elif dept == "Store Wide Dashboard":
                    dept_data = period[period['Dept'] == "Store Wide"]
                
                dept_data = dept_data.drop(['Dept'], axis=1)

                dept_data['RevPlan'] = ''
                dept_data['RevPercentPlan'] = ''
                dept_data['COGSPlan'] = ''
                dept_data['COGSPercentPlan'] = ''
                dept_data['MARGINPlan'] = ''
                dept_data['MARGINPercentPlan'] = ''
                dept_data['ProfitPlan'] = ''
                dept_data['ProfitPercentPlan'] = ''

                match period.name:
                    case "Daily" :
                        dept_data = dept_data[[
                            'Day', 'Revenue', 'RevPlan', 'RevPercentPlan', 'LW Revenue', 'Ninety_Trailing_Revenue', 'LY REVENUE', 'REVENUE_GROWTH',
                            'Cost', 'COGSPlan', 'COGSPercentPlan', 'LW Cost', 'Ninety_Trailing_Cost', 'LY COST', 'COST_GROWTH',
                            'Margin', 'MARGINPlan', 'MARGINPercentPlan', 'LW Margin', 'Ninety_Trailing_Margin', 'LY MARGIN', 'YoY Diff',
                            'Profit', 'ProfitPlan', 'ProfitPercentPlan', 'LW Profit', 'Ninety_Trailing_Profit', 'LY PROFIT', 'PROFIT_GROWTH',
                            'Volume', 'LW Volume', 'Ninety_Trailing_Volume', 'LY VOLUME', 'VOLUME_GROWTH'
                            ]]
                    case "Weekly" | "Monthly" :
                        dept_data = dept_data[[
                            'Day', 'Revenue', 'RevPlan', 'RevPercentPlan', 'Ninety_Trailing_Revenue', 'LY REVENUE', 'REVENUE_GROWTH',
                            'Cost', 'COGSPlan', 'COGSPercentPlan', 'Ninety_Trailing_Cost', 'LY COST', 'COST_GROWTH',
                            'Margin', 'MARGINPlan', 'MARGINPercentPlan', 'Ninety_Trailing_Margin', 'LY MARGIN', 'YoY Diff',
                            'Profit', 'ProfitPlan', 'ProfitPercentPlan', 'Ninety_Trailing_Profit', 'LY PROFIT', 'PROFIT_GROWTH',
                            'Volume', 'Ninety_Trailing_Volume', 'LY VOLUME', 'VOLUME_GROWTH'
                            ]]
                    case "Yearly" :
                        dept_data = dept_data[[
                            'Day', 'Revenue', 'RevPlan', 'RevPercentPlan', 'LY REVENUE', 'REVENUE_GROWTH',
                            'Cost', 'COGSPlan', 'COGSPercentPlan', 'LY COST', 'COST_GROWTH',
                            'Margin', 'MARGINPlan', 'MARGINPercentPlan', 'LY MARGIN', 'YoY Diff',
                            'Profit', 'ProfitPlan', 'ProfitPercentPlan', 'LY PROFIT', 'PROFIT_GROWTH',
                            'Volume', 'LY VOLUME', 'VOLUME_GROWTH'
                            ]]
                # print(dept_data)
                # print(period.name)
                # print(get_values(workbook, period.name + '!A3')[0][0] == dept_data['Day'].values.tolist()[0])
                # print(get_values(workbook, period.name + '!A3')[0][0])
                # print(dept_data['Day'].values.tolist()[0])
                sheetId = [x for x in sheets.spreadsheets().get(spreadsheetId=workbook).execute().get('sheets', '')
                                if x.get('properties', '').get('title', '') == period.name][0].get('properties', '').get('sheetId', '')
                f = open("exception_log_dash.txt", "a")
                f.write("\n" + dept)
                f.write(str(dept_data.values.tolist()[1]))
                f.close()
                if(get_values(workbook, period.name + '!A3')[0][0] == dept_data['Day'].values.tolist()[0]):
                    update_values(workbook, range_name=period.name + '!A3', value_input_option="RAW", _values = [dept_data.values.tolist()[0]])
                else:
                    batchUpdate(workbookId=workbook, bodyList=[insert_row_or_column_body(sheetId=sheetId, dimension="ROWS", start=2, end=3)])
                    update_values(workbook, range_name=period.name + '!A4', value_input_option="RAW", _values = [dept_data.values.tolist()[1]])
                    match period.name:
                        case "Daily":
                            batchUpdate(workbookId=workbook, bodyList=[
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : 1,
                                            "endColumnIndex" : daily_merge_ranges[9][1]
                                        },
                                        "cell" : {
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
                                        "fields" : """
                                                userEnteredFormat.textFormat.fontFamily,
                                                userEnteredFormat.textFormat.fontSize,
                                                userEnteredFormat.borders.top,
                                                userEnteredFormat.borders.bottom,
                                                userEnteredFormat.borders.left,
                                                userEnteredFormat.borders.right"""
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : 0,
                                            "endColumnIndex" : 1
                                        },
                                        "cell" : {
                                            "userEnteredFormat":{
                                                "borders":{
                                                    "top": {"style" : "SOLID_THICK"},
                                                    "bottom":{"style" : "SOLID_THICK"},
                                                    "left":{"style" : "SOLID_THICK"},
                                                    "right":{"style" : "SOLID_THICK"}
                                                    },
                                                "textFormat": { "fontFamily" : "Arial", "fontSize" : 12, "bold" : True}
                                            },
                                        },
                                        "fields" : """
                                                userEnteredFormat.textFormat.bold,
                                                userEnteredFormat.textFormat.fontFamily,
                                                userEnteredFormat.textFormat.fontSize,
                                                userEnteredFormat.borders.top,
                                                userEnteredFormat.borders.bottom,
                                                userEnteredFormat.borders.left,
                                                userEnteredFormat.borders.right"""
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : daily_merge_ranges[0][1]-1,
                                            "endColumnIndex" : daily_merge_ranges[0][1]
                                        },
                                        "cell" : {"userEnteredFormat":{ "borders":{ "right":{"style" : "SOLID_THICK"}}}},
                                        "fields" : """userEnteredFormat.borders.right"""
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : daily_merge_ranges[1][1]-1,
                                            "endColumnIndex" : daily_merge_ranges[1][1]
                                        },
                                        "cell" : {"userEnteredFormat":{ "borders":{ "right":{"style" : "SOLID_THICK"}}}},
                                        "fields" : """userEnteredFormat.borders.right"""
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : daily_merge_ranges[2][1]-1,
                                            "endColumnIndex" : daily_merge_ranges[2][1]
                                        },
                                        "cell" : {"userEnteredFormat":{ "borders":{ "right":{"style" : "SOLID_THICK"}}}},
                                        "fields" : """userEnteredFormat.borders.right"""
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : daily_merge_ranges[3][1]-1,
                                            "endColumnIndex" : daily_merge_ranges[3][1]
                                        },
                                        "cell" : {"userEnteredFormat":{ "borders":{ "right":{"style" : "SOLID_THICK"}}}},
                                        "fields" : """userEnteredFormat.borders.right"""
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : daily_merge_ranges[4][1]-1,
                                            "endColumnIndex" : daily_merge_ranges[4][1]
                                        },
                                        "cell" : {"userEnteredFormat":{ "borders":{ "right":{"style" : "SOLID_THICK"}}}},
                                        "fields" : """userEnteredFormat.borders.right"""
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : daily_merge_ranges[5][1]-1,
                                            "endColumnIndex" : daily_merge_ranges[5][1]
                                        },
                                        "cell" : {"userEnteredFormat":{ "borders":{ "right":{"style" : "SOLID_THICK"}}}},
                                        "fields" : """userEnteredFormat.borders.right"""
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : daily_merge_ranges[6][1]-1,
                                            "endColumnIndex" : daily_merge_ranges[6][1]
                                        },
                                        "cell" : {"userEnteredFormat":{ "borders":{ "right":{"style" : "SOLID_THICK"}}}},
                                        "fields" : """userEnteredFormat.borders.right"""
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : daily_merge_ranges[7][1]-1,
                                            "endColumnIndex" : daily_merge_ranges[7][1]
                                        },
                                        "cell" : {"userEnteredFormat":{ "borders":{ "right":{"style" : "SOLID_THICK"}}}},
                                        "fields" : """userEnteredFormat.borders.right"""
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : daily_merge_ranges[8][1]-1,
                                            "endColumnIndex" : daily_merge_ranges[8][1]
                                        },
                                        "cell" : {"userEnteredFormat":{ "borders":{ "right":{"style" : "SOLID_THICK"}}}},
                                        "fields" : """userEnteredFormat.borders.right"""
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : daily_merge_ranges[9][1]-1,
                                            "endColumnIndex" : daily_merge_ranges[9][1]
                                        },
                                        "cell" : {"userEnteredFormat":{ "borders":{ "right":{"style" : "SOLID_THICK"}}}},
                                        "fields" : """userEnteredFormat.borders.right"""
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : daily_merge_ranges[0][0],
                                            "endColumnIndex" : daily_merge_ranges[0][0]+2
                                        },
                                        "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "CURRENCY", "pattern" : "$#,##0.00"}}},
                                        "fields" : """userEnteredFormat.numberFormat.type,
                                                        userEnteredFormat.numberFormat.pattern
                                                """
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : daily_merge_ranges[0][1]+1,
                                            "endColumnIndex" : daily_merge_ranges[0][1]+2
                                        },
                                        "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "PERCENT", "pattern" : "0.00%"}}},
                                        "fields" : """userEnteredFormat.numberFormat.type,
                                                        userEnteredFormat.numberFormat.pattern
                                                """
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : daily_merge_ranges[0][0]+3,
                                            "endColumnIndex" : daily_merge_ranges[0][1]-1
                                        },
                                        "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "CURRENCY", "pattern" : "$#,##0.00"}}},
                                        "fields" : """userEnteredFormat.numberFormat.type,
                                                        userEnteredFormat.numberFormat.pattern
                                                """
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : daily_merge_ranges[0][1]-1,
                                            "endColumnIndex" : daily_merge_ranges[0][1]
                                        },
                                        "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "PERCENT", "pattern" : "0.00%"}}},
                                        "fields" : """userEnteredFormat.numberFormat.type,
                                                        userEnteredFormat.numberFormat.pattern
                                                """
                                    }
                                },
                                #COGS
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : daily_merge_ranges[1][0],
                                            "endColumnIndex" : daily_merge_ranges[1][0]+2
                                        },
                                        "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "CURRENCY", "pattern" : "$#,##0.00"}}},
                                        "fields" : """userEnteredFormat.numberFormat.type,
                                                        userEnteredFormat.numberFormat.pattern
                                                """
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : daily_merge_ranges[1][1]+1,
                                            "endColumnIndex" : daily_merge_ranges[1][1]+2
                                        },
                                        "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "PERCENT", "pattern" : "0.00%"}}},
                                        "fields" : """userEnteredFormat.numberFormat.type,
                                                        userEnteredFormat.numberFormat.pattern
                                                """
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : daily_merge_ranges[1][0]+3,
                                            "endColumnIndex" : daily_merge_ranges[1][1]-1
                                        },
                                        "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "CURRENCY", "pattern" : "$#,##0.00"}}},
                                        "fields" : """userEnteredFormat.numberFormat.type,
                                                        userEnteredFormat.numberFormat.pattern
                                                """
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : daily_merge_ranges[1][1]-1,
                                            "endColumnIndex" : daily_merge_ranges[1][1]
                                        },
                                        "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "PERCENT", "pattern" : "0.00%"}}},
                                        "fields" : """userEnteredFormat.numberFormat.type,
                                                        userEnteredFormat.numberFormat.pattern
                                                """
                                    }
                                },
                                #Margins
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : daily_merge_ranges[2][0],
                                            "endColumnIndex" : daily_merge_ranges[2][1]+1
                                        },
                                        "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "PERCENT", "pattern" : "0.00%"}}},
                                        "fields" : """userEnteredFormat.numberFormat.type,
                                                        userEnteredFormat.numberFormat.pattern
                                                """
                                    }
                                },
                                #Profit
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : daily_merge_ranges[3][0],
                                            "endColumnIndex" : daily_merge_ranges[3][0]+2
                                        },
                                        "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "CURRENCY", "pattern" : "$#,##0.00"}}},
                                        "fields" : """userEnteredFormat.numberFormat.type,
                                                        userEnteredFormat.numberFormat.pattern
                                                """
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : daily_merge_ranges[3][1]+1,
                                            "endColumnIndex" : daily_merge_ranges[3][1]+2
                                        },
                                        "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "PERCENT", "pattern" : "0.00%"}}},
                                        "fields" : """userEnteredFormat.numberFormat.type,
                                                        userEnteredFormat.numberFormat.pattern
                                                """
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : daily_merge_ranges[3][0]+3,
                                            "endColumnIndex" : daily_merge_ranges[3][1]-1
                                        },
                                        "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "CURRENCY", "pattern" : "$#,##0.00"}}},
                                        "fields" : """userEnteredFormat.numberFormat.type,
                                                        userEnteredFormat.numberFormat.pattern
                                                """
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : daily_merge_ranges[3][1]-1,
                                            "endColumnIndex" : daily_merge_ranges[3][1]
                                        },
                                        "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "PERCENT", "pattern" : "0.00%"}}},
                                        "fields" : """userEnteredFormat.numberFormat.type,
                                                        userEnteredFormat.numberFormat.pattern
                                                """
                                    }
                                },
                                #Volume
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : daily_merge_ranges[4][0],
                                            "endColumnIndex" : daily_merge_ranges[4][0]+2
                                        },
                                        "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "NUMBER", "pattern" : "#,##0"}}},
                                        "fields" : """userEnteredFormat.numberFormat.type,
                                                        userEnteredFormat.numberFormat.pattern
                                                """
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : daily_merge_ranges[4][1]-1,
                                            "endColumnIndex" : daily_merge_ranges[4][1]
                                        },
                                        "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "PERCENT", "pattern" : "0.00%"}}},
                                        "fields" : """userEnteredFormat.numberFormat.type,
                                                        userEnteredFormat.numberFormat.pattern
                                                """
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : 0,
                                            "endColumnIndex" : 1
                                        },
                                        "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "DATE", "pattern" : "d/m/yy"}}},
                                        "fields" : """userEnteredFormat.numberFormat.type,
                                                        userEnteredFormat.numberFormat.pattern
                                                """
                                    }
                                }])
                        case "Weekly" | "Monthly":
                            batchUpdate(workbookId=workbook, bodyList=[
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : 1,
                                            "endColumnIndex" : weekly_monthly_merge_ranges[9][1]
                                        },
                                        "cell" : {
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
                                        "fields" : """
                                                userEnteredFormat.textFormat.fontFamily,
                                                userEnteredFormat.textFormat.fontSize,
                                                userEnteredFormat.borders.top,
                                                userEnteredFormat.borders.bottom,
                                                userEnteredFormat.borders.left,
                                                userEnteredFormat.borders.right"""
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : 0,
                                            "endColumnIndex" : 1
                                        },
                                        "cell" : {
                                            "userEnteredFormat":{
                                                "borders":{
                                                    "top": {"style" : "SOLID_THICK"},
                                                    "bottom":{"style" : "SOLID_THICK"},
                                                    "left":{"style" : "SOLID_THICK"},
                                                    "right":{"style" : "SOLID_THICK"}
                                                    },
                                                "textFormat": { "fontFamily" : "Arial", "fontSize" : 12, "bold" : True}
                                            },
                                        },
                                        "fields" : """
                                                userEnteredFormat.textFormat.bold,
                                                userEnteredFormat.textFormat.fontFamily,
                                                userEnteredFormat.textFormat.fontSize,
                                                userEnteredFormat.borders.top,
                                                userEnteredFormat.borders.bottom,
                                                userEnteredFormat.borders.left,
                                                userEnteredFormat.borders.right"""
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : weekly_monthly_merge_ranges[0][1]-1,
                                            "endColumnIndex" : weekly_monthly_merge_ranges[0][1]
                                        },
                                        "cell" : {"userEnteredFormat":{ "borders":{ "right":{"style" : "SOLID_THICK"}}}},
                                        "fields" : """userEnteredFormat.borders.right"""
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : weekly_monthly_merge_ranges[1][1]-1,
                                            "endColumnIndex" : weekly_monthly_merge_ranges[1][1]
                                        },
                                        "cell" : {"userEnteredFormat":{ "borders":{ "right":{"style" : "SOLID_THICK"}}}},
                                        "fields" : """userEnteredFormat.borders.right"""
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : weekly_monthly_merge_ranges[2][1]-1,
                                            "endColumnIndex" : weekly_monthly_merge_ranges[2][1]
                                        },
                                        "cell" : {"userEnteredFormat":{ "borders":{ "right":{"style" : "SOLID_THICK"}}}},
                                        "fields" : """userEnteredFormat.borders.right"""
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : weekly_monthly_merge_ranges[3][1]-1,
                                            "endColumnIndex" : weekly_monthly_merge_ranges[3][1]
                                        },
                                        "cell" : {"userEnteredFormat":{ "borders":{ "right":{"style" : "SOLID_THICK"}}}},
                                        "fields" : """userEnteredFormat.borders.right"""
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : weekly_monthly_merge_ranges[4][1]-1,
                                            "endColumnIndex" : weekly_monthly_merge_ranges[4][1]
                                        },
                                        "cell" : {"userEnteredFormat":{ "borders":{ "right":{"style" : "SOLID_THICK"}}}},
                                        "fields" : """userEnteredFormat.borders.right"""
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : weekly_monthly_merge_ranges[5][1]-1,
                                            "endColumnIndex" : weekly_monthly_merge_ranges[5][1]
                                        },
                                        "cell" : {"userEnteredFormat":{ "borders":{ "right":{"style" : "SOLID_THICK"}}}},
                                        "fields" : """userEnteredFormat.borders.right"""
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : weekly_monthly_merge_ranges[6][1]-1,
                                            "endColumnIndex" : weekly_monthly_merge_ranges[6][1]
                                        },
                                        "cell" : {"userEnteredFormat":{ "borders":{ "right":{"style" : "SOLID_THICK"}}}},
                                        "fields" : """userEnteredFormat.borders.right"""
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : weekly_monthly_merge_ranges[7][1]-1,
                                            "endColumnIndex" : weekly_monthly_merge_ranges[7][1]
                                        },
                                        "cell" : {"userEnteredFormat":{ "borders":{ "right":{"style" : "SOLID_THICK"}}}},
                                        "fields" : """userEnteredFormat.borders.right"""
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : weekly_monthly_merge_ranges[8][1]-1,
                                            "endColumnIndex" : weekly_monthly_merge_ranges[8][1]
                                        },
                                        "cell" : {"userEnteredFormat":{ "borders":{ "right":{"style" : "SOLID_THICK"}}}},
                                        "fields" : """userEnteredFormat.borders.right"""
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : weekly_monthly_merge_ranges[9][1]-1,
                                            "endColumnIndex" : weekly_monthly_merge_ranges[9][1]
                                        },
                                        "cell" : {"userEnteredFormat":{ "borders":{ "right":{"style" : "SOLID_THICK"}}}},
                                        "fields" : """userEnteredFormat.borders.right"""
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : weekly_monthly_merge_ranges[0][0],
                                            "endColumnIndex" : weekly_monthly_merge_ranges[0][0]+2
                                        },
                                        "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "CURRENCY", "pattern" : "$#,##0.00"}}},
                                        "fields" : """userEnteredFormat.numberFormat.type,
                                                        userEnteredFormat.numberFormat.pattern
                                                """
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : weekly_monthly_merge_ranges[0][1]+1,
                                            "endColumnIndex" : weekly_monthly_merge_ranges[0][1]+2
                                        },
                                        "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "PERCENT", "pattern" : "0.00%"}}},
                                        "fields" : """userEnteredFormat.numberFormat.type,
                                                        userEnteredFormat.numberFormat.pattern
                                                """
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : weekly_monthly_merge_ranges[0][0]+3,
                                            "endColumnIndex" : weekly_monthly_merge_ranges[0][1]-1
                                        },
                                        "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "CURRENCY", "pattern" : "$#,##0.00"}}},
                                        "fields" : """userEnteredFormat.numberFormat.type,
                                                        userEnteredFormat.numberFormat.pattern
                                                """
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : weekly_monthly_merge_ranges[0][1]-1,
                                            "endColumnIndex" : weekly_monthly_merge_ranges[0][1]
                                        },
                                        "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "PERCENT", "pattern" : "0.00%"}}},
                                        "fields" : """userEnteredFormat.numberFormat.type,
                                                        userEnteredFormat.numberFormat.pattern
                                                """
                                    }
                                },
                                #COGS
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : weekly_monthly_merge_ranges[1][0],
                                            "endColumnIndex" : weekly_monthly_merge_ranges[1][0]+2
                                        },
                                        "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "CURRENCY", "pattern" : "$#,##0.00"}}},
                                        "fields" : """userEnteredFormat.numberFormat.type,
                                                        userEnteredFormat.numberFormat.pattern
                                                """
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : weekly_monthly_merge_ranges[1][1]+1,
                                            "endColumnIndex" : weekly_monthly_merge_ranges[1][1]+2
                                        },
                                        "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "PERCENT", "pattern" : "0.00%"}}},
                                        "fields" : """userEnteredFormat.numberFormat.type,
                                                        userEnteredFormat.numberFormat.pattern
                                                """
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : weekly_monthly_merge_ranges[1][0]+3,
                                            "endColumnIndex" : weekly_monthly_merge_ranges[1][1]-1
                                        },
                                        "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "CURRENCY", "pattern" : "$#,##0.00"}}},
                                        "fields" : """userEnteredFormat.numberFormat.type,
                                                        userEnteredFormat.numberFormat.pattern
                                                """
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : weekly_monthly_merge_ranges[1][1]-1,
                                            "endColumnIndex" : weekly_monthly_merge_ranges[1][1]
                                        },
                                        "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "PERCENT", "pattern" : "0.00%"}}},
                                        "fields" : """userEnteredFormat.numberFormat.type,
                                                        userEnteredFormat.numberFormat.pattern
                                                """
                                    }
                                },
                                #Margins
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : weekly_monthly_merge_ranges[2][0],
                                            "endColumnIndex" : weekly_monthly_merge_ranges[2][1]+1
                                        },
                                        "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "PERCENT", "pattern" : "0.00%"}}},
                                        "fields" : """userEnteredFormat.numberFormat.type,
                                                        userEnteredFormat.numberFormat.pattern
                                                """
                                    }
                                },
                                #Profit
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : weekly_monthly_merge_ranges[3][0],
                                            "endColumnIndex" : weekly_monthly_merge_ranges[3][0]+2
                                        },
                                        "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "CURRENCY", "pattern" : "$#,##0.00"}}},
                                        "fields" : """userEnteredFormat.numberFormat.type,
                                                        userEnteredFormat.numberFormat.pattern
                                                """
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : weekly_monthly_merge_ranges[3][1]+1,
                                            "endColumnIndex" : weekly_monthly_merge_ranges[3][1]+2
                                        },
                                        "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "PERCENT", "pattern" : "0.00%"}}},
                                        "fields" : """userEnteredFormat.numberFormat.type,
                                                        userEnteredFormat.numberFormat.pattern
                                                """
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : weekly_monthly_merge_ranges[3][0]+3,
                                            "endColumnIndex" : weekly_monthly_merge_ranges[3][1]-1
                                        },
                                        "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "CURRENCY", "pattern" : "$#,##0.00"}}},
                                        "fields" : """userEnteredFormat.numberFormat.type,
                                                        userEnteredFormat.numberFormat.pattern
                                                """
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : weekly_monthly_merge_ranges[3][1]-1,
                                            "endColumnIndex" : weekly_monthly_merge_ranges[3][1]
                                        },
                                        "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "PERCENT", "pattern" : "0.00%"}}},
                                        "fields" : """userEnteredFormat.numberFormat.type,
                                                        userEnteredFormat.numberFormat.pattern
                                                """
                                    }
                                },
                                #Volume
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : weekly_monthly_merge_ranges[4][0],
                                            "endColumnIndex" : weekly_monthly_merge_ranges[4][0]+2
                                        },
                                        "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "NUMBER", "pattern" : "#,##0"}}},
                                        "fields" : """userEnteredFormat.numberFormat.type,
                                                        userEnteredFormat.numberFormat.pattern
                                                """
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : weekly_monthly_merge_ranges[4][1]-1,
                                            "endColumnIndex" : weekly_monthly_merge_ranges[4][1]
                                        },
                                        "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "PERCENT", "pattern" : "0.00%"}}},
                                        "fields" : """userEnteredFormat.numberFormat.type,
                                                        userEnteredFormat.numberFormat.pattern
                                                """
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : 0,
                                            "endColumnIndex" : 1
                                        },
                                        "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "DATE", "pattern" : "d/m/yy"}}},
                                        "fields" : """userEnteredFormat.numberFormat.type,
                                                        userEnteredFormat.numberFormat.pattern
                                                """
                                        } 
                                }
                            ])
                        case "Yearly":
                            batchUpdate(workbookId=workbook, bodyList=[
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : 1,
                                            "endColumnIndex" : yearly_merge_ranges[9][1]
                                        },
                                        "cell" : {
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
                                        "fields" : """
                                                userEnteredFormat.textFormat.fontFamily,
                                                userEnteredFormat.textFormat.fontSize,
                                                userEnteredFormat.borders.top,
                                                userEnteredFormat.borders.bottom,
                                                userEnteredFormat.borders.left,
                                                userEnteredFormat.borders.right"""
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : 0,
                                            "endColumnIndex" : 1
                                        },
                                        "cell" : {
                                            "userEnteredFormat":{
                                                "borders":{
                                                    "top": {"style" : "SOLID_THICK"},
                                                    "bottom":{"style" : "SOLID_THICK"},
                                                    "left":{"style" : "SOLID_THICK"},
                                                    "right":{"style" : "SOLID_THICK"}
                                                    },
                                                "textFormat": { "fontFamily" : "Arial", "fontSize" : 12, "bold" : True}
                                            },
                                        },
                                        "fields" : """
                                                userEnteredFormat.textFormat.bold,
                                                userEnteredFormat.textFormat.fontFamily,
                                                userEnteredFormat.textFormat.fontSize,
                                                userEnteredFormat.borders.top,
                                                userEnteredFormat.borders.bottom,
                                                userEnteredFormat.borders.left,
                                                userEnteredFormat.borders.right"""
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : yearly_merge_ranges[0][1]-1,
                                            "endColumnIndex" : yearly_merge_ranges[0][1]
                                        },
                                        "cell" : {"userEnteredFormat":{ "borders":{ "right":{"style" : "SOLID_THICK"}}}},
                                        "fields" : """userEnteredFormat.borders.right"""
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : yearly_merge_ranges[1][1]-1,
                                            "endColumnIndex" : yearly_merge_ranges[1][1]
                                        },
                                        "cell" : {"userEnteredFormat":{ "borders":{ "right":{"style" : "SOLID_THICK"}}}},
                                        "fields" : """userEnteredFormat.borders.right"""
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : yearly_merge_ranges[2][1]-1,
                                            "endColumnIndex" : yearly_merge_ranges[2][1]
                                        },
                                        "cell" : {"userEnteredFormat":{ "borders":{ "right":{"style" : "SOLID_THICK"}}}},
                                        "fields" : """userEnteredFormat.borders.right"""
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : yearly_merge_ranges[3][1]-1,
                                            "endColumnIndex" : yearly_merge_ranges[3][1]
                                        },
                                        "cell" : {"userEnteredFormat":{ "borders":{ "right":{"style" : "SOLID_THICK"}}}},
                                        "fields" : """userEnteredFormat.borders.right"""
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : yearly_merge_ranges[4][1]-1,
                                            "endColumnIndex" : yearly_merge_ranges[4][1]
                                        },
                                        "cell" : {"userEnteredFormat":{ "borders":{ "right":{"style" : "SOLID_THICK"}}}},
                                        "fields" : """userEnteredFormat.borders.right"""
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : yearly_merge_ranges[5][1]-1,
                                            "endColumnIndex" : yearly_merge_ranges[5][1]
                                        },
                                        "cell" : {"userEnteredFormat":{ "borders":{ "right":{"style" : "SOLID_THICK"}}}},
                                        "fields" : """userEnteredFormat.borders.right"""
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : yearly_merge_ranges[6][1]-1,
                                            "endColumnIndex" : yearly_merge_ranges[6][1]
                                        },
                                        "cell" : {"userEnteredFormat":{ "borders":{ "right":{"style" : "SOLID_THICK"}}}},
                                        "fields" : """userEnteredFormat.borders.right"""
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : yearly_merge_ranges[7][1]-1,
                                            "endColumnIndex" : yearly_merge_ranges[7][1]
                                        },
                                        "cell" : {"userEnteredFormat":{ "borders":{ "right":{"style" : "SOLID_THICK"}}}},
                                        "fields" : """userEnteredFormat.borders.right"""
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : yearly_merge_ranges[8][1]-1,
                                            "endColumnIndex" : yearly_merge_ranges[8][1]
                                        },
                                        "cell" : {"userEnteredFormat":{ "borders":{ "right":{"style" : "SOLID_THICK"}}}},
                                        "fields" : """userEnteredFormat.borders.right"""
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : yearly_merge_ranges[9][1]-1,
                                            "endColumnIndex" : yearly_merge_ranges[9][1]
                                        },
                                        "cell" : {"userEnteredFormat":{ "borders":{ "right":{"style" : "SOLID_THICK"}}}},
                                        "fields" : """userEnteredFormat.borders.right"""
                                    }
                                },
                                #Sales
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : yearly_merge_ranges[0][0],
                                            "endColumnIndex" : yearly_merge_ranges[0][0]+2
                                        },
                                        "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "CURRENCY", "pattern" : "$#,##0.00"}}},
                                        "fields" : """userEnteredFormat.numberFormat.type,
                                                        userEnteredFormat.numberFormat.pattern
                                                """
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : yearly_merge_ranges[0][1]+1,
                                            "endColumnIndex" : yearly_merge_ranges[0][1]+2
                                        },
                                        "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "PERCENT", "pattern" : "0.00%"}}},
                                        "fields" : """userEnteredFormat.numberFormat.type,
                                                        userEnteredFormat.numberFormat.pattern
                                                """
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : yearly_merge_ranges[0][0]+3,
                                            "endColumnIndex" : yearly_merge_ranges[0][1]-1
                                        },
                                        "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "CURRENCY", "pattern" : "$#,##0.00"}}},
                                        "fields" : """userEnteredFormat.numberFormat.type,
                                                        userEnteredFormat.numberFormat.pattern
                                                """
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : yearly_merge_ranges[0][1]-1,
                                            "endColumnIndex" : yearly_merge_ranges[0][1]
                                        },
                                        "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "PERCENT", "pattern" : "0.00%"}}},
                                        "fields" : """userEnteredFormat.numberFormat.type,
                                                        userEnteredFormat.numberFormat.pattern
                                                """
                                    }
                                },
                                #COGS
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : yearly_merge_ranges[1][0],
                                            "endColumnIndex" : yearly_merge_ranges[1][0]+2
                                        },
                                        "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "CURRENCY", "pattern" : "$#,##0.00"}}},
                                        "fields" : """userEnteredFormat.numberFormat.type,
                                                        userEnteredFormat.numberFormat.pattern
                                                """
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : yearly_merge_ranges[1][1]+1,
                                            "endColumnIndex" : yearly_merge_ranges[1][1]+2
                                        },
                                        "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "PERCENT", "pattern" : "0.00%"}}},
                                        "fields" : """userEnteredFormat.numberFormat.type,
                                                        userEnteredFormat.numberFormat.pattern
                                                """
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : yearly_merge_ranges[1][0]+3,
                                            "endColumnIndex" : yearly_merge_ranges[1][1]-1
                                        },
                                        "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "CURRENCY", "pattern" : "$#,##0.00"}}},
                                        "fields" : """userEnteredFormat.numberFormat.type,
                                                        userEnteredFormat.numberFormat.pattern
                                                """
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : yearly_merge_ranges[1][1]-1,
                                            "endColumnIndex" : yearly_merge_ranges[1][1]
                                        },
                                        "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "PERCENT", "pattern" : "0.00%"}}},
                                        "fields" : """userEnteredFormat.numberFormat.type,
                                                        userEnteredFormat.numberFormat.pattern
                                                """
                                    }
                                },
                                #Margins
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : yearly_merge_ranges[2][0],
                                            "endColumnIndex" : yearly_merge_ranges[2][1]+1
                                        },
                                        "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "PERCENT", "pattern" : "0.00%"}}},
                                        "fields" : """userEnteredFormat.numberFormat.type,
                                                        userEnteredFormat.numberFormat.pattern
                                                """
                                    }
                                },
                                #Profit
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : yearly_merge_ranges[3][0],
                                            "endColumnIndex" : yearly_merge_ranges[3][0]+2
                                        },
                                        "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "CURRENCY", "pattern" : "$#,##0.00"}}},
                                        "fields" : """userEnteredFormat.numberFormat.type,
                                                        userEnteredFormat.numberFormat.pattern
                                                """
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : yearly_merge_ranges[3][1]+1,
                                            "endColumnIndex" : yearly_merge_ranges[3][1]+2
                                        },
                                        "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "PERCENT", "pattern" : "0.00%"}}},
                                        "fields" : """userEnteredFormat.numberFormat.type,
                                                        userEnteredFormat.numberFormat.pattern
                                                """
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : yearly_merge_ranges[3][0]+3,
                                            "endColumnIndex" : yearly_merge_ranges[3][1]-1
                                        },
                                        "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "CURRENCY", "pattern" : "$#,##0.00"}}},
                                        "fields" : """userEnteredFormat.numberFormat.type,
                                                        userEnteredFormat.numberFormat.pattern
                                                """
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : yearly_merge_ranges[3][1]-1,
                                            "endColumnIndex" : yearly_merge_ranges[3][1]
                                        },
                                        "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "PERCENT", "pattern" : "0.00%"}}},
                                        "fields" : """userEnteredFormat.numberFormat.type,
                                                        userEnteredFormat.numberFormat.pattern
                                                """
                                    }
                                },
                                #Volume
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : yearly_merge_ranges[4][0],
                                            "endColumnIndex" : yearly_merge_ranges[4][0]+2
                                        },
                                        "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "NUMBER", "pattern" : "#,##0"}}},
                                        "fields" : """userEnteredFormat.numberFormat.type,
                                                        userEnteredFormat.numberFormat.pattern
                                                """
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : yearly_merge_ranges[4][1]-1,
                                            "endColumnIndex" : yearly_merge_ranges[4][1]
                                        },
                                        "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "PERCENT", "pattern" : "0.00%"}}},
                                        "fields" : """userEnteredFormat.numberFormat.type,
                                                        userEnteredFormat.numberFormat.pattern
                                                """
                                    }
                                },
                                {
                                    "repeatCell" :{
                                        "range" : {
                                            "sheetId" : sheetId,
                                            "startRowIndex" : 2,
                                            "endRowIndex" : 3,
                                            "startColumnIndex" : 0,
                                            "endColumnIndex" : 1
                                        },
                                        "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "DATE", "pattern" : "d/m/yy"}}},
                                        "fields" : """userEnteredFormat.numberFormat.type,
                                                        userEnteredFormat.numberFormat.pattern
                                                """
                                        } 
                                }
                            ])
                    append_values(workbook, range_name=period.name + '!A3', dim="ROWS",value_input_option="RAW", _values = [dept_data.values.tolist()[0]])
                
            time.sleep(7)          
            if dept == "Store Wide Dashboard":
                for period in [all_daily, all_weekly, all_monthly, all_yearly]:
                    data = period
                    data['RevPlan'] = ''
                    data['RevPercentPlan'] = ''
                    data['COGSPlan'] = ''
                    data['COGSPercentPlan'] = ''
                    data['MARGINPlan'] = ''
                    data['MARGINPercentPlan'] = ''
                    data['ProfitPlan'] = ''
                    data['ProfitPercentPlan'] = ''

                    match period.name:
                        case "Daily" :
                            data = data[[
                                'Day', 'Dept', 'Revenue', 'RevPlan', 'RevPercentPlan', 'LW Revenue', 'Ninety_Trailing_Revenue', 'LY REVENUE', 'REVENUE_GROWTH',
                                'Cost', 'COGSPlan', 'COGSPercentPlan', 'LW Cost', 'Ninety_Trailing_Cost', 'LY COST', 'COST_GROWTH',
                                'Margin', 'MARGINPlan', 'MARGINPercentPlan', 'LW Margin', 'Ninety_Trailing_Margin', 'LY MARGIN', 'YoY Diff',
                                'Profit', 'ProfitPlan', 'ProfitPercentPlan', 'LW Profit', 'Ninety_Trailing_Profit', 'LY PROFIT', 'PROFIT_GROWTH',
                                'Volume', 'LW Volume', 'Ninety_Trailing_Volume', 'LY VOLUME', 'VOLUME_GROWTH'
                                ]]
                        case "Weekly" | "Monthly" :
                            data = data[[
                                'Day', 'Dept', 'Revenue', 'RevPlan', 'RevPercentPlan', 'Ninety_Trailing_Revenue', 'LY REVENUE', 'REVENUE_GROWTH',
                                'Cost', 'COGSPlan', 'COGSPercentPlan', 'Ninety_Trailing_Cost', 'LY COST', 'COST_GROWTH',
                                'Margin', 'MARGINPlan', 'MARGINPercentPlan', 'Ninety_Trailing_Margin', 'LY MARGIN', 'YoY Diff',
                                'Profit', 'ProfitPlan', 'ProfitPercentPlan', 'Ninety_Trailing_Profit', 'LY PROFIT', 'PROFIT_GROWTH',
                                'Volume', 'Ninety_Trailing_Volume', 'LY VOLUME', 'VOLUME_GROWTH'
                                ]]
                        case "Yearly" :
                            data = data[[
                                'Day', 'Dept', 'Revenue', 'RevPlan', 'RevPercentPlan', 'LY REVENUE', 'REVENUE_GROWTH',
                                'Cost', 'COGSPlan', 'COGSPercentPlan', 'LY COST', 'COST_GROWTH',
                                'Margin', 'MARGINPlan', 'MARGINPercentPlan', 'LY MARGIN', 'YoY Diff',
                                'Profit', 'ProfitPlan', 'ProfitPercentPlan', 'LY PROFIT', 'PROFIT_GROWTH',
                                'Volume', 'LY VOLUME', 'VOLUME_GROWTH'
                                ]]

                    sheetId = [x for x in sheets.spreadsheets().get(spreadsheetId=workbook).execute().get('sheets', '')
                                    if x.get('properties', '').get('title', '') == period.name + " Depts"][0].get('properties', '').get('sheetId', '')
                    if(get_values(workbook, period.name + " Depts" + '!A3')[0][0] == data['Day'].values.tolist()[0]):
                        last_row = 0
                        for i in range(0, 18):
                            print(get_values(workbook, period.name + " Depts" + '!A%s' % (i + 3))[0][0])
                            if(get_values(workbook, period.name + " Depts" + '!A%s' % (i + 3))[0][0] != datetime.datetime.today()):
                                last_row == (i + 3)
                        if(last_row != 0):
                            batchUpdate(workbookId=workbook, bodyList=[delete_range(sheetId=sheetId, dimension="ROWS", startRow = 2, endRow = last_row-1, startColumn=0, endColumn=1)])
                            batchUpdate(workbookId=workbook, bodyList=[insert_row_or_column_body(sheetId=sheetId, dimension="ROWS", start=2, end=data[data['Day'] == datetime.datetime.today()].shape[0])])
                            update_values(workbook, range_name=period.name + " Depts" + '!A3:AF%s' % (last_row - 1), value_input_option="RAW", _values = data[data['Day'] == datetime.datetime.today()].to_numpy().tolist())
                        else:    
                            update_values(workbook, range_name=period.name + " Depts" + '!A3:AF21', value_input_option="RAW", _values = data[data['Day'] == datetime.datetime.today()].to_numpy().tolist())
                    else:
                        print()
                    # print(get_values(workbook, period.name + " Depts" + '!A3')[0][0])
                    # print(data['Day'].values.tolist()[0])
                    # print(get_values(workbook, period.name + " Depts" + '!A3')[0][0] == data['Day'].values.tolist()[0])
                    if(get_values(workbook, period.name + " Depts" + '!A3')[0][0] == data['Day'].values.tolist()[0]):
                        if(period.name == "Daily" or period.name == "Weekly"):
                            last_row = 0
                            for i in range(0, 18):
                                print(get_values(workbook, period.name + " Depts" + '!A%s' % (i + 3))[0][0])
                                if(get_values(workbook, period.name + " Depts" + '!A%s' % (i + 3))[0][0] != datetime.datetime.today()):
                                    last_row == (i + 3)
                            if(last_row != 0):
                                batchUpdate(workbookId=workbook, bodyList=[delete_range(sheetId=sheetId, dimension="ROWS", startRow = 2, endRow = last_row-1, startColumn=0, endColumn=1)])
                                batchUpdate(workbookId=workbook, bodyList=[insert_row_or_column_body(sheetId=sheetId, dimension="ROWS", start=2, end=data[data['Day'] == datetime.datetime.today()].shape[0])])
                                update_values(workbook, range_name=period.name + " Depts" + '!A3:AF%s' % (last_row - 1), value_input_option="RAW", _values = data[data['Day'] == datetime.datetime.today()].to_numpy().tolist())
                            else:    
                                update_values(workbook, range_name=period.name + " Depts" + '!A3:AF21', value_input_option="RAW", _values = data[data['Day'] == datetime.datetime.today()].to_numpy().tolist())
                        #Monthly, Quarterly, Yearly
                        else:
                            if(get_values(workbook, period.name + " Depts" + '!A3')[0][0] == dept_data['Day'].values.tolist()[0]):
                                update_values(workbook, range_name=period.name + " Depts" + '!A3', value_input_option="RAW", _values = [dept_data.values.tolist()[0]])
                            else:
                                batchUpdate(workbookId=workbook, bodyList=[insert_row_or_column_body(sheetId=sheetId, dimension="ROWS", start=2, end=3)])
                                update_values(workbook, range_name=period.name + " Depts" + '!A4', value_input_option="RAW", _values = [dept_data.values.tolist()[1]])
                    else:
                        batchUpdate(workbookId=workbook, bodyList=[insert_row_or_column_body(sheetId=sheetId, dimension="ROWS", start=2, end=20)])
                        update_values(workbook, range_name=period.name + " Depts" + '!A22:AE40', value_input_option="RAW", _values = data[19:].to_numpy().tolist())
                        match period.name + " Depts":
                            case "Daily":
                                batchUpdate(workbookId=workbook, bodyList=[
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : 1,
                                                "endColumnIndex" : grand_daily_merge_range[9][1]
                                            },
                                            "cell" : {
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
                                            "fields" : """
                                                    userEnteredFormat.textFormat.fontFamily,
                                                    userEnteredFormat.textFormat.fontSize,
                                                    userEnteredFormat.borders.top,
                                                    userEnteredFormat.borders.bottom,
                                                    userEnteredFormat.borders.left,
                                                    userEnteredFormat.borders.right"""
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : 0,
                                                "endColumnIndex" : 1
                                            },
                                            "cell" : {
                                                "userEnteredFormat":{
                                                    "borders":{
                                                        "top": {"style" : "SOLID_THICK"},
                                                        "bottom":{"style" : "SOLID_THICK"},
                                                        "left":{"style" : "SOLID_THICK"},
                                                        "right":{"style" : "SOLID_THICK"}
                                                        },
                                                    "textFormat": { "fontFamily" : "Arial", "fontSize" : 12, "bold" : True}
                                                },
                                            },
                                            "fields" : """
                                                    userEnteredFormat.textFormat.bold,
                                                    userEnteredFormat.textFormat.fontFamily,
                                                    userEnteredFormat.textFormat.fontSize,
                                                    userEnteredFormat.borders.top,
                                                    userEnteredFormat.borders.bottom,
                                                    userEnteredFormat.borders.left,
                                                    userEnteredFormat.borders.right"""
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_daily_merge_range[0][1]-1,
                                                "endColumnIndex" : grand_daily_merge_range[0][1]
                                            },
                                            "cell" : {"userEnteredFormat":{ "borders":{ "right":{"style" : "SOLID_THICK"}}}},
                                            "fields" : """userEnteredFormat.borders.right"""
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_daily_merge_range[1][1]-1,
                                                "endColumnIndex" : grand_daily_merge_range[1][1]
                                            },
                                            "cell" : {"userEnteredFormat":{ "borders":{ "right":{"style" : "SOLID_THICK"}}}},
                                            "fields" : """userEnteredFormat.borders.right"""
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_daily_merge_range[2][1]-1,
                                                "endColumnIndex" : grand_daily_merge_range[2][1]
                                            },
                                            "cell" : {"userEnteredFormat":{ "borders":{ "right":{"style" : "SOLID_THICK"}}}},
                                            "fields" : """userEnteredFormat.borders.right"""
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_daily_merge_range[3][1]-1,
                                                "endColumnIndex" : grand_daily_merge_range[3][1]
                                            },
                                            "cell" : {"userEnteredFormat":{ "borders":{ "right":{"style" : "SOLID_THICK"}}}},
                                            "fields" : """userEnteredFormat.borders.right"""
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_daily_merge_range[4][1]-1,
                                                "endColumnIndex" : grand_daily_merge_range[4][1]
                                            },
                                            "cell" : {"userEnteredFormat":{ "borders":{ "right":{"style" : "SOLID_THICK"}}}},
                                            "fields" : """userEnteredFormat.borders.right"""
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_daily_merge_range[5][1]-1,
                                                "endColumnIndex" : grand_daily_merge_range[5][1]
                                            },
                                            "cell" : {"userEnteredFormat":{ "borders":{ "right":{"style" : "SOLID_THICK"}}}},
                                            "fields" : """userEnteredFormat.borders.right"""
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_daily_merge_range[6][1]-1,
                                                "endColumnIndex" : grand_daily_merge_range[6][1]
                                            },
                                            "cell" : {"userEnteredFormat":{ "borders":{ "right":{"style" : "SOLID_THICK"}}}},
                                            "fields" : """userEnteredFormat.borders.right"""
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_daily_merge_range[7][1]-1,
                                                "endColumnIndex" : grand_daily_merge_range[7][1]
                                            },
                                            "cell" : {"userEnteredFormat":{ "borders":{ "right":{"style" : "SOLID_THICK"}}}},
                                            "fields" : """userEnteredFormat.borders.right"""
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_daily_merge_range[8][1]-1,
                                                "endColumnIndex" : grand_daily_merge_range[8][1]
                                            },
                                            "cell" : {"userEnteredFormat":{ "borders":{ "right":{"style" : "SOLID_THICK"}}}},
                                            "fields" : """userEnteredFormat.borders.right"""
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_daily_merge_range[9][1]-1,
                                                "endColumnIndex" : grand_daily_merge_range[9][1]
                                            },
                                            "cell" : {"userEnteredFormat":{ "borders":{ "right":{"style" : "SOLID_THICK"}}}},
                                            "fields" : """userEnteredFormat.borders.right"""
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_daily_merge_range[0][0],
                                                "endColumnIndex" : grand_daily_merge_range[0][0]+2
                                            },
                                            "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "CURRENCY", "pattern" : "$#,##0.00"}}},
                                            "fields" : """userEnteredFormat.numberFormat.type,
                                                            userEnteredFormat.numberFormat.pattern
                                                    """
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_daily_merge_range[0][1]+1,
                                                "endColumnIndex" : grand_daily_merge_range[0][1]+2
                                            },
                                            "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "PERCENT", "pattern" : "0.00%"}}},
                                            "fields" : """userEnteredFormat.numberFormat.type,
                                                            userEnteredFormat.numberFormat.pattern
                                                    """
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_daily_merge_range[0][0]+3,
                                                "endColumnIndex" : grand_daily_merge_range[0][1]-1
                                            },
                                            "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "CURRENCY", "pattern" : "$#,##0.00"}}},
                                            "fields" : """userEnteredFormat.numberFormat.type,
                                                            userEnteredFormat.numberFormat.pattern
                                                    """
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_daily_merge_range[0][1]-1,
                                                "endColumnIndex" : grand_daily_merge_range[0][1]
                                            },
                                            "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "PERCENT", "pattern" : "0.00%"}}},
                                            "fields" : """userEnteredFormat.numberFormat.type,
                                                            userEnteredFormat.numberFormat.pattern
                                                    """
                                        }
                                    },
                                    #COGS
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_daily_merge_range[1][0],
                                                "endColumnIndex" : grand_daily_merge_range[1][0]+2
                                            },
                                            "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "CURRENCY", "pattern" : "$#,##0.00"}}},
                                            "fields" : """userEnteredFormat.numberFormat.type,
                                                            userEnteredFormat.numberFormat.pattern
                                                    """
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_daily_merge_range[1][1]+1,
                                                "endColumnIndex" : grand_daily_merge_range[1][1]+2
                                            },
                                            "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "PERCENT", "pattern" : "0.00%"}}},
                                            "fields" : """userEnteredFormat.numberFormat.type,
                                                            userEnteredFormat.numberFormat.pattern
                                                    """
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_daily_merge_range[1][0]+3,
                                                "endColumnIndex" : grand_daily_merge_range[1][1]-1
                                            },
                                            "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "CURRENCY", "pattern" : "$#,##0.00"}}},
                                            "fields" : """userEnteredFormat.numberFormat.type,
                                                            userEnteredFormat.numberFormat.pattern
                                                    """
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_daily_merge_range[1][1]-1,
                                                "endColumnIndex" : grand_daily_merge_range[1][1]
                                            },
                                            "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "PERCENT", "pattern" : "0.00%"}}},
                                            "fields" : """userEnteredFormat.numberFormat.type,
                                                            userEnteredFormat.numberFormat.pattern
                                                    """
                                        }
                                    },
                                    #Margins
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_daily_merge_range[2][0],
                                                "endColumnIndex" : grand_daily_merge_range[2][1]+1
                                            },
                                            "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "PERCENT", "pattern" : "0.00%"}}},
                                            "fields" : """userEnteredFormat.numberFormat.type,
                                                            userEnteredFormat.numberFormat.pattern
                                                    """
                                        }
                                    },
                                    #Profit
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_daily_merge_range[3][0],
                                                "endColumnIndex" : grand_daily_merge_range[3][0]+2
                                            },
                                            "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "CURRENCY", "pattern" : "$#,##0.00"}}},
                                            "fields" : """userEnteredFormat.numberFormat.type,
                                                            userEnteredFormat.numberFormat.pattern
                                                    """
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_daily_merge_range[3][1]+1,
                                                "endColumnIndex" : grand_daily_merge_range[3][1]+2
                                            },
                                            "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "PERCENT", "pattern" : "0.00%"}}},
                                            "fields" : """userEnteredFormat.numberFormat.type,
                                                            userEnteredFormat.numberFormat.pattern
                                                    """
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_daily_merge_range[3][0]+3,
                                                "endColumnIndex" : grand_daily_merge_range[3][1]-1
                                            },
                                            "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "CURRENCY", "pattern" : "$#,##0.00"}}},
                                            "fields" : """userEnteredFormat.numberFormat.type,
                                                            userEnteredFormat.numberFormat.pattern
                                                    """
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_daily_merge_range[3][1]-1,
                                                "endColumnIndex" : grand_daily_merge_range[3][1]
                                            },
                                            "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "PERCENT", "pattern" : "0.00%"}}},
                                            "fields" : """userEnteredFormat.numberFormat.type,
                                                            userEnteredFormat.numberFormat.pattern
                                                    """
                                        }
                                    },
                                    #Volume
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_daily_merge_range[4][0],
                                                "endColumnIndex" : grand_daily_merge_range[4][0]+2
                                            },
                                            "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "NUMBER", "pattern" : "#,##0"}}},
                                            "fields" : """userEnteredFormat.numberFormat.type,
                                                            userEnteredFormat.numberFormat.pattern
                                                    """
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_daily_merge_range[4][1]-1,
                                                "endColumnIndex" : grand_daily_merge_range[4][1]
                                            },
                                            "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "PERCENT", "pattern" : "0.00%"}}},
                                            "fields" : """userEnteredFormat.numberFormat.type,
                                                            userEnteredFormat.numberFormat.pattern
                                                    """
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : 0,
                                                "endColumnIndex" : 1
                                            },
                                            "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "DATE", "pattern" : "d/m/yy"}}},
                                            "fields" : """userEnteredFormat.numberFormat.type,
                                                            userEnteredFormat.numberFormat.pattern
                                                    """
                                        }
                                    }])
                            case "Weekly" | "Monthly":
                                batchUpdate(workbookId=workbook, bodyList=[
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : 1,
                                                "endColumnIndex" : grand_weekly_monthly_merge_range[9][1]
                                            },
                                            "cell" : {
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
                                            "fields" : """
                                                    userEnteredFormat.textFormat.fontFamily,
                                                    userEnteredFormat.textFormat.fontSize,
                                                    userEnteredFormat.borders.top,
                                                    userEnteredFormat.borders.bottom,
                                                    userEnteredFormat.borders.left,
                                                    userEnteredFormat.borders.right"""
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : 0,
                                                "endColumnIndex" : 1
                                            },
                                            "cell" : {
                                                "userEnteredFormat":{
                                                    "borders":{
                                                        "top": {"style" : "SOLID_THICK"},
                                                        "bottom":{"style" : "SOLID_THICK"},
                                                        "left":{"style" : "SOLID_THICK"},
                                                        "right":{"style" : "SOLID_THICK"}
                                                        },
                                                    "textFormat": { "fontFamily" : "Arial", "fontSize" : 12, "bold" : True}
                                                },
                                            },
                                            "fields" : """
                                                    userEnteredFormat.textFormat.bold,
                                                    userEnteredFormat.textFormat.fontFamily,
                                                    userEnteredFormat.textFormat.fontSize,
                                                    userEnteredFormat.borders.top,
                                                    userEnteredFormat.borders.bottom,
                                                    userEnteredFormat.borders.left,
                                                    userEnteredFormat.borders.right"""
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_weekly_monthly_merge_range[0][1]-1,
                                                "endColumnIndex" : grand_weekly_monthly_merge_range[0][1]
                                            },
                                            "cell" : {"userEnteredFormat":{ "borders":{ "right":{"style" : "SOLID_THICK"}}}},
                                            "fields" : """userEnteredFormat.borders.right"""
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_weekly_monthly_merge_range[1][1]-1,
                                                "endColumnIndex" : grand_weekly_monthly_merge_range[1][1]
                                            },
                                            "cell" : {"userEnteredFormat":{ "borders":{ "right":{"style" : "SOLID_THICK"}}}},
                                            "fields" : """userEnteredFormat.borders.right"""
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_weekly_monthly_merge_range[2][1]-1,
                                                "endColumnIndex" : grand_weekly_monthly_merge_range[2][1]
                                            },
                                            "cell" : {"userEnteredFormat":{ "borders":{ "right":{"style" : "SOLID_THICK"}}}},
                                            "fields" : """userEnteredFormat.borders.right"""
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_weekly_monthly_merge_range[3][1]-1,
                                                "endColumnIndex" : grand_weekly_monthly_merge_range[3][1]
                                            },
                                            "cell" : {"userEnteredFormat":{ "borders":{ "right":{"style" : "SOLID_THICK"}}}},
                                            "fields" : """userEnteredFormat.borders.right"""
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_weekly_monthly_merge_range[4][1]-1,
                                                "endColumnIndex" : grand_weekly_monthly_merge_range[4][1]
                                            },
                                            "cell" : {"userEnteredFormat":{ "borders":{ "right":{"style" : "SOLID_THICK"}}}},
                                            "fields" : """userEnteredFormat.borders.right"""
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_weekly_monthly_merge_range[5][1]-1,
                                                "endColumnIndex" : grand_weekly_monthly_merge_range[5][1]
                                            },
                                            "cell" : {"userEnteredFormat":{ "borders":{ "right":{"style" : "SOLID_THICK"}}}},
                                            "fields" : """userEnteredFormat.borders.right"""
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_weekly_monthly_merge_range[6][1]-1,
                                                "endColumnIndex" : grand_weekly_monthly_merge_range[6][1]
                                            },
                                            "cell" : {"userEnteredFormat":{ "borders":{ "right":{"style" : "SOLID_THICK"}}}},
                                            "fields" : """userEnteredFormat.borders.right"""
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_weekly_monthly_merge_range[7][1]-1,
                                                "endColumnIndex" : grand_weekly_monthly_merge_range[7][1]
                                            },
                                            "cell" : {"userEnteredFormat":{ "borders":{ "right":{"style" : "SOLID_THICK"}}}},
                                            "fields" : """userEnteredFormat.borders.right"""
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_weekly_monthly_merge_range[8][1]-1,
                                                "endColumnIndex" : grand_weekly_monthly_merge_range[8][1]
                                            },
                                            "cell" : {"userEnteredFormat":{ "borders":{ "right":{"style" : "SOLID_THICK"}}}},
                                            "fields" : """userEnteredFormat.borders.right"""
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_weekly_monthly_merge_range[9][1]-1,
                                                "endColumnIndex" : grand_weekly_monthly_merge_range[9][1]
                                            },
                                            "cell" : {"userEnteredFormat":{ "borders":{ "right":{"style" : "SOLID_THICK"}}}},
                                            "fields" : """userEnteredFormat.borders.right"""
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_weekly_monthly_merge_range[0][0],
                                                "endColumnIndex" : grand_weekly_monthly_merge_range[0][0]+2
                                            },
                                            "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "CURRENCY", "pattern" : "$#,##0.00"}}},
                                            "fields" : """userEnteredFormat.numberFormat.type,
                                                            userEnteredFormat.numberFormat.pattern
                                                    """
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_weekly_monthly_merge_range[0][1]+1,
                                                "endColumnIndex" : grand_weekly_monthly_merge_range[0][1]+2
                                            },
                                            "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "PERCENT", "pattern" : "0.00%"}}},
                                            "fields" : """userEnteredFormat.numberFormat.type,
                                                            userEnteredFormat.numberFormat.pattern
                                                    """
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_weekly_monthly_merge_range[0][0]+3,
                                                "endColumnIndex" : grand_weekly_monthly_merge_range[0][1]-1
                                            },
                                            "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "CURRENCY", "pattern" : "$#,##0.00"}}},
                                            "fields" : """userEnteredFormat.numberFormat.type,
                                                            userEnteredFormat.numberFormat.pattern
                                                    """
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_weekly_monthly_merge_range[0][1]-1,
                                                "endColumnIndex" : grand_weekly_monthly_merge_range[0][1]
                                            },
                                            "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "PERCENT", "pattern" : "0.00%"}}},
                                            "fields" : """userEnteredFormat.numberFormat.type,
                                                            userEnteredFormat.numberFormat.pattern
                                                    """
                                        }
                                    },
                                    #COGS
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_weekly_monthly_merge_range[1][0],
                                                "endColumnIndex" : grand_weekly_monthly_merge_range[1][0]+2
                                            },
                                            "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "CURRENCY", "pattern" : "$#,##0.00"}}},
                                            "fields" : """userEnteredFormat.numberFormat.type,
                                                            userEnteredFormat.numberFormat.pattern
                                                    """
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_weekly_monthly_merge_range[1][1]+1,
                                                "endColumnIndex" : grand_weekly_monthly_merge_range[1][1]+2
                                            },
                                            "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "PERCENT", "pattern" : "0.00%"}}},
                                            "fields" : """userEnteredFormat.numberFormat.type,
                                                            userEnteredFormat.numberFormat.pattern
                                                    """
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_weekly_monthly_merge_range[1][0]+3,
                                                "endColumnIndex" : grand_weekly_monthly_merge_range[1][1]-1
                                            },
                                            "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "CURRENCY", "pattern" : "$#,##0.00"}}},
                                            "fields" : """userEnteredFormat.numberFormat.type,
                                                            userEnteredFormat.numberFormat.pattern
                                                    """
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_weekly_monthly_merge_range[1][1]-1,
                                                "endColumnIndex" : grand_weekly_monthly_merge_range[1][1]
                                            },
                                            "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "PERCENT", "pattern" : "0.00%"}}},
                                            "fields" : """userEnteredFormat.numberFormat.type,
                                                            userEnteredFormat.numberFormat.pattern
                                                    """
                                        }
                                    },
                                    #Margins
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_weekly_monthly_merge_range[2][0],
                                                "endColumnIndex" : grand_weekly_monthly_merge_range[2][1]+1
                                            },
                                            "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "PERCENT", "pattern" : "0.00%"}}},
                                            "fields" : """userEnteredFormat.numberFormat.type,
                                                            userEnteredFormat.numberFormat.pattern
                                                    """
                                        }
                                    },
                                    #Profit
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_weekly_monthly_merge_range[3][0],
                                                "endColumnIndex" : grand_weekly_monthly_merge_range[3][0]+2
                                            },
                                            "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "CURRENCY", "pattern" : "$#,##0.00"}}},
                                            "fields" : """userEnteredFormat.numberFormat.type,
                                                            userEnteredFormat.numberFormat.pattern
                                                    """
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_weekly_monthly_merge_range[3][1]+1,
                                                "endColumnIndex" : grand_weekly_monthly_merge_range[3][1]+2
                                            },
                                            "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "PERCENT", "pattern" : "0.00%"}}},
                                            "fields" : """userEnteredFormat.numberFormat.type,
                                                            userEnteredFormat.numberFormat.pattern
                                                    """
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_weekly_monthly_merge_range[3][0]+3,
                                                "endColumnIndex" : grand_weekly_monthly_merge_range[3][1]-1
                                            },
                                            "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "CURRENCY", "pattern" : "$#,##0.00"}}},
                                            "fields" : """userEnteredFormat.numberFormat.type,
                                                            userEnteredFormat.numberFormat.pattern
                                                    """
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_weekly_monthly_merge_range[3][1]-1,
                                                "endColumnIndex" : grand_weekly_monthly_merge_range[3][1]
                                            },
                                            "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "PERCENT", "pattern" : "0.00%"}}},
                                            "fields" : """userEnteredFormat.numberFormat.type,
                                                            userEnteredFormat.numberFormat.pattern
                                                    """
                                        }
                                    },
                                    #Volume
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_weekly_monthly_merge_range[4][0],
                                                "endColumnIndex" : grand_weekly_monthly_merge_range[4][0]+2
                                            },
                                            "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "NUMBER", "pattern" : "#,##0"}}},
                                            "fields" : """userEnteredFormat.numberFormat.type,
                                                            userEnteredFormat.numberFormat.pattern
                                                    """
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_weekly_monthly_merge_range[4][1]-1,
                                                "endColumnIndex" : grand_weekly_monthly_merge_range[4][1]
                                            },
                                            "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "PERCENT", "pattern" : "0.00%"}}},
                                            "fields" : """userEnteredFormat.numberFormat.type,
                                                            userEnteredFormat.numberFormat.pattern
                                                    """
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : 0,
                                                "endColumnIndex" : 1
                                            },
                                            "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "DATE", "pattern" : "d/m/yy"}}},
                                            "fields" : """userEnteredFormat.numberFormat.type,
                                                            userEnteredFormat.numberFormat.pattern
                                                    """
                                            } 
                                    }
                                ])
                            case "Yearly":
                                batchUpdate(workbookId=workbook, bodyList=[
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : 1,
                                                "endColumnIndex" : grand_yearly_merge_range[9][1]
                                            },
                                            "cell" : {
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
                                            "fields" : """
                                                    userEnteredFormat.textFormat.fontFamily,
                                                    userEnteredFormat.textFormat.fontSize,
                                                    userEnteredFormat.borders.top,
                                                    userEnteredFormat.borders.bottom,
                                                    userEnteredFormat.borders.left,
                                                    userEnteredFormat.borders.right"""
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : 0,
                                                "endColumnIndex" : 1
                                            },
                                            "cell" : {
                                                "userEnteredFormat":{
                                                    "borders":{
                                                        "top": {"style" : "SOLID_THICK"},
                                                        "bottom":{"style" : "SOLID_THICK"},
                                                        "left":{"style" : "SOLID_THICK"},
                                                        "right":{"style" : "SOLID_THICK"}
                                                        },
                                                    "textFormat": { "fontFamily" : "Arial", "fontSize" : 12, "bold" : True}
                                                },
                                            },
                                            "fields" : """
                                                    userEnteredFormat.textFormat.bold,
                                                    userEnteredFormat.textFormat.fontFamily,
                                                    userEnteredFormat.textFormat.fontSize,
                                                    userEnteredFormat.borders.top,
                                                    userEnteredFormat.borders.bottom,
                                                    userEnteredFormat.borders.left,
                                                    userEnteredFormat.borders.right"""
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_yearly_merge_range[0][1]-1,
                                                "endColumnIndex" : grand_yearly_merge_range[0][1]
                                            },
                                            "cell" : {"userEnteredFormat":{ "borders":{ "right":{"style" : "SOLID_THICK"}}}},
                                            "fields" : """userEnteredFormat.borders.right"""
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_yearly_merge_range[1][1]-1,
                                                "endColumnIndex" : grand_yearly_merge_range[1][1]
                                            },
                                            "cell" : {"userEnteredFormat":{ "borders":{ "right":{"style" : "SOLID_THICK"}}}},
                                            "fields" : """userEnteredFormat.borders.right"""
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_yearly_merge_range[2][1]-1,
                                                "endColumnIndex" : grand_yearly_merge_range[2][1]
                                            },
                                            "cell" : {"userEnteredFormat":{ "borders":{ "right":{"style" : "SOLID_THICK"}}}},
                                            "fields" : """userEnteredFormat.borders.right"""
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_yearly_merge_range[3][1]-1,
                                                "endColumnIndex" : grand_yearly_merge_range[3][1]
                                            },
                                            "cell" : {"userEnteredFormat":{ "borders":{ "right":{"style" : "SOLID_THICK"}}}},
                                            "fields" : """userEnteredFormat.borders.right"""
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_yearly_merge_range[4][1]-1,
                                                "endColumnIndex" : grand_yearly_merge_range[4][1]
                                            },
                                            "cell" : {"userEnteredFormat":{ "borders":{ "right":{"style" : "SOLID_THICK"}}}},
                                            "fields" : """userEnteredFormat.borders.right"""
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_yearly_merge_range[5][1]-1,
                                                "endColumnIndex" : grand_yearly_merge_range[5][1]
                                            },
                                            "cell" : {"userEnteredFormat":{ "borders":{ "right":{"style" : "SOLID_THICK"}}}},
                                            "fields" : """userEnteredFormat.borders.right"""
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_yearly_merge_range[6][1]-1,
                                                "endColumnIndex" : grand_yearly_merge_range[6][1]
                                            },
                                            "cell" : {"userEnteredFormat":{ "borders":{ "right":{"style" : "SOLID_THICK"}}}},
                                            "fields" : """userEnteredFormat.borders.right"""
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_yearly_merge_range[7][1]-1,
                                                "endColumnIndex" : grand_yearly_merge_range[7][1]
                                            },
                                            "cell" : {"userEnteredFormat":{ "borders":{ "right":{"style" : "SOLID_THICK"}}}},
                                            "fields" : """userEnteredFormat.borders.right"""
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_yearly_merge_range[8][1]-1,
                                                "endColumnIndex" : grand_yearly_merge_range[8][1]
                                            },
                                            "cell" : {"userEnteredFormat":{ "borders":{ "right":{"style" : "SOLID_THICK"}}}},
                                            "fields" : """userEnteredFormat.borders.right"""
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_yearly_merge_range[9][1]-1,
                                                "endColumnIndex" : grand_yearly_merge_range[9][1]
                                            },
                                            "cell" : {"userEnteredFormat":{ "borders":{ "right":{"style" : "SOLID_THICK"}}}},
                                            "fields" : """userEnteredFormat.borders.right"""
                                        }
                                    },
                                    #Sales
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_yearly_merge_range[0][0],
                                                "endColumnIndex" : grand_yearly_merge_range[0][0]+2
                                            },
                                            "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "CURRENCY", "pattern" : "$#,##0.00"}}},
                                            "fields" : """userEnteredFormat.numberFormat.type,
                                                            userEnteredFormat.numberFormat.pattern
                                                    """
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_yearly_merge_range[0][1]+1,
                                                "endColumnIndex" : grand_yearly_merge_range[0][1]+2
                                            },
                                            "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "PERCENT", "pattern" : "0.00%"}}},
                                            "fields" : """userEnteredFormat.numberFormat.type,
                                                            userEnteredFormat.numberFormat.pattern
                                                    """
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_yearly_merge_range[0][0]+3,
                                                "endColumnIndex" : grand_yearly_merge_range[0][1]-1
                                            },
                                            "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "CURRENCY", "pattern" : "$#,##0.00"}}},
                                            "fields" : """userEnteredFormat.numberFormat.type,
                                                            userEnteredFormat.numberFormat.pattern
                                                    """
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_yearly_merge_range[0][1]-1,
                                                "endColumnIndex" : grand_yearly_merge_range[0][1]
                                            },
                                            "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "PERCENT", "pattern" : "0.00%"}}},
                                            "fields" : """userEnteredFormat.numberFormat.type,
                                                            userEnteredFormat.numberFormat.pattern
                                                    """
                                        }
                                    },
                                    #COGS
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_yearly_merge_range[1][0],
                                                "endColumnIndex" : grand_yearly_merge_range[1][0]+2
                                            },
                                            "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "CURRENCY", "pattern" : "$#,##0.00"}}},
                                            "fields" : """userEnteredFormat.numberFormat.type,
                                                            userEnteredFormat.numberFormat.pattern
                                                    """
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_yearly_merge_range[1][1]+1,
                                                "endColumnIndex" : grand_yearly_merge_range[1][1]+2
                                            },
                                            "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "PERCENT", "pattern" : "0.00%"}}},
                                            "fields" : """userEnteredFormat.numberFormat.type,
                                                            userEnteredFormat.numberFormat.pattern
                                                    """
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_yearly_merge_range[1][0]+3,
                                                "endColumnIndex" : grand_yearly_merge_range[1][1]-1
                                            },
                                            "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "CURRENCY", "pattern" : "$#,##0.00"}}},
                                            "fields" : """userEnteredFormat.numberFormat.type,
                                                            userEnteredFormat.numberFormat.pattern
                                                    """
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_yearly_merge_range[1][1]-1,
                                                "endColumnIndex" : grand_yearly_merge_range[1][1]
                                            },
                                            "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "PERCENT", "pattern" : "0.00%"}}},
                                            "fields" : """userEnteredFormat.numberFormat.type,
                                                            userEnteredFormat.numberFormat.pattern
                                                    """
                                        }
                                    },
                                    #Margins
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_yearly_merge_range[2][0],
                                                "endColumnIndex" : grand_yearly_merge_range[2][1]+1
                                            },
                                            "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "PERCENT", "pattern" : "0.00%"}}},
                                            "fields" : """userEnteredFormat.numberFormat.type,
                                                            userEnteredFormat.numberFormat.pattern
                                                    """
                                        }
                                    },
                                    #Profit
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_yearly_merge_range[3][0],
                                                "endColumnIndex" : grand_yearly_merge_range[3][0]+2
                                            },
                                            "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "CURRENCY", "pattern" : "$#,##0.00"}}},
                                            "fields" : """userEnteredFormat.numberFormat.type,
                                                            userEnteredFormat.numberFormat.pattern
                                                    """
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_yearly_merge_range[3][1]+1,
                                                "endColumnIndex" : grand_yearly_merge_range[3][1]+2
                                            },
                                            "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "PERCENT", "pattern" : "0.00%"}}},
                                            "fields" : """userEnteredFormat.numberFormat.type,
                                                            userEnteredFormat.numberFormat.pattern
                                                    """
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_yearly_merge_range[3][0]+3,
                                                "endColumnIndex" : grand_yearly_merge_range[3][1]-1
                                            },
                                            "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "CURRENCY", "pattern" : "$#,##0.00"}}},
                                            "fields" : """userEnteredFormat.numberFormat.type,
                                                            userEnteredFormat.numberFormat.pattern
                                                    """
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_yearly_merge_range[3][1]-1,
                                                "endColumnIndex" : grand_yearly_merge_range[3][1]
                                            },
                                            "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "PERCENT", "pattern" : "0.00%"}}},
                                            "fields" : """userEnteredFormat.numberFormat.type,
                                                            userEnteredFormat.numberFormat.pattern
                                                    """
                                        }
                                    },
                                    #Volume
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_yearly_merge_range[4][0],
                                                "endColumnIndex" : grand_yearly_merge_range[4][0]+2
                                            },
                                            "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "NUMBER", "pattern" : "#,##0"}}},
                                            "fields" : """userEnteredFormat.numberFormat.type,
                                                            userEnteredFormat.numberFormat.pattern
                                                    """
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : grand_yearly_merge_range[4][1]-1,
                                                "endColumnIndex" : grand_yearly_merge_range[4][1]
                                            },
                                            "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "PERCENT", "pattern" : "0.00%"}}},
                                            "fields" : """userEnteredFormat.numberFormat.type,
                                                            userEnteredFormat.numberFormat.pattern
                                                    """
                                        }
                                    },
                                    {
                                        "repeatCell" :{
                                            "range" : {
                                                "sheetId" : sheetId,
                                                "startRowIndex" : 2,
                                                "endRowIndex" : 3,
                                                "startColumnIndex" : 0,
                                                "endColumnIndex" : 1
                                            },
                                            "cell" : {"userEnteredFormat":{ "numberFormat":{ "type": "DATE", "pattern" : "d/m/yy"}}},
                                            "fields" : """userEnteredFormat.numberFormat.type,
                                                            userEnteredFormat.numberFormat.pattern
                                                    """
                                            } 
                                    }
                                ])
                        append_values(workbook, range_name=period.name + " Depts" + '!A3', dim="ROWS",value_input_option="RAW", _values = data[:18].to_numpy().tolist())
            

    toc = time.perf_counter()
    with open('basic_update_log.txt', 'a') as out:
        out.write(f"Time elapsed: {toc-tic:0.04f} seconds, finished at %s\n" % datetime.datetime.now())
except:
    logging.exception('failed at %s' % datetime.datetime.now())
    raise