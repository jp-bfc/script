from sqlite3 import converters
import openpyxl
import xlrd
#https://docs.python.org/3/library/tkinter.html
import tkinter as tk
from tkinter import filedialog
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
import time
import numpy as np
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

def insert_row_or_column_body(sales_sheetId, dimension, start, end, inherit=False):
        return {
            "insertDimension" : {
                "range" : {
                    "sheetId" : sales_sheetId,
                    "dimension" : dimension,
                    "startIndex" : start,
                    "endIndex" : end
                },
                "inheritFromBefore" : inherit
            }
        }

def repeatCell(gridRange, cell):
    if (gridRange is not None and gridRange != {}) and (cell is not None and cell != {}):
        repeat_cell = {}
        repeat_cell["range"] = gridRange
        repeat_cell["cell"] = cell
        init_field = find_deep(cell, "")
        fields = ", ".join(init_field)
        repeat_cell["fields"] = "%s" % fields
        return {"repeatCell": repeat_cell}
    else:
        print("Something missing in repeatCellRequest.")

def dimensionRange(sheetId=None, dimension=None, startIndex=None, endIndex=None):
    dim = {}
    if sheetId is not None: dim['sheetId'] = sheetId
    if dimension is not None: dim['dimension'] = dimension
    if startIndex is not None: dim['startIndex'] = startIndex
    if endIndex is not None: dim['endIndex'] = endIndex
    return dim

def dimensionProps(pixelSize=None, hiddenByFilter=None, hiddenByUser=None, developerMetadata=None, dataSourceColumnReference=None):
    props = {}
    if hiddenByFilter is not None: props["hiddenByFilter"] = hiddenByFilter,
    if hiddenByUser is not None: props["hiddenByUser"] = hiddenByUser,
    if developerMetadata is not None: props["developerMetadata"] = developerMetadata,
    if dataSourceColumnReference is not None: props["dataSourceColumnReference"] = dataSourceColumnReference,
    if pixelSize is not None : props['pixelSize'] = pixelSize
    return props

def updateDimensionProps(dimProperties=None, dimRange=None):
    update = {}
    if dimRange is not None: update["range"] = dimRange
    if dimProperties is not None:
        update["properties"] = dimProperties
        update['fields'] = ", ".join(find_deep(dimProperties, ""))
    return {"updateDimensionProperties" : update}

solid = border_style(style="SOLID")
thick = border_style(style="SOLID_THICK")
all_thick = [thick, thick, thick, thick]
all_solid = [solid, solid, solid, solid]
percent = numberFormat(type="PERCENT", pattern="0.00%")
currency = numberFormat(type="CURRENCY", pattern="$#,##0.00")
nmbr = numberFormat(type="NUMBER", pattern="#,##0.00")
greenVal = getColor(50,248,40,0.05)
redVal = getColor(225, 40, 40, 0.05)

#https://docs.microsoft.com/en-us/sql/connect/python/pyodbc/step-3-proof-of-concept-connecting-to-sql-using-pyodbc?view=sql-server-ver16
config = configparser.ConfigParser()
config.read('script_configs.ini')
username = config['DEFAULT']['user']
password = config['DEFAULT']['password']
server = config['DEFAULT']['server']
port = config['DEFAULT']['port']
database = config['DEFAULT']['database']
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';PORT='+port+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()


unfi_folder_id = config['DEFAULT']['unfi_folder']


query = """select ot.F01 as 'UPC', rpc.F1024 as 'Department', sdp.F1022 as 'SubDept', ot.F22 as 'Size', ot.F29 as 'Description',
ot.F155 as 'Brand', c.F27 as 'Vendor ID', c.F19 as 'Case Size', c.F38 as 'Base Cost', c.F1184 as 'Buying Format',
c.F196 as 'Case Net Cost', c.F1140 as 'Unit Net Cost', prc.F30 as 'Current Price'
from STORESQL.dbo.OBJ_TAB ot
left join STORESQL.dbo.COST_TAB c on ot.F01 = c.F01
LEFT join STORESQL.dbo.RPC_TAB rpc on ot.F18 = rpc.F18
LEFT join STORESQL.dbo.POS_TAB pos on ot.F01 = pos.F01
LEFT join STORESQL.dbo.SDP_TAB sdp on pos.F04 = sdp.F04
left join STORESQL.dbo.PRICE_TAB prc on ot.F01 = prc.F01
where c.F27 = '0001' or c.F27 = '1'"""

#https://pandas.pydata.org/docs/reference/api/pandas.read_sql.html
unfi = pd.read_sql(query, cnxn)
#print(unfi)
all_update_frames = []

def splitUPC(x):
    return str(x[:13])

def tryParseFloat(value):
    try:
        float(value)
        return True
    except ValueError:
        return False

#open every file in AGNE CSVs folder, then read each .xls into a pandas Dataframe, add that dataframe to the end of the all_update_frames list
for file in os.listdir('UNFI\\'):
    if file.endswith(".CSV") | file.endswith(".csv"):
        #print(file)
        #https://pandas.pydata.org/docs/reference/api/pandas.read_excel.html
        #https://stackoverflow.com/questions/32591466/python-pandas-how-to-specify-data-types-when-reading-an-excel-file
        #it is important to ensure the UPCs are text here, otherwise it will drop the leading 0s, though if that is the case,
        #one could still use: https://pandas.pydata.org/docs/reference/api/pandas.Series.str.zfill.html, but it's much easier to ensure the text type of this column at import time
        #print(file)
        df = pd.read_csv('UNFI\\%s' % file, converters={'UPC':splitUPC})
        #print(df)
        all_update_frames.append(df)
        #print(all_update_frames)
    if "core set" in file:
        if file.endswith(".CSV") | file.endswith(".csv"):
            core = pd.read_csv('UNFI\\%s' % file)
            core.fillna("", inplace=True)
            core = core[core['EDLP Price'] != ""]
            core['EDLP Price'] = core['EDLP Price'].apply(lambda x: x if tryParseFloat(x) else str(x).split(",")[0])
            #print(core)
        elif file.endswith(".XLSX") | file.endswith(".xlsx"):
            core = pd.read_excel('UNFI\\%s' % file)
            core.fillna("", inplace=True)
            core = core[core['EDLP Price'] != ""]
            core['EDLP Price'] = core['EDLP Price'].apply(lambda x: x if tryParseFloat(x) else str(x).split(",")[0])
            core['UPC'] = core['Reporting UPC\n(12 digits w/o check)'].astype(str).str.zfill(13)
            core = core.drop("Dept", axis=1)
            #print(core)
            #core.to_csv("core-test.csv")

            core_compliance_folder = '1dxRAjN81rLOIwiMf55-hKTY4SCRoN0Op'

            look_up_query = """
            select
            obj.F01 as 'UPC', obj.F155 as 'Brand', obj.F29 as 'Name', pt.F30 as 'SMS Price', dpt.F1024 as 'Dept'
            from
            STORESQL.dbo.OBJ_TAB obj
            inner join STORESQL.dbo.PRICE_TAB pt on obj.F01 = pt.F01
            inner join STORESQL.dbo.RPC_TAB dpt on obj.F18 = dpt.F18
            """

            sms_frame = pd.read_sql(look_up_query, cnxn)
            print(sms_frame)

            current_core = core.merge(sms_frame, how="inner", on='UPC')
            print(current_core)
            current_core = current_core[current_core['EDLP Price'] != current_core['SMS Price']]
            current_core['Diff'] = current_core['EDLP Price'] - current_core['SMS Price']
            current_core['Change Price'] = ""
            current_core.fillna("", inplace=True)
            current_core = current_core.astype(str)

            current_core = current_core.rename({'EDLP Price' : 'Core Price'}, axis=1)
            period_name = 'November 2022'
            compliance_book = createBook("Core Sets Compliance - " + period_name, core_compliance_folder, 'application/vnd.google-apps.spreadsheet', drive)
            compliance_id = compliance_book.get('id', '')

            dept_list = current_core['Dept'].unique().tolist()
            batchUpdate(compliance_id, addSheet(sheetProp(title="SMS Import", gridProperties=gridProp(current_core.shape[0]+1, current_core.shape[1], 1))))
            append(compliance_id, "SMS IMPORT!A1:B1", "ROWS", [current_core.columns.tolist()], "RAW")
            append(compliance_id, "SMS IMPORT!A2:B2", "ROWS", current_core.to_numpy().tolist(), "RAW")

            format_updates = []
            for dept in dept_list:
                dept_frame = current_core[current_core['Dept'] == dept]
                # dept_frame = dept_frame.drop(['Dept'], axis=1)
                dept_frame = dept_frame[['UPC', 'Name', 'Core Price', 'SMS Price', 'Diff', 'Change Price', 'Line Notes', 'Changes']]
                sheet = batchUpdate(compliance_id, addSheet(sheetProp(title=dept.split()[0], gridProperties=gridProp(dept_frame.shape[0]+1, dept_frame.shape[1], 1))))
                sheet_id = sheet.get('replies', '')[0].get('addSheet', '').get('properties', '').get('sheetId', '')
                append(compliance_id, dept.split()[0]+"!A1:B1", "ROWS", [dept_frame.columns.tolist()], "USER_ENTERED")
                append(compliance_id, dept.split()[0]+"!A2:B2", "ROWS", dept_frame.to_numpy().tolist(), "USER_ENTERED")
                format_updates.extend([
                    repeatCell(
                    gridRange(sheet_id, 0, 1, 0, dept_frame.shape[1]),
                    cellData(userEnteredFormat=cellFormat(
                        borders=borders(*all_thick),
                        textFormat=textFormat(fontFamily="Arial", fontSize=14, bold=True),
                        backgroundColor=getColor(255, 255, 0, 0.01),
                        horizontalAlignment="CENTER",
                        verticalAlignment="MIDDLE"
                        ))
                    ),
                repeatCell(
                    gridRange(sheet_id, 1, dept_frame.shape[0]+1, 0, dept_frame.shape[1]),
                    cellData(userEnteredFormat=cellFormat(
                        borders=borders(*all_solid),
                        textFormat=textFormat(fontFamily="Arial", fontSize=12)
                        ))
                    ),
                repeatCell(
                    gridRange(sheet_id, 1, dept_frame.shape[0]+1, 2, 5),
                    cellData(userEnteredFormat=cellFormat(numberFormat=currency))
                    ),
                addConditional(
                    rule(
                        gridRange(sheet_id, 1, dept_frame.shape[0]+1, 4, 5),
                        boolRule(
                            boolCond("NUMBER_GREATER", [condVal("0")]), cellFormat(backgroundColor=greenVal))),
                    0),
                addConditional(
                    rule(
                        gridRange(sheet_id, 1, dept_frame.shape[0]+1, 4, 5),
                        boolRule(
                            boolCond("NUMBER_LESS", [condVal("0")]), cellFormat(backgroundColor=redVal))),
                    0),
                veryBasicFilter(gridRange(sheet_id, 0, dept_frame.shape[0]+1, 0, dept_frame.shape[1])),
                autoSize(sheet_id, "COLUMNS")
                ])

            batchUpdate(compliance_id, format_updates)
            batchUpdate(compliance_id, deleteSheet(0))

dept_margins = {
"Grocery Department" : 0.35,
"Taxable Grocery" : 0.38,
"Frozen Department" : 0.35,
"Dairy Department" : 0.30,
"Bulk Department" : 0.43,
"TCH Department" : 0.35,
"Produce Department" : 0.35,
"Floral Department" : 0.25,
"Wine Department" : 0.26,
"Beer Department" : 0.25,
"Supplements Department" : 0.45,
"Haba Department" : 0.45,
"CBM Department" : 0.40,
"Housewares Department" : 0.45,
"Deli Department" : 0.58,
"Meat Department" : 0.30,
"Cheese Department" : 0.36,
"Seafood Department" : 0.30
}



#plan_margins = pd.DataFrame(data=dept_margins)

# previous_month_query = """
# select rpt.F01 as 'UPC', round(sum(rpt.F64), 2) as 'Volume', round(sum(rpt.F65) - sum(rpt.F1301), 2) as 'Profit',
# CASE WHEN sum(rpt.F64) > 0 then round(sum(rpt.F65)/sum(rpt.F64), 2) else 0 END as 'Avg Revenue',
# CASE WHEN sum(rpt.F64) > 0 then round(sum(rpt.F1301)/sum(rpt.F64), 2) else 0 END as 'Avg Cost',
# CASE WHEN sum(rpt.F65) > 0 then round((sum(rpt.F65) - sum(rpt.F1301))/sum(rpt.F65), 4) else 0 END as 'Avg Margin',
# CASE WHEN sum(rpt.F64) > 0 then round((sum(rpt.F65) - sum(rpt.F1301))/sum(rpt.F64), 4) else 0 END as 'Avg Profit',
# prc.F30 as 'Active Price', ct.F1140 as 'Current Unit Cost'
# from (select * from STORESQL.dbo.RPT_ITM_D
# where F254 < GETDATE() and F254 >= DATEADD(day, -30, GETDATE()) and F1034 = 3) rpt
# inner join STORESQL.dbo.OBJ_TAB obj on obj.F01 = rpt.F01
# inner join STORESQL.dbo.POS_TAB pos on obj.F01 = pos.F01
# inner join STORESQL.dbo.RPC_TAB rpc on obj.F18 = rpc.F18
# inner join STORESQL.dbo.sdp_tab sdp on pos.F04 = sdp.F04
# inner join STORESQL.dbo.PRICE_TAB prc on obj.F01 = prc.F01
# inner join STORESQL.dbo.COST_TAB ct on ct.F01 = rpt.F01
# where rpc.F18 not in (21, 23, 97) and prc.F30 = prc.F1007
# group by rpt.F01, obj.F29, obj.F155, rpc.F1024, sdp.F1022, prc.F30, ct.F1140
# order by rpc.F1024, sum(rpt.F65) desc
# """

previous_month_query = """
select rpt.F01 as 'UPC', round(sum(rpt.F64), 2) as 'Last Month Volume', round(sum(rpt.F65) - sum(rpt.F1301), 2) as 'Last Month Profit',
round(sum(rpt.F65), 2) as 'Last Month Revenue',
round(sum(rpt.F1301), 2) as 'Last Month Cost',
CASE WHEN sum(rpt.F65) > 0 then round((sum(rpt.F65) - sum(rpt.F1301))/sum(rpt.F65), 4) else 0 end as 'Last Month Margin',
prc.F30 as 'Active Price', ct.F1140 as 'Current Unit Cost'
from (select * from STORESQL.dbo.RPT_ITM_D
where F254 < GETDATE() and F254 >= DATEADD(day, -30, GETDATE()) and F1034 = 3) rpt
inner join STORESQL.dbo.OBJ_TAB obj on obj.F01 = rpt.F01
inner join STORESQL.dbo.POS_TAB pos on obj.F01 = pos.F01
inner join STORESQL.dbo.RPC_TAB rpc on obj.F18 = rpc.F18
inner join STORESQL.dbo.sdp_tab sdp on pos.F04 = sdp.F04
inner join STORESQL.dbo.PRICE_TAB prc on obj.F01 = prc.F01
inner join STORESQL.dbo.COST_TAB ct on ct.F01 = rpt.F01
where rpc.F18 not in (21, 23, 97) and prc.F30 = prc.F1007
group by rpt.F01, obj.F29, obj.F155, rpc.F1024, sdp.F1022, prc.F30, ct.F1140
order by rpc.F1024, sum(rpt.F65) desc"""

prev_month = pd.read_sql(previous_month_query, cnxn)
prev_month['Last Month Volume'] = prev_month['Last Month Volume'].fillna(0.0)
prev_month['Last Month Profit'] = prev_month['Last Month Profit'].fillna(0.0)
prev_month = prev_month.astype({"Last Month Volume":float, "Last Month Profit":float})
#https://pandas.pydata.org/docs/reference/api/pandas.concat.html
#https://stackoverflow.com/questions/28097222/pandas-merge-two-dataframes-with-different-columns

combined = pd.concat(all_update_frames, axis=0, ignore_index=True)

#print(combined.UPC)
combined['UPC'] = combined['UPC'].str.zfill(13)
#print(combined.UPC)
#https://en.wikipedia.org/wiki/Enumerated_type
#!!!this is the real trick in this entire program!!!:
#   - build an ordered list of dates and remove duplicates according to it by transforming the "Order Date" column of DateTimes from AGNE into 'categories'/enumerables/enums/mutually distinct tags/whatever you want to call them
#       1. find all the unique "Order Date" dates in the different AGNE files
#       2. a simple sort() method will return them in ascending order which is what we want: [Day 1 < Day 2 < Day 3 < etc.]
#       3. transform this list into a pandas Categorical Series

#       4. sort all the AGNE items by this Categorical
#       5. drop all duplicate UPCs with a preference for the "largest" date
combined = combined.drop([
    'Whs','Product','Catg',
    'APPRV', 'Reg Cost', 'Reg Unit', 'Disc PCT', 'Disc SRC',
    'P', 'SRP', 'Customer Prod','Catg Desc','Whs Avail','Prod Info',
    'PrdOrdBrPt','BrPtDesc','Buyer','Customer-Prod-2'], axis=1)


#https://pandas.pydata.org/docs/reference/api/pandas.Categorical.html
#https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.sort_values.html
ordered_dates = pd.Categorical(combined['Net Cost'], categories=combined['Net Cost'].unique().sort(), ordered=True)
#https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.drop_duplicates.html
combined = combined.sort_values(["UPC", "Net Cost"]).drop_duplicates("UPC", keep="last")
#print(combined)
combined.to_csv("before.csv")
combined['UPC'] = combined['UPC'].astype(str).str.zfill(13)
#print(pd.Series(core['Reporting UPC\n(12 digits w/o check)']))
excluded = combined[combined['UPC'].isin(pd.Series(core['UPC']))].reset_index()
#print(excluded.dtypes)
exclude_combine = excluded.merge(core, how="inner", on=['UPC'])
exclude_combine.to_csv("excluded.csv")
combined = combined[~combined['UPC'].isin(pd.Series(core['Reporting UPC\n(12 digits w/o check)']))]
#print(combined)
combined.to_csv("after.csv")

#https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.drop.html
#https://stackoverflow.com/questions/13411544/delete-a-column-from-a-pandas-dataframe

'Reportable Unit of Measure',
# this is just pandas terminology for what is essentially a SQL join operation
# https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.merge.html

combined['UPC'] = combined['UPC'].astype(str).str.zfill(13)
unfi['UPC'] = unfi['UPC'].astype(str).str.zfill(13)
merged = unfi.merge(combined, how='inner', on=['UPC'], suffixes=('_BFC', '_UNFI'))
#if the case costs between what is in SMS and the UNFI update are the same, drop those records 
merged['Update'] = (merged['Case Net Cost'] == merged['Net Cost']).astype(bool)
updates = merged[merged['Update']==False]
updates = updates.drop(['Update'], axis=1)

#https://stackoverflow.com/questions/34023918/make-new-column-in-panda-dataframe-by-adding-values-from-other-columns
#calculate delta of margins
updates['Old Margin'] = ((updates['Current Price'] - updates['Unit Net Cost']) / updates['Current Price'])
updates['New Margin'] = ((updates['Current Price'] - updates['Net Unit']) / updates['Current Price'])
updates['Margin Diff'] = updates['New Margin'] - updates['Old Margin']

updates['Plan Margin'] = dept_margins[updates['Department'].iloc[0]]
#updates['Plan Margin'] = updates['Plan Margin'].astype(float)
#print(updates)  
# updates = updates.drop([
#     'Sizeagne','Descriptionagne','Vendor ID','Base Cost','Buying Format','Case Net Cost','Pack','Ord Date'
#     ], axis=1)
#(np.ceil(((grand_under_margin.Cost.astype(float)/grand_under_margin.Volume.astype(float)) / (1.0 - plan_margins.loc[plan_margins['Dept'] == dept, 'Planned Margin'].iloc[0]))*2) / 2)-0.01

updates['Plan Margin Price'] = (np.ceil((updates['Net Unit'] / (1.0 - updates['Plan Margin']))*2)/2)-0.01
updates['Old Profit Price'] = (np.ceil(((updates['Current Price'] - updates['Unit Net Cost']) + updates['Net Unit'])*2)/2)-0.01
updates['Old Margin Price'] = (np.ceil((updates['Net Unit']/(1 - updates['Old Margin']))*2)/2)-0.01
#https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.rename.html
updates.to_csv('up_testing.csv')
updates.rename(columns={
    "Unit Net Cost" : "BFC Net Cost",
    "Descriptionbfc" : "Description",
    "Net Unit" : "UNFI Net Unit", "SubDept" : "Sub-Dept"
}, inplace=True)



#build the Google Sheet we will be sending to the department managers, with the Drive API
#https://googleapis.github.io/google-api-python-client/docs/dyn/drive_v3.files.html#create
output_workbook = createBook('UNFI Cost Updates - September 2022 Imports', unfi_folder_id, 'application/vnd.google-apps.spreadsheet', drive)
#callback for Google API file ID lookups
workbookId = output_workbook.get('id', '')



add_response = batchUpdate(workbookId, body=addSheet(sheetProp(title="Raw Cost Changes", gridProperties=gridProp(frozenRow=1))))
updates = updates[updates['BFC Net Cost'] != updates['UNFI Net Unit']]

raw_sms_cost_updates = updates[[
    'Vendor ID',
    'Department','Sub-Dept','UPC',
    'Description_BFC','Description_UNFI',
    'Size_BFC','Size_UNFI',
    'Brand_BFC','Brand_UNFI',
    'Case Size','Pack',
    'Base Cost','Case Net Cost','Net Cost',
    'Buying Format',
    'BFC Net Cost','UNFI Net Unit',
    'Current Price',
    'Old Margin','New Margin','Margin Diff'
]]

headers = raw_sms_cost_updates.columns.tolist()

# print(raw_sms_cost_updates.to_numpy().tolist())
# print([headers])
# cost_list = raw_sms_cost_updates.to_numpy().tolist()
# print([headers].extend(cost_list))
append(workbookId, "Raw Cost Changes!A1:B1", "ROWS", [headers], "USER_ENTERED")
append(workbookId, "Raw Cost Changes!A1:B1", "ROWS", raw_sms_cost_updates.to_numpy().tolist(), "USER_ENTERED")
# result = sheets.spreadsheets().values().append(
#     spreadsheetId=workbookId,
#     range="Raw Cost Changes!A1:B1",
#     body={ "majorDimension" : "ROWS", "values" : [headers]},
#     valueInputOption="RAW"
#         ).execute()
# #values
# result = sheets.spreadsheets().values().append(
#     spreadsheetId=workbookId,
#     range="Raw Cost Changes!A1:B1",
#     body={ "majorDimension" : "ROWS", "values" : raw_sms_cost_updates.to_numpy().tolist()},
#     valueInputOption="RAW"
#         ).execute()
rawSheetId = add_response.get('replies', '')[0].get('addSheet', '').get('properties', '').get('sheetId', '')
batchUpdate(
        workbookId,
        body=[repeatCell(
            gridRange(rawSheetId,1,raw_sms_cost_updates.shape[0]+1,raw_sms_cost_updates.columns.get_loc("Base Cost"),raw_sms_cost_updates.columns.get_loc("Buying Format")),
            cellData(userEnteredFormat=cellFormat(numberFormat=numberFormat("NUMBER", "0.00")))
        ),
        repeatCell(
            gridRange(rawSheetId,1,raw_sms_cost_updates.shape[0]+1,raw_sms_cost_updates.columns.get_loc("BFC Net Cost"),raw_sms_cost_updates.columns.get_loc("Old Margin")),
            cellData(userEnteredFormat=cellFormat(numberFormat=numberFormat("NUMBER", "0.00")))
        )]
        )


sms_import_sheet = batchUpdate(workbookId, body=addSheet(sheetProp(title="SMS Import", gridProperties=gridProp(frozenRow=1))))
# add_response = sheets.spreadsheets().batchUpdate(
#         spreadsheetId = workbookId,
#         body = {
#             "requests": {
#                 "addSheet":{
#                     "properties": {
#                     "title": "SMS Import",
#                     "gridProperties" : { "frozenRowCount" : 1 }
#                         }
#                     }
#         }}).execute()



sms_import = updates[[
    'UPC','Pack','Net Cost',
    'BFC Net Cost','UNFI Net Unit'
]]

headers = sms_import.columns.tolist()
# result = sheets.spreadsheets().values().append(
#     spreadsheetId=workbookId,
#     range="SMS Import!A1:B1",
#     body={ "majorDimension" : "ROWS", "values" : [headers]},
#     valueInputOption="RAW"
#         ).execute()
append(workbookId, "SMS Import!A1:B1", "ROWS", [headers], "USER_ENTERED")
append(workbookId, "SMS Import!A1:B1", "ROWS", sms_import.to_numpy().tolist(), "USER_ENTERED")
#values
# result = sheets.spreadsheets().values().append(
#     spreadsheetId=workbookId,
#     range="SMS Import!A1:B1",
#     body={ "majorDimension" : "ROWS", "values" : sms_import.to_numpy().tolist()},
#     valueInputOption="RAW"
#         ).execute()
importId = sms_import_sheet.get('replies', '')[0].get('addSheet', '').get('properties', '').get('sheetId', '')
# response = sheets.spreadsheets().batchUpdate(spreadsheetId = workbookId,
#             body = {"requests" :[
#                         {"repeatCell" :
#                             {"range" :
#                                 {"sheetId": importId,
#                                 "startRowIndex": 1,
#                                 "endRowIndex" : sms_import.shape[0]+1,
#                                 "startColumnIndex": sms_import.columns.get_loc("Net Cost"),
#                                 "endColumnIndex": sms_import.columns.get_loc("UNFI Net Unit")+1},
#                             "cell": {
#                                 "userEnteredFormat":{"numberFormat": {"type": "NUMBER", "pattern" : "0.00" }}
#                                 },
#                                 "fields" : """userEnteredFormat.numberFormat.type,userEnteredFormat.numberFormat.pattern"""
#                             }
#                         }]
#                     }
#             ).execute()
batchUpdate(workbookId,body=[
                        repeatCell(
                            gridRange(importId,1,sms_import.shape[0]+1,sms_import.columns.get_loc("Net Cost"),sms_import.columns.get_loc("UNFI Net Unit")),
                            cellData(userEnteredFormat=cellFormat(numberFormat=numberFormat("NUMBER", "0.00")))
                        )]
                        )










updates.insert(len(updates.columns), 'Updated Price', '')
updates.insert(len(updates.columns), 'Updated Margin', '')
updates.insert(len(updates.columns), 'Line Pricing', '')
updates.insert(len(updates.columns), 'Avg Vol', '')
updates.insert(len(updates.columns), 'Projected Profit Change', '')
# updates.insert(len(updates.columns), 'Last Month Volume', 0.0)
# updates.insert(len(updates.columns), 'Last Month Profit', 0.0)
updates.insert(len(updates.columns), 'Generated Price', '')
updates.insert(len(updates.columns), 'Apply Plan Margin', '')
updates.insert(len(updates.columns), 'Apply Old Profit', '')
updates.insert(len(updates.columns), 'Apply Old Margin', '')



updates['Last Month Volume'] = updates.merge(prev_month, on=['UPC'], how='inner')['Last Month Volume']
updates['Last Month Profit'] = updates.merge(prev_month, on=['UPC'], how='inner')['Last Month Profit']
#updates = updates.fillna("")
#=((ROUNDUP((T2/(1-Y2))*2,0)/2)-0.01)
#=((ROUNDUP(((N2-M2)+T2)*2,0)/2)-0.01)
#=((ROUNDUP((T2/(1-V2))*2,0)/2)-0.01)

#reorder Dataframe columns to match what we want in Google Sheets
#https://stackoverflow.com/questions/41968732/set-order-of-columns-in-pandas-dataframe
updates = updates[['UPC', 'Brand_BFC', 'Description_BFC', 'Department', 'Sub-Dept',
                    'Case Size', 'BFC Net Cost', 'UNFI Net Unit', 'Current Price',
                    'Old Margin', 'New Margin', 'Margin Diff',
                    'Plan Margin Price', 'Apply Plan Margin',
                    'Old Profit Price', 'Apply Old Profit',
                    'Old Margin Price', 'Apply Old Margin',
                    'Generated Price', 'Updated Price', 'Updated Margin', 'Line Pricing',
                    'Last Month Volume', 'Last Month Profit', 'Projected Profit Change']]

updates.rename(columns={
    'Brand_BFC' : 'Brand',
    'Description_BFC' : 'Description',
    'BFC Net Cost' : 'Current Unit Cost',
    'UNFI Net Unit' : 'New Unit Cost'
}, inplace=True)


#updates['Department'] = updates['Department'].apply(lambda x: x.split()[0])

salesCompare = updates.copy(deep=True)


print("starting")

salesHistories = []



for index, value in salesCompare.iterrows():
    new_query = """
            select
            F01 as 'UPC',
            sum(F65) - sum(F1301) as 'Actual Profit Last Month',
            sum(F65) - (sum(F64) * %s) as 'Projected Profit'
            from STORESQL.dbo.RPT_ITM_D
            where F254 > (GetDate() - 30)
            and F01 ='%s'
            and F1034 = 3
            group by F01
            """ % (value['New Unit Cost'] ,value['UPC'])
    salesHistories.append(pd.read_sql(new_query, cnxn))
#salesHistories
print("COMPARES",salesCompare)
catted = pd.concat(salesHistories, axis=0, ignore_index=True)
catted.to_csv("histories.csv")
#groupedHistories = catted

catted['Change in Profit'] = catted['Projected Profit'] - catted['Actual Profit Last Month']

sales = salesCompare.merge(catted, how='inner', on=['UPC'])
sales = sales[['UPC',  'Description', 'Brand','Department', 'Sub-Dept',
        'Actual Profit Last Month', 'Projected Profit', 'Change in Profit','Margin Diff',
        'Old Margin', 'New Margin', 'Current Price',
        'Current Unit Cost', 'New Unit Cost'
        ]]
sales = sales.sort_values(['Change in Profit'], ascending=True)

#build the Google Sheet we will be sending to the department managers, with the Drive API
#https://googleapis.github.io/google-api-python-client/docs/dyn/drive_v3.files.html#create
# sales_workbook = drive.files().create(body={
#     'name' : 'UNFI Cost Updates - Sales Impact',
#     'parents' : ['%s' % agne_folder_id],
#     'mimeType' : 'application/vnd.google-apps.spreadsheet'
# }, fields='id').execute()
sales_workbook = createBook('UNFI Cost Updates - Sales Impact - September 2022', unfi_folder_id, 'application/vnd.google-apps.spreadsheet', drive)
#callback for Google API file ID lookups
salesId = sales_workbook.get('id', '')
sales_analysis_sheet = batchUpdate(salesId, [addSheet(sheetProp(title="Sales Analysis", gridProperties=gridProp(row=sales.shape[0]+4, column=sales.shape[1], frozenRow=4, frozenColumn=2))), deleteSheet(0)])
# response = sheets.spreadsheets().batchUpdate(
#         spreadsheetId = salesId,
#         body = {
#             "requests": {
#                 "addSheet":{
#                     "properties": {
#                     "title": "Sales Analysis",
#                     "gridProperties" : { "frozenRowCount" : 4, "frozenColumnCount" : 2, "columnCount" : sales.shape[1], "rowCount" : sales.shape[0]+4 }
#                         }
#                     }
#         }}).execute()
sales_sheetId = sales_analysis_sheet.get('replies', '')[0].get('addSheet', '').get('properties', '').get('sheetId', '')

sales_headers = sales.columns.tolist()
append(salesId, "Sales Analysis!A1:B1", "ROWS", [['Projected Profit Loss'], [""], [""],sales_headers], "USER_ENTERED")
# result = sheets.spreadsheets().values().append(
#     spreadsheetId=salesId,
#     range="Sales Analysis!A1:B1",
#     body={ "majorDimension" : "ROWS", "values" : [['Projected Profit Loss']]},
#     valueInputOption="RAW"
#         ).execute()


# result = sheets.spreadsheets().values().append(
#     spreadsheetId=salesId,
#     range="Sales Analysis!A4:B4",
#     body={ "majorDimension" : "ROWS", "values" : [sales_headers]},
#     valueInputOption="RAW"
#         ).execute()
#values
# result = sheets.spreadsheets().values().append(
#     spreadsheetId=salesId,
#     range="Sales Analysis!A5:B5",
#     body={ "majorDimension" : "ROWS", "values" : sales.to_numpy().tolist()},
#     valueInputOption="RAW"
#         ).execute()
append(salesId, "Sales Analysis!A5:B5", "ROWS", sales.to_numpy().tolist(), "USER_ENTERED")


response = sheets.spreadsheets().batchUpdate(
        spreadsheetId = salesId,
        body = {"requests": [
                # {"deleteSheet":{"sheetId" : 0}},
                {"repeatCell" :
    #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/other#GridRange
        {"range" :
            {"sheetId": sales_sheetId,
            "startRowIndex": 3,
            "endRowIndex" : 4,
            "startColumnIndex": 0,
            "endColumnIndex": sales.shape[1]},
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
                "wrapStrategy" : "OVERFLOW_CELL",
                "horizontalAlignment" : "CENTER",
                "verticalAlignment" : "MIDDLE"
                },
            },
            #https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#google.protobuf.FieldMask
            #https://cloud.google.com/blog/products/application-development/formatting-cells-with-the-google-sheets-api
            "fields" : """
                        userEnteredFormat.wrapStrategy,
                        userEnteredFormat.textFormat.bold,
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
            {"sheetId": sales_sheetId,
            "startRowIndex": 3,
            "endRowIndex" : 4,
            "startColumnIndex": sales.columns.get_loc('Actual Profit Last Month'),
            "endColumnIndex": sales.columns.get_loc('Projected Profit')+1},
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
                "backgroundColor" : {"red":39/255, "green":255/255, "blue":120/255, "alpha": 0.35},
                "wrapStrategy" : "OVERFLOW_CELL",
                "horizontalAlignment" : "CENTER",
                "verticalAlignment" : "MIDDLE"
                },
            },
            #https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#google.protobuf.FieldMask
            #https://cloud.google.com/blog/products/application-development/formatting-cells-with-the-google-sheets-api
            "fields" : """
                        userEnteredFormat.wrapStrategy,
                        userEnteredFormat.textFormat.bold,
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
            {"sheetId": sales_sheetId,
            "startRowIndex": 3,
            "endRowIndex" : 4,
            "startColumnIndex": sales.columns.get_loc('Change in Profit'),
            "endColumnIndex": sales.columns.get_loc('Margin Diff')+1},
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
                "backgroundColor" : {"red":255/255, "green":25/255, "blue":120/255, "alpha": 0.35},
                "wrapStrategy" : "OVERFLOW_CELL",
                "horizontalAlignment" : "CENTER",
                "verticalAlignment" : "MIDDLE"
                },
            },
            #https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#google.protobuf.FieldMask
            #https://cloud.google.com/blog/products/application-development/formatting-cells-with-the-google-sheets-api
            "fields" : """
                        userEnteredFormat.wrapStrategy,
                        userEnteredFormat.textFormat.bold,
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
        {
        'setBasicFilter': {
            'filter': {
                'range': {
                    "sheetId" : sales_sheetId,
                    "startRowIndex" : 3,
                    "endRowIndex" : sales.shape[0]+4,
                    "startColumnIndex" : 0,
                    "endColumnIndex" : sales.shape[1]
                }
            }
        }},
        {"repeatCell" :
    #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/other#GridRange
        {"range" :
            {"sheetId": sales_sheetId,
            "startRowIndex": 4,
            "endRowIndex" : sales.shape[0]+4,
            "startColumnIndex": 0,
            "endColumnIndex": sales.shape[1]},
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
            {"sheetId": sales_sheetId,
            "startRowIndex": 4,
            "endRowIndex" : sales.shape[0]+4,
            #https://stackoverflow.com/questions/13021654/get-column-index-from-column-name-in-python-pandas
            "startColumnIndex": sales.columns.get_loc("Margin Diff"),
            "endColumnIndex": sales.columns.get_loc("New Margin")+1},
        "cell": {
            "userEnteredFormat":{"numberFormat": { "type" : "PERCENT", "pattern" : "0.00%" }}
            },
            "fields" : """userEnteredFormat.numberFormat.type,userEnteredFormat.numberFormat.pattern"""
        }},
        {"repeatCell" :
        {"range" :
            {"sheetId": sales_sheetId,
            "startRowIndex": 4,
            "endRowIndex" : sales.shape[0]+4,
            "startColumnIndex": sales.columns.get_loc("Current Price"),
            "endColumnIndex": sales.columns.get_loc("New Unit Cost")+1},
        "cell": {
            "userEnteredFormat":{"numberFormat": {"type": "CURRENCY", "pattern" : "$0.00" }}
            },
            "fields" : """userEnteredFormat.numberFormat.type,userEnteredFormat.numberFormat.pattern"""
        }},
        {"repeatCell" :
        {"range" :
            {"sheetId": sales_sheetId,
            "startRowIndex": 4,
            "endRowIndex" : sales.shape[0]+4,
            "startColumnIndex": sales.columns.get_loc("Actual Profit Last Month"),
            "endColumnIndex": sales.columns.get_loc("Change in Profit")+1},
        "cell": {
            "userEnteredFormat":{"numberFormat": {"type": "CURRENCY", "pattern" : "$0.00" }}
            },
            "fields" : """userEnteredFormat.numberFormat.type,userEnteredFormat.numberFormat.pattern"""
        }},
        {"repeatCell":{  
                        "range" :{
                            "sheetId" : sales_sheetId,
                            "startRowIndex": 0,
                            "endRowIndex" : 1,
                            "startColumnIndex": 0,
                            "endColumnIndex": 1
                            },
                            
                        "cell" : {
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
                                    "wrapStrategy" : "OVERFLOW_CELL",
                                    "horizontalAlignment" : "CENTER",
                                    "verticalAlignment" : "MIDDLE"
                            }
                            },
                        "fields" : """
                                    userEnteredFormat.wrapStrategy,
                                    userEnteredFormat.textFormat.bold,
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
                                    userEnteredFormat.verticalAlignment
                                    """
        }},
        {"repeatCell":{  
                        "range" :{
                            "sheetId" : sales_sheetId,
                            "startRowIndex": 1,
                            "endRowIndex" : 2,
                            "startColumnIndex": 0,
                            "endColumnIndex": 1
                            },
                        "cell" : {
                            "userEnteredValue" : { "formulaValue" :  "=SUM(H5:H)"},
                            "userEnteredFormat" : {"numberFormat" : {"type" : "CURRENCY", "pattern" : "$0.00" },
                            "borders":{
                                    "top": {"style" : "SOLID"},
                                    "bottom":{"style" : "SOLID"},
                                    "left":{"style" : "SOLID"},
                                    "right":{"style" : "SOLID"}
                                    },
                            "textFormat": { "fontFamily" : "Arial", "fontSize" : 12},}
                            },
                        "fields" : """
                                    userEnteredFormat.numberFormat.type,
                                    userEnteredFormat.numberFormat.type,
                                    userEnteredFormat.numberFormat.pattern,
                                    userEnteredValue.formulaValue,
                                    userEnteredFormat.borders.top,
                                    userEnteredFormat.borders.bottom,
                                    userEnteredFormat.borders.left,
                                    userEnteredFormat.borders.right,
                                    userEnteredFormat.textFormat.fontFamily,
                                    userEnteredFormat.textFormat.fontSize
                                    """
            }},
            {
            "autoResizeDimensions":{
                "dimensions" : {
                    "sheetId" : sales_sheetId,
                    "dimension" : "COLUMNS",
                    "startIndex" : 0,
                    "endIndex" : sales.shape[0]
                    }
                }
            }
        ]}).execute()    


# full = updates.merge(prev_month, how='left', on='UPC')
# print(full)
# full = full.fillna(0)
# #full[]
# # for dept in full['Dept'].unique().tolist():
# #     dept_filter = full[full['Dept']==dept]
# #     dept_filter['Margin Based Price'] = (np.ceil(((full.New Unit Cost.astype(float)) / (1.0 - plan_margins.loc[plan_margins['Dept'] == dept, 'Planned Margin'].iloc[0]))*2) / 2)-0.01
# full['Profit Change'] = (full['Avg Profit'] - (full['Avg Revenue'] - full['New Unit Cost']))*full['Volume']

# full.to_csv('more_tests.csv', index=False)






































#updates = updates[(float(0.03)>= updates['Margin Diff'])!=(float(-0.03) <= updates['Margin Diff'])]
updates = updates[updates['Margin Diff'].abs() > 0.03]
#remove the trailing " Department" string from every department name in the list of departments

#get the list of Department names we will be looping over and sort them
depts = updates['Department'].unique().tolist()
depts.sort()
# updates = updates.merge(prev_month, on='UPC', how='inner')
# print(updates.columns.tolist())
# updates = updates.reset_index()
#print(updates.dtypes)
updates = updates.sort_values(by=['Brand', 'UPC'], key=lambda col: col.str.lower())
# print(updates.columns.tolist())

updates = updates[['UPC', 'Description', 'Brand', 'Department', 'Sub-Dept',
                    'Case Size', 'Current Unit Cost', 'New Unit Cost', 'Current Price',
                    'Old Margin', 'New Margin', 'Margin Diff',
                    'Plan Margin Price', "Apply Plan Margin",
                    'Old Profit Price', 'Apply Old Profit',
                    'Old Margin Price', 'Apply Old Margin',
                    'Generated Price', 'Updated Price', 'Updated Margin', 'Line Pricing',
                    'Last Month Volume', 'Last Month Profit', 'Projected Profit Change']]
print("FINAL",updates)
updates = updates.astype({"Last Month Volume" :float,  "Last Month Profit" : float})
updates['Projected Profit Change'] =  (updates['Last Month Volume'] * (updates['Current Price'] - updates['New Unit Cost'])) - updates['Last Month Profit']
updates = updates.fillna('')
updates = updates.astype(str)
#print(updates)
#build the Google Sheet we will be sending to the department managers, with the Drive API
#https://googleapis.github.io/google-api-python-client/docs/dyn/drive_v3.files.html#create
departments_workbook = drive.files().create(body={
    'name' : 'UNFI Cost Changes - September 2022',
    'parents' : ['%s' % unfi_folder_id],
    'mimeType' : 'application/vnd.google-apps.spreadsheet'
}, fields='id').execute()
#callback for Google API file ID lookups
deptId = departments_workbook.get('id', '')

#https://www.w3schools.com/python/python_lists_comprehension.asp
#https://stackoverflow.com/questions/51314424/extracting-the-first-word-from-every-value-in-a-list
i = 0
format_updates = []
for dept in depts:
    i+=1
    #Set Sheet title property
    title = str(dept.split()[0])

    #filter the updates to just this department
    #not exactly but close, .where and .apply(lambda) would work just as well as mine: https://stackoverflow.com/questions/40134313/conditionally-calculated-column-for-a-pandas-dataframe
    dept_updates = updates[updates['Department']==dept]
    dept_updates = dept_updates.drop(['Department'], axis=1)

    #!!!the Dataframe must by transformed to a numpy array and then to a list for the Google API to successfully read it!!!
    data = dept_updates.to_numpy().tolist()
    dept_sheet = batchUpdate(deptId, addSheet(sheetProp(title=title, gridProperties=gridProp(row=dept_updates.shape[0]+4, column=dept_updates.shape[1], frozenRow=4, frozenColumn=2))))
    #https://developers.google.com/sheets/api/samples/sheet
    # response = sheets.spreadsheets().batchUpdate(
    #     spreadsheetId = deptId,
    #     body = {
    #         "requests": {
    #             "addSheet":{
    #                 "properties": {
    #                 "title": title,
    #                 "gridProperties" : { "frozenRowCount" : 4, "frozenColumnCount" : 3, "rowCount" : dept_updates.shape[0]+4, "columnCount" : dept_updates.shape[1] }
    #                     }
    #                 }
    #     }}).execute()
    #callback ref for formatting this sheet - most annoying part of the Google API is that these responses are structred as:
    #                   Dict{            List[    Dict{                 Dict{            Dict{...}       }}]}
    sheetId = dept_sheet.get('replies', '')[0].get('addSheet', '').get('properties', '').get('sheetId', '')
    # or in other words, the 'replies' key in the API response is a list of dictionaries,
    # and most other forms of request and response with the API are dictionaries {key1 : value1, key2 : value2, ...}, not lists [value1, value2, ...]
    

    #####################
    ####DATA WRITING#####
    #####################
    headers = dept_updates.columns.tolist()
    result = sheets.spreadsheets().values().append(
        spreadsheetId=deptId,
        range=title+"!A1:B1",
        body={ "majorDimension" : "ROWS", "values" : [['Profit Change'], ["=IFERROR(SUM(X5:X),)"], [""], headers]},
        valueInputOption="USER_ENTERED"
            ).execute()
    result = sheets.spreadsheets().values().append(
        spreadsheetId=deptId,
        range=title+"!A5:B5",
        body={ "majorDimension" : "ROWS", "values" : data},
        valueInputOption="USER_ENTERED"
            ).execute()
    # result = sheets.spreadsheets().values().append(
    #     spreadsheetId=deptId,
    #     range=title+"!A4:B4",
    #     body={ "majorDimension" : "ROWS", "values" : [headers]},
    #     valueInputOption="RAW"
    #         ).execute()
    #values
    # result = sheets.spreadsheets().values().append(
    #     spreadsheetId=deptId,
    #     range=title+"!A5:B5",
    #     body={ "majorDimension" : "ROWS", "values" : data},
    #     valueInputOption="RAW"
    #         ).execute()

    format_updates.extend([
        repeatCell(
            gridRange(sheetId, 0, 1, 0, 1),
            cellData(userEnteredFormat=cellFormat(
                borders=borders(*all_thick),
                textFormat=textFormat(fontFamily="Arial", fontSize=14, bold=True),
                backgroundColor=getColor(39, 175, 245, 0.35),
                horizontalAlignment="CENTER",
                verticalAlignment="MIDDLE"
                ))
        ),
        repeatCell(
            gridRange(sheetId, 3, 4, 0, dept_updates.shape[1]),
            cellData(userEnteredFormat=cellFormat(
                borders=borders(*all_thick),
                textFormat=textFormat(fontFamily="Arial", fontSize=14, bold=True),
                backgroundColor=getColor(39, 175, 245, 0.35),
                horizontalAlignment="CENTER",
                verticalAlignment="MIDDLE"
                ))
        ),
        repeatCell(
            gridRange(sheetId, 4, dept_updates.shape[0]+4, 0, dept_updates.shape[1]),
            cellData(userEnteredFormat=cellFormat(borders=borders(*all_solid), textFormat=textFormat(fontFamily="Arial", fontSize=12)))
        ),
        veryBasicFilter(gridRange(sheetId, 3, dept_updates.shape[0]+4, 0, dept_updates.shape[1])),
        repeatCell(
            gridRange(sheetId, 4, dept_updates.shape[0]+4, dept_updates.columns.get_loc("Old Margin"), dept_updates.columns.get_loc("Margin Diff")+1),
            cellData(userEnteredFormat=cellFormat(numberFormat=numberFormat("PERCENT", "0.00%")))
        ),
        repeatCell(
            gridRange(sheetId, 4, dept_updates.shape[0]+4, dept_updates.columns.get_loc("Current Unit Cost"), dept_updates.columns.get_loc("Current Price")),
            cellData(userEnteredFormat=cellFormat(numberFormat=numberFormat("NUMBER", "0.00")))
        ),
        repeatCell(
            gridRange(sheetId, 4, dept_updates.shape[0]+4, dept_updates.columns.get_loc("Last Month Profit"), dept_updates.columns.get_loc("Projected Profit Change")),
            cellData(userEnteredFormat=cellFormat(numberFormat=numberFormat("NUMBER", "0.00")))
        ),
        addConditional(rule(
            gridRange(sheetId, 4, dept_updates.shape[0]+4, dept_updates.columns.get_loc("Margin Diff"), dept_updates.columns.get_loc("Margin Diff")+1),
            boolRule(boolCond("NUMBER_GREATER", [condVal("0")]), cellFormat(backgroundColor=greenVal))), 0),
        addConditional(rule(
            gridRange(sheetId, 4, dept_updates.shape[0]+4, dept_updates.columns.get_loc("Margin Diff"), dept_updates.columns.get_loc("Margin Diff")+1),
            boolRule(boolCond("NUMBER_LESS", [condVal("0")]), cellFormat(backgroundColor=redVal))), 0),
        repeatCell(
            gridRange(sheetId, 4, dept_updates.shape[0]+4, dept_updates.columns.get_loc("Updated Margin"), dept_updates.columns.get_loc("Updated Margin")+1),
            cellData(userEnteredValue=extendedValue(formulaValue="=IF(ISBLANK(P5)=FALSE, (P5-G5)/P5, \"\")"),userEnteredFormat=cellFormat(numberFormat=numberFormat("PERCENT", "0.00%")))
        ),
        autoSize(sheetId, "COLUMNS")

# "repeatCell":{  
#                     "range" :{
#                         "sheetId" : sheetId,
#                         "startRowIndex": 4,
#                         "endRowIndex" : dept_updates.shape[0]+4,
#                         "startColumnIndex": dept_updates.columns.get_loc("Updated Margin"),
#                         "endColumnIndex": dept_updates.columns.get_loc("Updated Margin")+1
#                         },
#                         #"=(R[0]C[-1]-R[0]C[-6]/R[0]C[-1])"
#                     "cell" : {
#                         "userEnteredValue" : { "formulaValue" :  "=IF(ISBLANK(L2)=FALSE, (L2-G2)/L2, \"\")"},
#                         "userEnteredFormat" : {"numberFormat" : {"type" : "PERCENT", "pattern" : "0.00%" }}
#                         },
#                     "fields" : """
#                                 userEnteredFormat.numberFormat.type,
#                                 userEnteredFormat.numberFormat.pattern,
#                                 userEnteredValue.formulaValue
#                                 """

# addConditional(rule(gridRange(sheet_id, 2, df.shape[0]+2, i[1]-3, i[1]-2),
# boolRule(boolCond("CUSTOM_FORMULA", [condVal("=INDIRECT(\"R[0]C[-%s]\", FALSE)>INDIRECT(\"R[0]C[0]\", FALSE)" % ninety_day_offset[0])]), cellFormat(backgroundColor=greenVal))), 0)

#         "requests": {
#                 "addConditionalFormatRule":{
#                     "rule":{
#                         "ranges" :[{
#                             "sheetId" : sheetId,
#                             "startRowIndex": 4,
#                             "endRowIndex" : dept_updates.shape[0]+4,
#                             "startColumnIndex": dept_updates.columns.get_loc("Margin Diff"),
#                             "endColumnIndex": dept_updates.columns.get_loc("Margin Diff")+1
#                             }],
#                         "booleanRule" : {
#                             "condition" : { "type" : "NUMBER_GREATER", "values" : [{"userEnteredValue" : "0"}]},
#                             "format" : {"backgroundColor" : {"red":50/255, "green":248/255, "blue":40/255, "alpha": 0.05}}
#                             }
#                         },
#                         "index" : 0
#                     }
    ])
    # response = sheets.spreadsheets().batchUpdate(
    #     spreadsheetId = deptId,
    #     body = {"requests" : [
    # #         {"repeatCell" :
    # # #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/other#GridRange
    # #     {"range" :
    # #         {"sheetId": sheetId,
    # #         "startRowIndex": 0,
    # #         "endRowIndex" : 1,
    # #         "startColumnIndex": 0,
    # #         "endColumnIndex": 1},
    # #     #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/cells#CellData
    # #     "cell": {
    # #         "userEnteredFormat":{
    # #             "borders":{
    # #                 "top": {"style" : "SOLID_THICK"},
    # #                 "bottom":{"style" : "SOLID_THICK"},
    # #                 "left":{"style" : "SOLID_THICK"},
    # #                 "right":{"style" : "SOLID_THICK"}
    # #                 },
    # #             "textFormat": { "fontFamily" : "Arial", "fontSize" : 14, "bold" : True },
    # #             #https://rgbacolorpicker.com/
    # #             "backgroundColor" : {"red":39/255, "green":175/255, "blue":245/255, "alpha": 0.35}
    # #             },
    # #         },
    # #         #https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#google.protobuf.FieldMask
    # #         #https://cloud.google.com/blog/products/application-development/formatting-cells-with-the-google-sheets-api
    # #         "fields" : """userEnteredFormat.textFormat.bold,
    # #                     userEnteredFormat.backgroundColor.red,
    # #                     userEnteredFormat.backgroundColor.green,
    # #                     userEnteredFormat.backgroundColor.blue,
    # #                     userEnteredFormat.backgroundColor.alpha,
    # #                     userEnteredFormat.textFormat.fontFamily,
    # #                     userEnteredFormat.textFormat.fontSize,
    # #                     userEnteredFormat.borders.top,
    # #                     userEnteredFormat.borders.bottom,
    # #                     userEnteredFormat.borders.left,
    # #                     userEnteredFormat.borders.right"""
    # #     }},
    #     {"repeatCell" :
    # #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/other#GridRange
    #     {"range" :
    #         {"sheetId": sheetId,
    #         "startRowIndex": 3,
    #         "endRowIndex" : 4,
    #         "startColumnIndex": 0,
    #         "endColumnIndex": dept_updates.shape[1]},
    #     #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/cells#CellData
    #     "cell": {
    #         "userEnteredFormat":{
    #             "borders":{
    #                 "top": {"style" : "SOLID_THICK"},
    #                 "bottom":{"style" : "SOLID_THICK"},
    #                 "left":{"style" : "SOLID_THICK"},
    #                 "right":{"style" : "SOLID_THICK"}
    #                 },
    #             "textFormat": { "fontFamily" : "Arial", "fontSize" : 14, "bold" : True },
    #             #https://rgbacolorpicker.com/
    #             "backgroundColor" : {"red":39/255, "green":175/255, "blue":245/255, "alpha": 0.35}
    #             },
    #         },
    #         #https://developers.google.com/protocol-buffers/docs/reference/google.protobuf#google.protobuf.FieldMask
    #         #https://cloud.google.com/blog/products/application-development/formatting-cells-with-the-google-sheets-api
    #         "fields" : """userEnteredFormat.textFormat.bold,
    #                     userEnteredFormat.backgroundColor.red,
    #                     userEnteredFormat.backgroundColor.green,
    #                     userEnteredFormat.backgroundColor.blue,
    #                     userEnteredFormat.backgroundColor.alpha,
    #                     userEnteredFormat.textFormat.fontFamily,
    #                     userEnteredFormat.textFormat.fontSize,
    #                     userEnteredFormat.borders.top,
    #                     userEnteredFormat.borders.bottom,
    #                     userEnteredFormat.borders.left,
    #                     userEnteredFormat.borders.right"""
    #     }},
    #     ]}).execute()

           
                
    

    
    # #####################
    # ###DATA FORMATTING###
    # #####################
    # #borders
    # #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/request#RepeatCellRequest
    # border_body = {"requests" : {"repeatCell" :
    # #https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/other#GridRange
    #     {"range" :
    #         {"sheetId": sheetId,
    #         "startRowIndex": 4,
    #         "endRowIndex" : dept_updates.shape[0]+4,
    #         "startColumnIndex": 0,
    #         "endColumnIndex": dept_updates.shape[1]},
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
    #     }}}
    # #format percent numbers
    # percent_body = {"requests" : [{"repeatCell" :
    #     {"range" :
    #         {"sheetId": sheetId,
    #         "startRowIndex": 4,
    #         "endRowIndex" : dept_updates.shape[0]+4,
    #         #https://stackoverflow.com/questions/13021654/get-column-index-from-column-name-in-python-pandas
    #         "startColumnIndex": dept_updates.columns.get_loc("Old Margin"),
    #         "endColumnIndex": dept_updates.columns.get_loc("Margin Diff")+1},
    #     "cell": {
    #         "userEnteredFormat":{"numberFormat": { "type" : "PERCENT", "pattern" : "0.00%" }}
    #         },
    #         "fields" : """userEnteredFormat.numberFormat.type,userEnteredFormat.numberFormat.pattern"""
    #     }},
    #     {
    #     'setBasicFilter': {
    #         'filter': {
    #             'range': {
    #                 "sheetId" : sheetId,
    #                 "startRowIndex" : 3,
    #                 "endRowIndex" : dept_updates.shape[0]+4,
    #                 "startColumnIndex" : 0,
    #                 "endColumnIndex" : dept_updates.shape[1]
    #             }
    #         }
    #     }},]}
    # #seems like people prefer numbers that look like currency numbers without the leading $ symbol, so this is not actually a currency formatting but serves that purpose
    # currency_like_body = {"requests" : {"repeatCell" :
    #     {"range" :
    #         {"sheetId": sheetId,
    #         "startRowIndex": 4,
    #         "endRowIndex" : dept_updates.shape[0]+4,
    #         "startColumnIndex": dept_updates.columns.get_loc("Current Price"),
    #         "endColumnIndex": dept_updates.columns.get_loc("Current Price")},
    #     "cell": {
    #         "userEnteredFormat":{"numberFormat": {"type": "NUMBER", "pattern" : "0.00" }}
    #         },
    #         "fields" : """userEnteredFormat.numberFormat.type,userEnteredFormat.numberFormat.pattern"""
    #     }}}
    # updated_price_body = {"requests" : {"repeatCell" :
    #     {"range" :
    #         {"sheetId": sheetId,
    #         "startRowIndex": 4,
    #         "endRowIndex" : dept_updates.shape[0]+4,
    #         "startColumnIndex": dept_updates.columns.get_loc("Last Month Profit"),
    #         "endColumnIndex": dept_updates.columns.get_loc("Projected Profit Change")
    #         },
    #     "cell": {
    #         "userEnteredFormat":{"numberFormat": {"type": "CURRENCY", "pattern" : "0.00" }}
    #         },
    #         "fields" : """userEnteredFormat.numberFormat.type,userEnteredFormat.numberFormat.pattern"""
    #     }}}
    # # textformat_body = {"requests" : {"repeatCell" :
    # #     {"range" :
    # #         {"sheetId": sales_sheetId,
    # #         "startRowIndex": 1,
    # #         "endRowIndex" : dept_updates.shape[0]+1,
    # #         "startColumnIndex": 0,
    # #         "endColumnIndex": dept_updates.shape[1]},
    # #     "cell": {
    # #         "userEnteredFormat":{}
    # #         },
    # #         "fields" : """userEnteredFormat.textFormat.fontFamily, userEnteredFormat.textFormat.fontSize"""
    # #     }}}
    # response = sheets.spreadsheets().batchUpdate(spreadsheetId = deptId, body = border_body).execute()
    # response = sheets.spreadsheets().batchUpdate(spreadsheetId = deptId, body = percent_body).execute()
    # response = sheets.spreadsheets().batchUpdate(spreadsheetId = deptId, body = currency_like_body).execute()
    # response = sheets.spreadsheets().batchUpdate(spreadsheetId = deptId, body = updated_price_body).execute()
    # #####################
    # #####################
    # #####################

    # response = sheets.spreadsheets().batchUpdate(
    #     spreadsheetId = deptId,
    #     body = {
    #         "requests": {
    #             "addConditionalFormatRule":{
    #                 "rule":{
    #                     "ranges" :[{
    #                         "sheetId" : sheetId,
    #                         "startRowIndex": 4,
    #                         "endRowIndex" : dept_updates.shape[0]+4,
    #                         "startColumnIndex": dept_updates.columns.get_loc("Margin Diff"),
    #                         "endColumnIndex": dept_updates.columns.get_loc("Margin Diff")+1
    #                         }],
    #                     "booleanRule" : {
    #                         "condition" : { "type" : "NUMBER_GREATER", "values" : [{"userEnteredValue" : "0"}]},
    #                         "format" : {"backgroundColor" : {"red":50/255, "green":248/255, "blue":40/255, "alpha": 0.05}}
    #                         }
    #                     },
    #                     "index" : 0
    #                 }
    #     }}).execute()


    # response = sheets.spreadsheets().batchUpdate(
    #     spreadsheetId = deptId,
    #     body = {
    #     "requests": {
    #         "addConditionalFormatRule":{
    #             "rule":{    
    #                 "ranges" :[{
    #                     "sheetId" : sheetId,
    #                     "startRowIndex": 4,
    #                     "endRowIndex" : dept_updates.shape[0]+4,
    #                     "startColumnIndex": dept_updates.columns.get_loc("Margin Diff"),
    #                     "endColumnIndex": dept_updates.columns.get_loc("Margin Diff")+1
    #                     }],
    #                 "booleanRule" : {
    #                     "condition" : { "type" : "NUMBER_LESS", "values" : [{"userEnteredValue" : "0"}]},
    #                     "format" : {"backgroundColor" : {"red":225/255, "green":40/255, "blue":40/255, "alpha": 0.05}}
    #                     }
    #                 },
    #                 "index" : 0
    #             }
    #     }}).execute() 

    # response = sheets.spreadsheets().batchUpdate(
    #     spreadsheetId = deptId,
    #     body = {
    #     "requests": [{
    #         "repeatCell":{  
    #                 "range" :{
    #                     "sheetId" : sheetId,
    #                     "startRowIndex": 4,
    #                     "endRowIndex" : dept_updates.shape[0]+4,
    #                     "startColumnIndex": dept_updates.columns.get_loc("Updated Margin"),
    #                     "endColumnIndex": dept_updates.columns.get_loc("Updated Margin")+1
    #                     },
    #                     #"=(R[0]C[-1]-R[0]C[-6]/R[0]C[-1])"
    #                 "cell" : {
    #                     "userEnteredValue" : { "formulaValue" :  "=IF(ISBLANK(L2)=FALSE, (L2-G2)/L2, \"\")"},
    #                     "userEnteredFormat" : {"numberFormat" : {"type" : "PERCENT", "pattern" : "0.00%" }}
    #                     },
    #                 "fields" : """
    #                             userEnteredFormat.numberFormat.type,
    #                             userEnteredFormat.numberFormat.pattern,
    #                             userEnteredValue.formulaValue
    #                             """
                    
        
    #     }}]}).execute()

    # response = sheets.spreadsheets().batchUpdate(
    #     spreadsheetId = deptId,
    #     body = {
    #         "requests": {
    #             "autoResizeDimensions": {
    #                 "dimensions": {
    #                     "sheetId": sheetId,
    #                     "dimension": "COLUMNS",
    #                     "startIndex": 0,
    #                     "endIndex": dept_updates.shape[1]
    #                     }
    #                 }
    #             }
    #         }).execute()  
    
    #if(i != len(depts)): time.sleep(10)

batchUpdate(deptId, format_updates)


