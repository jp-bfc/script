import openpyxl
import tkinter as tk
from tkinter import filedialog
from pathlib import Path
import datetime
import pyodbc
import pandas as pd
import numpy as np
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
#from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient import discovery
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import os
import configparser
import time





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

config = configparser.ConfigParser()
config.read('script_configs.ini')
username = config['DEFAULT']['user']
password = config['DEFAULT']['password']
server = config['DEFAULT']['server']
port = config['DEFAULT']['port']
database = config['DEFAULT']['database']
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';PORT='+port+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()

PICS_Folder_Q1FY23 = config['DEFAULT']['pics_folder_FY23Q1']

query="""
select obj.F01 as 'UPC', obj.F155 as 'Brand', obj.F29 as 'Desc', obj.F22 as 'Size', rpc.F1024 as 'Dept', sdp.F1022 as 'Sub-dept', prc.F30 as 'Price', cos.F1140 as 'Unit Cost', btl.F50 as 'Bottle Deposit'
from STORESQL.dbo.OBJ_TAB obj
left join STORESQL.dbo.POS_TAB pos on OBJ.F01 = pos.F01
inner join STORESQL.dbo.RPC_TAB rpc on obj.F18 = rpc.F18
left join STORESQL.dbo.sdp_tab sdp on pos.F04 = sdp.F04
inner join STORESQL.dbo.PRICE_TAB prc on obj.F01 = prc.F01
left join STORESQL.dbo.COST_TAB cos on OBJ.F01 = cos.f01
left join STORESQL.dbo.BTL_TAB btl on pos.F05 = btl.F05
"""

df = pd.read_sql(query, cnxn)
#df.rename(columns={"UPC" : "BFC UPC"}, errors="raise")
#print(df)

root = tk.Tk()
root.withdraw()

file_path = filedialog.askopenfilename()

# section_frame = pd.DataFrame(data={
# 101 : 'front end',
# 102 : 'Gifts',
# 103 : 'rear ends',
# 104 : 'power bars',
# 201 : 'front end',
# 202 : 'lotion',
# 203 : 'rear ends',
# 204 : 'hair',
# 301 : 'front end',
# 302 : 'pain',
# 303 : 'rear ends',
# 304 : 'soap',
# 401 : 'front end',
# 402 : 'vitamins',
# 403 : 'rear ends',
# 404 : 'allergy',
# 501 : 'front end',
# 502 : 'probiotic',
# 503 : 'dni rear ends',
# 504 : 'vitamins',
# 601 : 'front end',
# 602 : 'wine (not cases)',
# 603 : 'rear ends',
# 604 : 'gifts/cards',
# 701 : 'front end',
# 702 : 'wine',
# 703 : 'rear end',
# 704 : 'wine',
# 801 : 'front end',
# 802 : 'beer',
# 803 : 'rear end',
# 804 : 'beer',
# 901 : 'front end',
# 902 : 'rice cakes',
# 903 : 'rear end',
# 904 : 'bread',
# 1001 : 'front end',
# 1002 :  'oil',
# 1003 :  'rear end',
# 1004 :  'peanut butter',
# 1101 :  'front end',
# 1102 :  'international',
# 1103 :  'read end',
# 1104 :  'pasta',
# 1201 :  'front end',
# 1202 :  'water',
# 1203 :  'rear end',
# 1204 :  'cereal',
# 1301 :  'front end',
# 1302 :  'pet',
# 1303 :  'rear end',
# 1304 :  'tea',
# 1401 :  'front end',
# 1402 :  'FZN',
# 1403 :  'rear end',
# 1404 :  'FZN',
# 6001 :  'Reg 1/cust service',
# 6002 :  'Reg 2',
# 6003 :  'Reg 3',
# 6004 :  'Reg 4',
# 6005 : "",
# 6006 : "",
# 6100 :  'vestibule',
# 6200 :  'HBA case',
# 6300 :  'cheese case',
# 6400 :  'rear case',
# 7000 :  'produce',
# 7002 :  'side wall cooler',
# 7003 :  'rear wall and stacks',
# 7004 :  'dairy'
# }, columns=["SectionID", "Area"])

pics = pd.read_csv(file_path, dtype='str', parse_dates=['TimeStamp'])
pics['UPC'] = pics['UPC'].str.zfill(13)
pics['Qty'] = pd.to_numeric(pics['Qty'])
pics['Qty'] = pics['Qty'].astype(float)
pics['Price'] = pics['Price'].astype(float)
pics['Cost'] = pics['Cost'].astype(float)
section_frame = pd.read_csv("fiddling.txt", dtype="str")
total_book = createBook("PICS Reports - Totals", PICS_Folder_Q1FY23, 'application/vnd.google-apps.spreadsheet', drive)
tb_id = total_book.get('id', '')
print(section_frame)

#print (pics)
merged = pd.merge(pics, df, how="inner", on="UPC", suffixes=["_pics", "_bfc"])
merged_with_sections = pd.merge(merged, section_frame, how="inner", left_on="Section", right_on="Area")

#print (merged_with_sections)
merged_with_sections = merged_with_sections.drop("Area", axis=1)
print(merged_with_sections)
merged_with_sections['Total Price'] = merged_with_sections['Qty'] * merged_with_sections['Price_bfc']
merged_with_sections['Total Cost'] = merged_with_sections['Qty'] * merged_with_sections['Unit Cost']
merged_with_sections['PICS Price'] = merged_with_sections['Qty'] * merged_with_sections['Price_pics']
merged_with_sections['PICS Cost'] = merged_with_sections['Qty'] * merged_with_sections['Cost']
merged_with_sections['Bottle Deposit'] = merged_with_sections['Qty'] * merged_with_sections['Bottle Deposit']
#merged_with_sections = merged_with_sections.reset_index()
print(merged_with_sections.columns)
depts = merged_with_sections[['UPC', 'Qty', 'Section', 'Section Name', 'Brand', 'Desc', 'Bottle Deposit', 'Size', 'Dept', 'Sub-dept', 'Price_bfc', 'Unit Cost', 'Total Price', 'Total Cost']]

dept_list = merged_with_sections['Dept'].unique().tolist()
print(dept_list)

for dept in dept_list:
    print(dept)
    if dept != "" and dept is not None and dept is not np.nan:
        print()
        print(dept)
        format_updates = []
        dept_book = createBook(dept + " PICS Inventory Count - Q1 FY2023", PICS_Folder_Q1FY23, 'application/vnd.google-apps.spreadsheet', drive)
        dept_id = dept_book.get('id', '')
        dept_frame = depts[depts['Dept'] == dept]
        dept_frame = dept_frame.drop(['Dept'], axis=1)
        sum_frame = dept_frame.groupby('Sub-dept').sum().reset_index()
        

        total_price = sum_frame['Total Price'].sum()
        total_cost = sum_frame['Total Cost'].sum()
        deposits = sum_frame['Bottle Deposit'].sum()
        #print(sum_frame['Sub-Dept', 'Total Price', 'Total Cost', 'Bottle Deposit'].values.tolist())
        # print(total_price, total_cost, deposits)
        # print(sum_frame, sum_frame.columns.dtype)
        dept_frame.fillna("", inplace=True)
        sum_frame.fillna("", inplace=True)
        dept_frame = dept_frame.astype(str)
        dept_frame = dept_frame.sort_values(['Brand', 'UPC'], ascending=[True, True])
        sum_frame = sum_frame.astype(str)
        sheet = batchUpdate(dept_id, body=addSheet(sheetProp(title=dept + " PICS", gridProperties=gridProp(dept_frame.shape[0]+1, 13, 1))))
        sheet_id = sheet.get('replies', '')[0].get('addSheet', '').get('properties', '').get('sheetId', '')
        sum_sheet = batchUpdate(dept_id, body=addSheet(sheetProp(title="Summary", gridProperties=gridProp(sum_frame.shape[0]+8, 5, 3))))
        sum_sheet_id = sum_sheet.get('replies', '')[0].get('addSheet', '').get('properties', '').get('sheetId', '')
        append(
            dept_id, dept + " PICS!A1:B1",
            "ROWS",
            [["UPC","Qty","Section","Section Name", "Brand","Desc","Bottle Deposit","Size","Sub-dept","Price","Unit Cost","Total Price","Total Cost"]],
            "RAW"
        )
        append(dept_id, dept + " PICS!A1:B1", "ROWS", dept_frame.values.tolist(),"USER_ENTERED")
        
        
        append(dept_id, "Summary!A1:B1", "ROWS",
        [[dept, "Q1 FY2023"],
        [],
        ["Sub-Dept", "Total Retail Value", "Total Cost of Goods", "Bottle Deposits", "Margin"]],"RAW")

        append(dept_id, "Summary!A3:B3", "ROWS",sum_frame[['Sub-dept', 'Total Price', 'Total Cost', 'Bottle Deposit']].values.tolist(),"USER_ENTERED")

        append(dept_id, "Summary!A3:B3", "ROWS",
        [["Total PICS", total_price, total_cost ],
        ["Total Backstock", ""],
        ["Complete", ""],
        ['PICS Bottle Deposits', deposits],
        ["Backstock Bottle Deposits", ""]], "USER_ENTERED")
        

        format_updates.extend([
            #Report
            repeatCell(
                gridRange(sheet_id, 0, 1, 0, 4),
                cellData(userEnteredFormat=cellFormat(
                    borders=borders(*all_thick),
                    textFormat=textFormat(fontFamily="Arial", fontSize=14, bold=True),
                    backgroundColor=getColor(225, 75, 0, 0.01),
                    horizontalAlignment="CENTER",
                    verticalAlignment="MIDDLE"
                    ))
                ),
            repeatCell(
                gridRange(sheet_id, 0, 1, 4, 11),
                cellData(userEnteredFormat=cellFormat(
                    borders=borders(*all_thick),
                    textFormat=textFormat(fontFamily="Arial", fontSize=14, bold=True),
                    backgroundColor=getColor(225, 225, 25, 0.1),
                    horizontalAlignment="CENTER",
                    verticalAlignment="MIDDLE"
                    ))
                ),
            repeatCell(
                gridRange(sheet_id, 0, 1, 11, 13),
                cellData(userEnteredFormat=cellFormat(
                    borders=borders(*all_thick),
                    textFormat=textFormat(fontFamily="Arial", fontSize=14, bold=True),
                    backgroundColor=getColor(50, 255, 50, 0.01),
                    horizontalAlignment="CENTER",
                    verticalAlignment="MIDDLE"
                    ))
                ),
            repeatCell(
                gridRange(sheet_id, 1, dept_frame.shape[0]+1, 0, 13),
                cellData(userEnteredFormat=cellFormat(
                    borders=borders(*all_solid),
                    textFormat=textFormat(fontFamily="Arial", fontSize=12)
                    ))
                ),
            repeatCell(
                gridRange(sheet_id, 1, dept_frame.shape[0]+1, 6, 7),
                cellData(userEnteredFormat=cellFormat(numberFormat=currency))
                ),
            repeatCell(
                gridRange(sheet_id, 1, dept_frame.shape[0]+1, 7, 13),
                cellData(userEnteredFormat=cellFormat(numberFormat=currency))
                ),
            veryBasicFilter(gridRange(sheet_id, 0, dept_frame.shape[0]+1, 0, 12)),
            autoSize(sheet_id, "COLUMNS"),
            #Summary
            repeatCell(
                gridRange(sum_sheet_id, 0, 1, 0, 2),
                cellData(userEnteredFormat=cellFormat(
                    textFormat=textFormat(fontFamily="Arial", fontSize=14, bold=True),
                    horizontalAlignment="CENTER",
                    verticalAlignment="MIDDLE"
                    ))
                ),
            repeatCell(
                gridRange(sum_sheet_id, 2, 3, 0, 5),
                cellData(userEnteredFormat=cellFormat(
                    borders=borders(*all_thick),
                    textFormat=textFormat(fontFamily="Arial", fontSize=14, bold=True),
                    backgroundColor=getColor(225, 75, 0, 0.01),
                    horizontalAlignment="CENTER",
                    verticalAlignment="MIDDLE"
                    ))
                ),
            repeatCell(
                gridRange(sum_sheet_id, 3, sum_frame.shape[0]+3, 0, 5),
                cellData(userEnteredFormat=cellFormat(
                    borders=borders(*all_solid),
                    textFormat=textFormat(fontFamily="Arial", fontSize=12)
                    ))
                ),
            repeatCell(
                gridRange(sum_sheet_id, 3, sum_frame.shape[0]+3, 0, 5),
                cellData(userEnteredFormat=cellFormat(
                    borders=borders(*all_solid),
                    textFormat=textFormat(fontFamily="Arial", fontSize=12)
                    ))
                ),
            repeatCell(gridRange(sum_sheet_id, 3, sum_frame.shape[0]+4, 4, 5), cellData(extendedValue(formulaValue=f"=((B4-C4)/B4)"))),
            repeatCell(
                gridRange(sum_sheet_id, sum_frame.shape[0]+3, sum_frame.shape[0]+6, 0, 5),
                cellData(userEnteredFormat=cellFormat(
                    borders=borders(*all_thick),
                    textFormat=textFormat(fontFamily="Arial", fontSize=12, bold=True)
                    ))
                ),
            repeatCell(
                gridRange(sum_sheet_id, sum_frame.shape[0]+6, sum_frame.shape[0]+8, 0, 2),
                cellData(userEnteredFormat=cellFormat(
                    borders=borders(*all_thick),
                    textFormat=textFormat(fontFamily="Arial", fontSize=12)
                    ))
                ),
            repeatCell(
                gridRange(sum_sheet_id, 3, sum_frame.shape[0]+8, 1, 4),
                cellData(userEnteredFormat=cellFormat(numberFormat=currency))
                ),
            repeatCell(
                gridRange(sum_sheet_id, 3, sum_frame.shape[0]+8, 4, 5),
                cellData(userEnteredFormat=cellFormat(numberFormat=percent))
                ),
            veryBasicFilter(gridRange(sum_sheet_id, 2, sum_frame.shape[0]+3, 0, 5)),
            autoSize(sum_sheet_id, "COLUMNS"),
            deleteSheet(0)
            ])
        batchUpdate(dept_id, body=format_updates)
        time.sleep(10)
    else:
        print("yeah")
        merged_sheet = batchUpdate(tb_id, body=addSheet(sheetProp(title="Missing Department Items", gridProperties=gridProp(merged.shape[0]+1, 15, 1))))
        merged_sheet_id = merged_sheet.get('replies', '')[0].get('addSheet', '').get('properties', '').get('sheetId', '')
        dept_frame = merged[merged['Dept'].isnull()]
        dept_frame.fillna("", inplace=True)
        dept_frame = dept_frame.astype(str)
        print(dept_frame.columns.values.tolist())
        append(tb_id, "Missing Department Items!A1:B1", "ROWS", [dept_frame.columns.tolist()],"RAW")
        append(tb_id, "Missing Department Items!A1:B1", "ROWS", dept_frame.values.tolist(),"RAW")

# file_name = 'Quarterly Inventory Summary Report - Q42021.xlsx'
# file_path = Path('//bfc-hv-01/SWAP/New Reports/%s' % file_name)
# file_path.touch(exist_ok=True)
# writer = pd.ExcelWriter(file_path, engine='openpyxl')
# merged.to_excel(writer, sheet_name='Data')
grouped = merged.groupby('Dept').sum().reset_index()
grouped.fillna("", inplace=True)
grouped = grouped.astype(str)
grouped_sheet = batchUpdate(tb_id, body=addSheet(sheetProp(title="Department Summary", gridProperties=gridProp(grouped.shape[0]+1, 10, 1))))
grouped_sheet_id = grouped_sheet.get('replies', '')[0].get('addSheet', '').get('properties', '').get('sheetId', '')
print(grouped.columns.tolist())
append(tb_id, "Department Summary!A1:B1", "ROWS", [grouped.columns.tolist()], "RAW")
append(tb_id, "Department Summary!A1:B1", "ROWS", grouped.values.tolist(), "RAW")
# grouped.to_excel(writer, sheet_name='Dept Summary')
# writer.save()



raw_sheet = batchUpdate(tb_id, body=addSheet(sheetProp(title="Raw", gridProperties=gridProp(pics.shape[0]+1, 7, 1))))
raw_id = raw_sheet.get('replies', '')[0].get('addSheet', '').get('properties', '').get('sheetId', '')
pics.fillna("", inplace=True)
pics = pics.astype(str)
#append(tb_id, "Raw!A1:B1", "ROWS", [pics.columns.tolist()], "RAW")
append(tb_id, "Raw!A1:B1", "ROWS", pics.values.tolist(), "RAW")

merged_with_sections.fillna("", inplace=True)
merged_with_sections = merged_with_sections.astype(str)
merged_with_sections_sheet = batchUpdate(tb_id, body=addSheet(sheetProp(title="Merged", gridProperties=gridProp(merged_with_sections.shape[0]+1, 7, 1))))
append(tb_id, "Merged!A1:B1", "ROWS", [merged_with_sections.columns.tolist()], "RAW")
append(tb_id, "Merged!A1:B1", "ROWS", merged_with_sections.values.tolist(), "RAW")