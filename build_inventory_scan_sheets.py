import configparser
from operator import add
from timeit import repeat
from turtle import color
from types import NoneType
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
##https://pypi.org/project/pyodbc/
import pyodbc
#https://pandas.pydata.org/docs/reference/index.html
import pandas as pd
import os
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
#from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient import discovery
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import time
import numpy as np
import calendar

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

department_folder_id_FY23Q1 = config['DEFAULT']['department_folder_FY23Q1']

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

departments = {
#'Marketing' : [21]
#'CBM' : [19, 6, 16, 11],
# 'Haba' : [6],
# 'Housewares' : [16],
# 'Supplements' : [11],
# 'Beer' : [4, 13],
# 'Bulk' : [5],
# 'Cheese' : [10],
'Dairy' : [3],
# 'Deli' : [8],
# 'Floral' : [18],
# 'Frozen' : [15],
'Grocery' : [1],
# 'Meat' : [9],
# 'Produce' : [2],
# 'Seafood' : [22],
# 'Taxable' : [7],
# 'TCH' : [17]
}


for group in departments: 
    inventory_frame = pd.read_sql("""
                    select obj.F01 as 'UPC', obj.F155 as 'Brand', obj.F29 as 'Desc',
                    obj.F22 as 'Size', rpc.F1024 as 'Dept', sdp.F1022 as 'Sub-dept',
                    prc.F30 as 'Price', cos.F1140 as 'Unit Cost', pos.F123 as 'PLU'
                    from STORESQL.dbo.OBJ_TAB obj
                    left join STORESQL.dbo.POS_TAB pos on OBJ.F01 = pos.F01
                    inner join STORESQL.dbo.RPC_TAB rpc on obj.F18 = rpc.F18
                    left join STORESQL.dbo.sdp_tab sdp on pos.F04 = sdp.F04
                    inner join STORESQL.dbo.PRICE_TAB prc on obj.F01 = prc.F01
                    left join STORESQL.dbo.COST_TAB cos on OBJ.F01 = cos.f01
                    where rpc.F18 in (%s)""" % (','.join([str(x) for x in departments[group]])), cnxn)

    if group in ["Produce", "Seafood", "Cheese", "Meat"]:
        subdept_frame = inventory_frame.copy(True)
        sub_book = createBook(group + " FY23 Q1 Scanning Sheet by Sub-Dept", department_folder_id_FY23Q1, 'application/vnd.google-apps.spreadsheet', drive)
        print(sub_book)
        sub_book_id = sub_book.get('id', '')
        format_updates = []
        subdept_frame = subdept_frame.drop(['Dept', 'Brand'], axis=1)
        subs = subdept_frame['Sub-dept'].unique().tolist()
        subs.sort()
        subdept_frame = subdept_frame[['UPC', 'Desc', 'PLU', 'Size', 'Unit Cost', 'Price', 'Sub-dept']]
        subdept_frame.fillna("", inplace=True)
        subdept_frame['UPC'] = subdept_frame['UPC'].str.lstrip("0")
        for sub in subs:
            sub_frame = subdept_frame[subdept_frame['Sub-dept']==sub]
            sub_frame = sub_frame.drop(["Sub-dept"], axis=1)
            if sub not in ["(PRODUCE) caselots and discos", "(SEAFOOD) caselots and discos", "(CHEESE) caselots and discos", "(MEAT) caselots and discos"]:
                sheet_title= sub
                #" ".join(sub.split()[0].split("-"))
            else:
                sheet_title = "Caselots"
            sub_sheet = batchUpdate(sub_book_id, body=addSheet(sheetProp(title=sheet_title, gridProperties=gridProp(row=sub_frame.shape[0]+1,column=10, frozenRow=1))))
            sub_sheet_id = sub_sheet.get('replies', '')[0].get('addSheet', '').get('properties', '').get('sheetId', '')
            append(sub_book_id, sheet_title+"!A1:B1", "ROWS", [["ITEM UPC",	"Desc","PLU","Size","Unit Cost","Retail","Count","Total Cost","Total Retail","Margin"]], "RAW")
            append(sub_book_id, sheet_title+"!A2:B2", "ROWS", sub_frame.values.tolist(), "RAW")
            format_updates.extend([repeatCell(
                            gridRange(sub_sheet_id, 0, 1, 0, 10),
                            cellData(userEnteredFormat=cellFormat(
                                                        borders=borders(*all_thick),
                                                        textFormat=textFormat(fontFamily="Arial", fontSize=14, bold=True),
                                                        horizontalAlignment="CENTER",
                                                        verticalAlignment="MIDDLE",
                                                        backgroundColor=getColor(225, 225, 0, 0.01)
                                                    ))),
                            repeatCell(
                            gridRange(sub_sheet_id, 0, 1, 6, 7),
                            cellData(userEnteredFormat=cellFormat(
                                                        backgroundColor=getColor(215, 25, 0, 0.001)
                                                    ))),
                            repeatCell(
                            gridRange(sub_sheet_id, 1,sub_frame.shape[0]+1, 0, 10),
                            cellData(userEnteredFormat=cellFormat(
                                                        borders=borders(*all_solid),
                                                        textFormat=textFormat(fontFamily="Arial", fontSize=12)
                                                    ))),
                            repeatCell(
                            gridRange(sub_sheet_id, 0, sub_frame.shape[0]+1, 4, 6),
                            cellData(userEnteredFormat=cellFormat(
                                                        numberFormat=numberFormat("CURRENCY", "$#,##0.00")
                                                    ))),
                            repeatCell(
                            gridRange(sub_sheet_id, 0, sub_frame.shape[0]+1, 7, 9),
                            cellData(userEnteredFormat=cellFormat(
                                                        numberFormat=numberFormat("CURRENCY", "$#,##0.00")
                                                    ))),
                            repeatCell(
                            gridRange(sub_sheet_id, 0, sub_frame.shape[0]+1, 9, 10),
                            cellData(userEnteredFormat=cellFormat(
                                                        numberFormat=numberFormat("PERCENT", "0.00%")
                                                    ))),
                repeatCell(gridRange(sub_sheet_id, 1, sub_frame.shape[0]+1, 7, 8), cellData(extendedValue(formulaValue="=IF(ISBLANK(G2)=FALSE, G2*E2, "")"))),
                repeatCell(gridRange(sub_sheet_id, 1, sub_frame.shape[0]+1, 8, 9), cellData(extendedValue(formulaValue="=IF(ISBLANK(G2)=FALSE, G2*F2, "")"))),
                repeatCell(gridRange(sub_sheet_id, 1, sub_frame.shape[0]+1, 9, 10), cellData(extendedValue(formulaValue="=IFERROR((I2-H2)/I2, "")"))),
                veryBasicFilter(gridRange(sub_sheet_id, 0, sub_frame.shape[0]+1, 0, 10)),
                #autoSize(sub_sheet_id, "COLUMNS"),
                updateDimensionProps(dimensionProps(pixelSize=140), dimensionRange(sub_sheet_id, "COLUMNS", 0, 1)),
                updateDimensionProps(dimensionProps(pixelSize=220), dimensionRange(sub_sheet_id, "COLUMNS", 1, 2)),
                updateDimensionProps(dimensionProps(pixelSize=75), dimensionRange(sub_sheet_id, "COLUMNS", 2, 4)),
                updateDimensionProps(dimensionProps(pixelSize=125), dimensionRange(sub_sheet_id, "COLUMNS", 4, 9)),
                updateDimensionProps(dimensionProps(pixelSize=90), dimensionRange(sub_sheet_id, "COLUMNS", 9, 10)),

            ])
            time.sleep(5)
        
        
        response = batchUpdate(sub_book_id, deleteSheet(0))
        print(sheets.spreadsheets().get(spreadsheetId=sub_book_id).execute())
        sheet_names = [x.get("properties", '').get('title', '') for x in sheets.spreadsheets().get(spreadsheetId=sub_book_id).execute().get('sheets', '')]
        print(sheet_names)
        summary_sheet = batchUpdate(sub_book_id, body=addSheet(sheetProp(title="Summary Sheet", gridProperties=gridProp(row=len(sheet_names)+4,column=4))))
        summary_sheet_id = summary_sheet.get('replies', '')[0].get('addSheet', '').get('properties', '').get('sheetId', '')
        append(sub_book_id, "Summary Sheet!A1:B1", "ROWS", [[group + " Inventory"], ["Q1 FY2023"], ["Sub-Department", "Total Cost", "Total Retail", "Margin"]], "RAW")
        sheet_length = len(sheet_names)
        names = []
        for v in sheet_names:
            names.append([v])
        names.append(["Total"])
        print(names)
        append(sub_book_id, "Summary Sheet!A3:A", "ROWS", names, "RAW")
        index = 0 
        for name in sheet_names:
            format_updates.extend([
                repeatCell(gridRange(summary_sheet_id, index+3, index+4, 1, 2), cellData(extendedValue(formulaValue=f"=SUM(\'{name}\'!H:H)"))),
                repeatCell(gridRange(summary_sheet_id, index+3, index+4, 2, 3), cellData(extendedValue(formulaValue=f"=SUM(\'{name}\'!I:I)"))),
                
            ])
            index += 1
        
        format_updates.extend([
                            repeatCell(gridRange(summary_sheet_id, index+3, index+4, 1, 2), cellData(extendedValue(formulaValue=f"=SUM(B4:B{sheet_length+3})"))),
                            repeatCell(gridRange(summary_sheet_id, index+3, index+4, 2, 3), cellData(extendedValue(formulaValue=f"=SUM(C4:C{sheet_length+3})"))),
                            repeatCell(
                                gridRange(summary_sheet_id, 3, sheet_length+4, 3, 4),
                                cellData(extendedValue(formulaValue=f"=IFERROR((C4-B4)/C4, "")"))),
                            repeatCell(
                                gridRange(summary_sheet_id, 0, 2, 0, 1),
                                cellData(userEnteredFormat=cellFormat(
                                                            textFormat=textFormat(fontFamily="Arial", fontSize=14, bold=True),
                                                            backgroundColor=getColor(25, 175, 0, 0.01),
                                                            horizontalAlignment="CENTER",
                                                            verticalAlignment="MIDDLE"
                                                        ))),
                            mergeCells(gridRange(summary_sheet_id, 0, 1, 0, 4), "MERGE_ALL"),
                            mergeCells(gridRange(summary_sheet_id, 1, 2, 0, 4), "MERGE_ALL"),

                            repeatCell(
                                gridRange(summary_sheet_id, 2, 3, 0, 4),
                                cellData(userEnteredFormat=cellFormat(
                                                            borders=borders(*all_thick),
                                                            horizontalAlignment="CENTER",
                                                            verticalAlignment="MIDDLE",
                                                            textFormat=textFormat(fontFamily="Arial", bold=True, fontSize=12)
                                                        ))),
                            repeatCell(
                                gridRange(summary_sheet_id, 3, sheet_length+4, 0, 4),
                                cellData(userEnteredFormat=cellFormat(
                                                            borders=borders(*all_solid),
                                                            textFormat=textFormat(fontFamily="Arial", fontSize=12)
                                                        ))),
                            repeatCell(
                                gridRange(summary_sheet_id, 3, sheet_length+4, 1, 3),
                                cellData(userEnteredFormat=cellFormat(
                                                            numberFormat=numberFormat("CURRENCY", "$#,##0.00")
                                                        ))),
                            repeatCell(
                                gridRange(summary_sheet_id, 3, sheet_length+4, 3, 4),
                                cellData(userEnteredFormat=cellFormat(
                                                            numberFormat=numberFormat("PERCENT", "0.00%")
                                                        ))),
                            repeatCell(
                                gridRange(summary_sheet_id, sheet_length+3, sheet_length+4, 0, 1),
                                cellData(userEnteredFormat=cellFormat(
                                                            textFormat=textFormat(bold=True)
                                                        ))),
                            updateDimensionProps(dimensionProps(pixelSize=135), dimensionRange(summary_sheet_id, "COLUMNS", 0, 1)),
                            updateDimensionProps(dimensionProps(pixelSize=110), dimensionRange(summary_sheet_id, "COLUMNS", 1, 4)),
                            ])
        batchUpdate(sub_book_id, body=format_updates)
        batchUpdate(sub_book_id, body={"updateSheetProperties":{ "properties":{ "sheetId" : summary_sheet_id, "index":0 }, "fields":"index" }})
    
    #inventory_frame.fillna("")
    book = createBook(group + " FY23 Q1 Scanning Sheet", department_folder_id_FY23Q1, 'application/vnd.google-apps.spreadsheet', drive)
    book_id = book.get('id', '')
    format_updates = []
    # if inventory_frame[len(inventory_frame['PLU'].values.unique()) > 1:
    #     print(inventory_frame['PLU'].values.unique())
    df = inventory_frame.copy(True)
    df.fillna("", inplace=True)
    plu = df[(df['PLU'] != "") & (df['PLU'] != "None")]
    if type(plu) != NoneType:
        print(plu)
        plu = plu.drop(['Dept', 'Sub-dept', 'UPC'], axis=1)
        plu = plu[['PLU', 'Brand', 'Desc', 'Size', 'Price', 'Unit Cost']]
        plu.fillna("", inplace=True)
    dept_frame = inventory_frame.drop(['Dept', 'Sub-dept', 'PLU'], axis=1)
    dept_frame.fillna("", inplace=True)
    
    data_sheet = batchUpdate(book_id, body=addSheet(sheetProp(title="Data", gridProperties=gridProp(column=dept_frame.shape[1], frozenRow=1))))
    data_sheet_id = data_sheet.get('replies', '')[0].get('addSheet', '').get('properties', '').get('sheetId', '')
    append(book_id, "Data!A1:B1", "ROWS", [dept_frame.columns.tolist()], "RAW")
    append(book_id, "Data!A2:B2", "ROWS", dept_frame.values.tolist(), "RAW")
    format_updates.extend([
                        #All the column indexes get incremented by one because we'll be inserting a column by the time these formats get applied.
                        repeatCell(
                        gridRange(data_sheet_id, 0, 1, 0, dept_frame.shape[1]+1),
                        cellData(userEnteredFormat=cellFormat(
                                                    borders=borders(*all_thick),
                                                    textFormat=textFormat(fontFamily="Arial", fontSize=14, bold=True),
                                                    horizontalAlignment="CENTER",
                                                    verticalAlignment="MIDDLE",
                                                    backgroundColor=getColor(25, 175, 0, 0.01)
                                                ))),
                        repeatCell(
                        gridRange(data_sheet_id, 1,dept_frame.shape[0]+1+ (0 if type(plu) == NoneType else plu.shape[0]), 0, dept_frame.shape[1]+1),
                        cellData(userEnteredFormat=cellFormat(
                                                    borders=borders(*all_solid),
                                                    textFormat=textFormat(fontFamily="Arial", fontSize=12)
                                                ))),
                        repeatCell(
                        gridRange(data_sheet_id, 1,dept_frame.shape[0]+1, 0, 1),
                        cellData(userEnteredFormat=cellFormat(
                                                    numberFormat=numberFormat("TEXT", "0000000000000")
                                                ))),
                        repeatCell(
                        gridRange(data_sheet_id, 1,dept_frame.shape[0]+1+ (0 if type(plu) == NoneType else plu.shape[0]), dept_frame.shape[1]-1, dept_frame.shape[1]+1),
                        cellData(userEnteredFormat=cellFormat(
                                                    numberFormat=numberFormat("CURRENCY", "$#,##0.00")
                                                ))),
                        veryBasicFilter(gridRange(data_sheet_id, 0, dept_frame.shape[0]+1+ (0 if type(plu) == NoneType else plu.shape[0]), 0, dept_frame.shape[1]+1)),
                        autoSize(data_sheet_id, "COLUMNS")
    ])
    if type(plu) != NoneType:
        append(book_id, "Data!A1:B1", "ROWS", plu.values.tolist(), "RAW")
    
    batchUpdate(book_id, body=insert_row_or_column_body(data_sheet_id, "COLUMNS", 1, 2, False))
    batchUpdate(book_id, body=repeatCell(gridRange(data_sheet_id, 1, dept_frame.shape[0]+1 + (0 if type(plu) == NoneType else plu.shape[0]), 1, 2),
        cellData(extendedValue(formulaValue="=A2*1"))))



    
    scan_sheet = batchUpdate(book_id, body=addSheet(sheetProp(title="Scan Sheet", gridProperties=gridProp(column=10, frozenRow=2))))
    scan_sheet_id = scan_sheet.get('replies', '')[0].get('addSheet', '').get('properties', '').get('sheetId', '')
    append(book_id, "Scan Sheet!A1:B1", "ROWS", [['','','','','','','Total'],["UPC/PLU", "Count", "Brand", "Description", "Size", "Price", "Cost", "Total Price", "Total Cost", "Margin"]], "USER_ENTERED")
    format_updates.extend([repeatCell(
                        gridRange(scan_sheet_id, 0, 1, 6, 7),
                        cellData(userEnteredFormat=cellFormat(
                                                    borders=borders(*all_thick),
                                                    textFormat=textFormat(fontFamily="Arial", fontSize=14, bold=True),
                                                    horizontalAlignment="CENTER",
                                                    verticalAlignment="MIDDLE",
                                                    backgroundColor=getColor(25, 175, 25, 0.01)
                                                ))),
                        repeatCell(
                        gridRange(scan_sheet_id, 0, 1, 6, 10),
                        cellData(userEnteredFormat=cellFormat(
                                                    borders=borders(*all_thick),
                                                    textFormat=textFormat(fontFamily="Arial", fontSize=14, bold=True),
                                                ))),
                        repeatCell(
                        gridRange(scan_sheet_id, 1, 2, 0, 1),
                        cellData(userEnteredFormat=cellFormat(
                                                    borders=borders(*all_thick),
                                                    horizontalAlignment="CENTER",
                                                    verticalAlignment="MIDDLE",
                                                    textFormat=textFormat(fontFamily="Arial", fontSize=14, bold=True),
                                                    backgroundColor=getColor(255, 255, 0, 0.01)
                                                ))),
                        repeatCell(
                        gridRange(scan_sheet_id, 1, 2, 1, 2),
                        cellData(userEnteredFormat=cellFormat(
                                                    borders=borders(*all_thick),
                                                    horizontalAlignment="CENTER",
                                                    verticalAlignment="MIDDLE",
                                                    textFormat=textFormat(fontFamily="Arial", fontSize=14, bold=True),
                                                    backgroundColor=getColor(200, 75, 0, 0.01)
                                                ))),
                        repeatCell(
                        gridRange(scan_sheet_id, 1, 2, 2, 10),
                        cellData(userEnteredFormat=cellFormat(
                                                    borders=borders(*all_thick),
                                                    horizontalAlignment="CENTER",
                                                    verticalAlignment="MIDDLE",
                                                    textFormat=textFormat(fontFamily="Arial", fontSize=14, bold=True),
                                                    backgroundColor=getColor(0, 175, 250, 0.0001)
                                                ))),
                        repeatCell(
                        gridRange(scan_sheet_id, 0, 1000, 5, 9),
                        cellData(userEnteredFormat=cellFormat(
                                                    numberFormat=numberFormat("CURRENCY", "$#,##0.00")
                                                ))),
                        repeatCell(
                        gridRange(scan_sheet_id, 0, 1000, 9, 10),
                        cellData(userEnteredFormat=cellFormat(
                                                    numberFormat=numberFormat("PERCENT", "0.00%")
                                                ))),

                        repeatCell(gridRange(scan_sheet_id, 0, 1, 7, 8), cellData(extendedValue(formulaValue="=SUM(H3:H)"))),
                        repeatCell(gridRange(scan_sheet_id, 0, 1, 8, 9), cellData(extendedValue(formulaValue="=SUM(I3:I)"))),
                        repeatCell(gridRange(scan_sheet_id, 0, 1, 9, 10), cellData(extendedValue(formulaValue="=IFERROR((H1-I1)/(H1), "")"))),
                        

                        
                        repeatCell(gridRange(scan_sheet_id, 2, 1000, 2, 3), cellData(extendedValue(formulaValue="=IF(ISBLANK(A3)=FALSE, IFNA(INDEX(Data!C:C, MATCH(A3, Data!B:B, 0)), INDEX(Data!C:C, MATCH(A3, Data!A:A, 0))), "")"))),
                        repeatCell(gridRange(scan_sheet_id, 2, 1000, 3, 4), cellData(extendedValue(formulaValue="=IF(ISBLANK(A3)=FALSE, IFNA(INDEX(Data!D:D, MATCH(A3, Data!B:B, 0)), INDEX(Data!D:D, MATCH(A3, Data!A:A, 0))), "")"))),
                        repeatCell(gridRange(scan_sheet_id, 2, 1000, 4, 5), cellData(extendedValue(formulaValue="=IF(ISBLANK(A3)=FALSE, IFNA(INDEX(Data!E:E, MATCH(A3, Data!B:B, 0)), INDEX(Data!E:E, MATCH(A3, Data!A:A, 0))), "")"))),
                        repeatCell(gridRange(scan_sheet_id, 2, 1000, 5, 6), cellData(extendedValue(formulaValue="=IF(ISBLANK(A3)=FALSE, IFNA(INDEX(Data!F:F, MATCH(A3, Data!B:B, 0)), INDEX(Data!F:F, MATCH(A3, Data!A:A, 0))), "")"))),
                        repeatCell(gridRange(scan_sheet_id, 2, 1000, 6, 7), cellData(extendedValue(formulaValue="=IF(ISBLANK(A3)=FALSE, IFNA(INDEX(Data!G:G, MATCH(A3, Data!B:B, 0)), INDEX(Data!G:G, MATCH(A3, Data!A:A, 0))),"")"))),


                        repeatCell(gridRange(scan_sheet_id, 2, 1000, 7, 8), cellData(extendedValue(formulaValue="=IF(ISBLANK(B3)=FALSE, F3*B3, "")"))),
                        repeatCell(gridRange(scan_sheet_id, 2, 1000, 8, 9), cellData(extendedValue(formulaValue="=IF(ISBLANK(B3)=FALSE, G3*B3, "")"))),
                        repeatCell(gridRange(scan_sheet_id, 2, 1000, 9, 10), cellData(extendedValue(formulaValue="=IFERROR((H3-I3)/H3, "")"))),
                        
                        repeatCell(
                        gridRange(scan_sheet_id, 2,1000, 0, 10),
                        cellData(userEnteredFormat=cellFormat(
                                                    borders=borders(*all_solid),
                                                    textFormat=textFormat(fontFamily="Arial", fontSize=12)
                                                ))),
                        
                        veryBasicFilter(gridRange(scan_sheet_id, 1, 1000, 0, 10)),
                        #autoSize(scan_sheet_id, "COLUMNS")
                        updateDimensionProps(dimensionProps(pixelSize=140), dimensionRange(scan_sheet_id, "COLUMNS", 0, 1)),
                        updateDimensionProps(dimensionProps(pixelSize=85), dimensionRange(scan_sheet_id, "COLUMNS", 1, 2)),
                        updateDimensionProps(dimensionProps(pixelSize=150), dimensionRange(scan_sheet_id, "COLUMNS", 2, 3)),
                        updateDimensionProps(dimensionProps(pixelSize=220), dimensionRange(scan_sheet_id, "COLUMNS", 3, 4)),
                        updateDimensionProps(dimensionProps(pixelSize=75), dimensionRange(scan_sheet_id, "COLUMNS", 4, 7)),
                        updateDimensionProps(dimensionProps(pixelSize=125), dimensionRange(scan_sheet_id, "COLUMNS", 7, 9)),
                        updateDimensionProps(dimensionProps(pixelSize=90), dimensionRange(scan_sheet_id, "COLUMNS", 9, 10)),
                        #140 UPC 150 Brand 220 Desc Count 85 Totals 125 each Size Price Cost 75 Margin 90
    ])
    
    batchUpdate(book_id, body=format_updates)

    batchUpdate(book_id, body=[
        deleteSheet(0),
        {"updateSheetProperties":{
            "properties":{
                "sheetId" : scan_sheet_id,
                "index":0
                },
            "fields":"index"
            }},{"updateSheetProperties":{
            "properties":{
                "sheetId" : data_sheet_id,
                "hidden": "True"
                },
            "fields":"hidden"
            }}])
    
    time.sleep(17)
