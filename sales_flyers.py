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

def get_sheets(workbook_id):
    try:
        all_sheets = sheets.spreadsheets().get(spreadsheetId=workbook_id).execute()['sheets']
        return all_sheets
    except HttpError as error:
        print(f"An error occurred: {error}")
        return error

def get_sheet(workbook_id, sheet_id):
    try:
        #print(sheets.spreadsheets().get(spreadsheetId=workbook_id).execute()['sheets'].get('properties').get('sheetId', sheet_id))
        #print([p.get('properties') for p in [x for x in sheets.spreadsheets().get(spreadsheetId=workbook_id).execute()['sheets']]])
        return [x for x in sheets.spreadsheets().get(spreadsheetId=workbook_id).execute()['sheets'] if sheet_id == x.get('properties').get('sheetId')]
    except HttpError as error:
        print(f"An error occurred: {error}")
        return error

def get_sheet_id(workbook_id, sheet_name):
    try:
        all_sheets = get_sheets(workbook_id=workbook_id)
        if len([x for x in all_sheets if sheet_name in x['properties']['title']]) > 1:
            print("That name is not specific enough, try a more specific one.")
            return False
        elif len([x for x in all_sheets if sheet_name in x['properties']['title']]) == 0:
            print("That name doesn't match a Sales Flyer period, try again.")
            return False
        elif len([x for x in all_sheets if sheet_name in x['properties']['title']]) == 1:
            return [x for x in all_sheets if sheet_name in x['properties']['title']][0]['properties']['sheetId']
    except HttpError as error:
        print(f"An error occurred: {error}")
        return error


SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
sheet_creds = service_account.Credentials.from_service_account_file(
                    'sheet_credentials.json', scopes=SCOPES)
#https://googleapis.github.io/google-api-python-client/docs/dyn/sheets_v4.html
sheets = build('sheets', 'v4', credentials=sheet_creds)

sales_flyer_wb_id = config['DEFAULT']['sales_flyer_sheet_FY23']
import_folder_id = config['DEFAULT']['sales_flyer_import_folder']
import_choice = input("Which Sales Flyer are you processing?: ")

#Validate user input
while get_sheet_id(sales_flyer_wb_id, import_choice) == False:
    import_choice = input("Which Sales Flyer are you processing?: ")

sheet_id = get_sheet_id(sales_flyer_wb_id, import_choice)
sheet_name = get_sheet(sales_flyer_wb_id, sheet_id=sheet_id)[0].get('properties').get('title')

headers = sheets.spreadsheets().values().get(spreadsheetId=sales_flyer_wb_id, range=sheet_name + '!A3:X3').execute().get('values', [])

data_rows = sheets.spreadsheets().values().get(spreadsheetId=sales_flyer_wb_id, range=sheet_name + '!A4:X').execute().get('values', [])

df = pd.DataFrame(data_rows, columns=headers[0])

df['Full Code'] = ""
df['Full Code'] = np.where(df['USDA\nOrganic'] == 'TRUE', df['Full Code'] + "O", df['Full Code'])
df['Full Code'] = np.where(df['Local VT'] == 'TRUE', df['Full Code'] + "V", df['Full Code'])
df['Full Code'] = np.where(df['Local 60'] == 'TRUE', df['Full Code'] + "6", df['Full Code'])
df['Full Code'] = np.where(df['Made in VT'] == 'TRUE', df['Full Code'] + "M", df['Full Code'])
df['Full Code'] = np.where(df['Fair Trade'] == 'TRUE', df['Full Code'] + "F", df['Full Code'])
df['Full Code'] = np.where(df['Gluten Free'] == 'TRUE', df['Full Code'] + "G", df['Full Code'])
df['Full Code'] = np.where(df['Vegan'] == 'TRUE', df['Full Code'] + "A", df['Full Code'])

df.rename(columns={'UPC/PLU' : 'UPC'}, inplace=True)
df = df[['UPC', 'Department', 'Regular Price', 'Sale Price', 'Brand/Farm', 'Full Code', 'Product']]
upcs = df['UPC'].values.tolist()

matching_query = """
select obj.F01 as 'UPC', F29 as 'SMS Desc', F155 as 'SMS Brand', F180 as 'SMS Code', rpc.F1024 as 'SMS Dept', prc.F30 as 'SMS Price'
from STORESQL.dbo.OBJ_TAB obj
left join PRICE_TAB prc on OBJ.F01 = prc.F01
LEFT join RPC_TAB rpc on OBJ.F18 = rpc.F18
where obj.F01 in (%s)
""" % ("\'" + '\',\''.join(upcs) + "\'")

q = pd.read_sql(matching_query, cnxn)

final = df.merge(q, how="left", on=['UPC'], suffixes=("marketing", "IT"))
final = final[['UPC','Product', 'SMS Desc', 'Department', 'SMS Dept', 'SMS Price', 'Regular Price', 'Sale Price', 'Brand/Farm', 'SMS Brand', 'Full Code', 'SMS Code']]
final.to_csv(f"{import_choice} SMS Import {datetime.datetime.today().date()}.csv", index=False)

import_book = createBook(f"{import_choice} SMS Import {datetime.datetime.today().date()}", import_folder_id, 'application/vnd.google-apps.spreadsheet', drive)
book_id = import_book.get('id', '')
final = final.astype(str)
final['SMS Price'] = final['SMS Price'].str.replace('$', '', regex=False)
final['Regular Price'] = final['Regular Price'].str.replace('$', '', regex=False)
final['SMS Price'] = final['SMS Price'].str.replace('nan', '', regex=False)
final['Regular Price'] = final['Regular Price'].str.replace('nan', '', regex=False)
final.fillna("", inplace=True)
print(final[['SMS Price', 'Regular Price']])

final = final.astype({'SMS Price' : float, 'Regular Price' : float})
final['Diff'] = final['SMS Price'] - final['Regular Price']
final = final[['UPC', 'Diff', 'SMS Price', 'Regular Price', 'Sale Price', 'Product', 'SMS Desc', 'Department', 'SMS Dept',  'Brand/Farm', 'SMS Brand', 'Full Code', 'SMS Code']]
final.fillna("", inplace=True)
final = final.astype(str)
# df['SMS Price'] = df['SMS Price'].str.replace('$', '', regex=False)
# df['Sale Price'] = df['Sale Price'].str.replace('$', '', regex=False)
# print(final)
append(book_id, "Sheet1!A1:B1", "ROWS", [['UPC', 'Diff', 'SMS Price', 'Regular Price', 'Sale Price', 'Product', 'SMS Desc', 'Department', 'SMS Dept',  'Brand/Farm', 'SMS Brand', 'Full Code', 'SMS Code']], "RAW")
append(book_id, "Sheet1!A2:B2", "ROWS", final.values.tolist(), "RAW")
batchUpdate(book_id, body=autoSize(0, "COLUMNS"))