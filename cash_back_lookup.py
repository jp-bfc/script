import tkinter as tk
from tkinter import filedialog
import numpy as np
import scipy.stats as st
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
#from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient import discovery
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd
import os
import configparser

SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
config = configparser.ConfigParser()
config.read('script_configs.ini')
cashback_report_id = config['DEFAULT']['cashback_report_sheet']

sheet_creds = service_account.Credentials.from_service_account_file('sheet_credentials.json', scopes=SCOPES)
drive_creds = service_account.Credentials.from_service_account_file('drive_credentials.json', scopes=SCOPES)

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



month_order = {
    'MAY 2021' : 0,
    'JUNE 2021' : 1,
    'JULY 2021' : 2,
    'AUGUST 2021' : 3,
    'SEPTEMBER 2021' : 4,
    'OCTOBER 2021' : 5,
    'NOVEMBER 2021' : 6,
    'DECEMBER 2021' : 7,
    'JANUARY 2022' : 8,
    'FEBRUARY 2022' : 9,
    'MARCH 2022' : 10,
    'APRIL 2022' : 11,
    'MAY 2022' : 12,
    'JUNE 2022' : 13,
    'JULY 2022' : 14,
    'AUGUST 2022' : 15,
    'SEPTEMBER 2022' : 16,
}

class Transaction:
    def __init__(self, transaction_dictionary, month):
        self.Month = month
        self.TransID = str(transaction_dictionary["Trans.#"][1]) if "Trans.#" in transaction_dictionary else ""
        self.Date = str(transaction_dictionary["Date"][1]) if "Date" in transaction_dictionary else ""
        self.Total = float(transaction_dictionary["TOTAL"][1]) if "TOTAL" in transaction_dictionary else 0.0
        self.Debit = float(transaction_dictionary["Debit"][1]) if "Debit" in transaction_dictionary else 0.0
        self.EBT = (float(transaction_dictionary["EBT"][1]) if transaction_dictionary["EBT"][1] != "stamps" else float(transaction_dictionary["EBT"][2])) if "EBT" in transaction_dictionary else 0.0
        self.Balance = float(transaction_dictionary["BALANCE"][0]) if "BALANCE" in transaction_dictionary else 0.0
    def print(self):
        return [self.TransID, self.Date, str(self.Total), str(self.Debit), str(self.EBT), str(self.Balance)]

trans_list = []

transaction_totals = {}

for filename in os.listdir(r"\\bfc-hv-01\SWAP\Transaction Logs By Month\New folder"):
    transactions = [{}]
    transaction_count = 1
    name = " ".join((filename.split(".")[0]).split("-")[1:])                            
    with open(r"\\bfc-hv-01\SWAP\Transaction Logs By Month\New folder\\" +filename) as f:
        for _ in range(4):
            next(f)
        while f:
            line = f.readline()
            if not line: break
            if line.strip() == "----------------------------------------------------------------------------------":
                transactions.append({})
                transaction_count += 1
            else:
                split_line = line.split()
                if len(split_line) > 0:
                    if split_line[0] in transactions[transaction_count-1]: transactions[transaction_count-1][split_line[0]].append(split_line[1:])
                    else: transactions[transaction_count-1][split_line[0]] = split_line[1:]
        
        trs = [y for y in [x for x in transactions if 'Trans.' in x] if "SALE" in y['Trans.']] 
        cashback_transactions = [x for x in trs if 'BALANCE' in x and x['BALANCE'] != ['0.00'] and 'Cash' not in x and ('Debit' in x or 'EBT' in x)]
        #print(len(trs))
        #transaction_totals['AUGUST 2022'] = len(trs)
        transaction_totals[name] = len(trs)
        #print(transaction_totals)
        for t in cashback_transactions:
            trans_list.append(Transaction(t, name))
                
trans_frame = pd.DataFrame(
                [
                    [x.Month, x.TransID, x.Date, x.Total, x.Debit, x.EBT, x.Balance]
                    for x in trans_list
                ],
                columns=[
                    "Month", "TransID", "Date",
                    "Total", "Debit", "EBT", "BALANCE"
                    ]
                )

trans_frame['Month'] = pd.Categorical(trans_frame['Month'], categories=month_order, ordered=True)
trans_frame.sort_values(['Month'], ascending=True, inplace=True, axis=0)
format_updates = []
#trans_frame = trans_frame.fillna("", inplace=True)
month_list = trans_frame["Month"].unique().tolist()
#print(trans_frame)
for m in [
    'JUNE 2021','JULY 2021','AUGUST 2021','SEPTEMBER 2021','OCTOBER 2021','NOVEMBER 2021','DECEMBER 2021',
            'JANUARY 2022','FEBRUARY 2022','MARCH 2022','APRIL 2022','MAY 2022','JUNE 2022','JULY 2022',
            'AUGUST 2022']:
    print(m)
    print(transaction_totals)
    sheet = batchUpdate(cashback_report_id, body=addSheet(sheetProp(title=m, gridProperties=gridProp(frozenRow=1, column=6))))
    sheet_id = sheet.get('replies', '')[0].get('addSheet', '').get('properties', '').get('sheetId', '')
    pushed_frame = trans_frame[trans_frame['Month'] == m]
    pushed_frame = pushed_frame[["TransID", "Date", "Total", "Debit", "EBT", "BALANCE"]]
    append(cashback_report_id, m+"!A1:B1", "ROWS", [["Trans #", "Date", "Total", "Debit Charge", "EBT", "Balance"]], "RAW")
    append(cashback_report_id, m+"!A1:B1", "ROWS", pushed_frame.to_numpy().tolist(), "RAW")
    format_updates.extend([
        repeatCell(
            gridRange(sheet_id, 0, 1, 0, 6),
            cellData(userEnteredFormat=cellFormat(
                borders=borders(*all_thick),
                textFormat=textFormat(fontFamily="Arial", fontSize=14, bold=True),
                backgroundColor=getColor(50, 235, 0, 0.01),
                horizontalAlignment="CENTER",
                verticalAlignment="MIDDLE"
                ))
            ),
        repeatCell(
            gridRange(sheet_id, 1, pushed_frame.shape[0]+1, 0, 6),
            cellData(userEnteredFormat=cellFormat(
                borders=borders(*all_solid),
                textFormat=textFormat(fontFamily="Arial", fontSize=12)
                ))
            ),
        repeatCell(
            gridRange(sheet_id, 1, pushed_frame.shape[0]+1, 1, 2),
            cellData(userEnteredFormat=cellFormat(
                                        numberFormat=numberFormat("DATE", "m/d/yy")
                                    ))),
        repeatCell(
            gridRange(sheet_id, 1, pushed_frame.shape[0]+1, 2, 6),
            cellData(userEnteredFormat=cellFormat(numberFormat=currency))
            ),
        veryBasicFilter(gridRange(sheet_id, 0, pushed_frame.shape[0]+1, 0, 6)),
        autoSize(sheet_id, "COLUMNS")
        
        ])
    print(pushed_frame)
    trans_length = pushed_frame.shape[0]
    total_avg = np.average(pushed_frame["Total"])
    total_sales = pushed_frame['Total'].sum()
    total_cashbacks = pushed_frame['BALANCE'].sum()
    rounded_avg = np.average(pushed_frame["Total"].apply(lambda x: round(x, 0)))
    average_balance = np.average(pushed_frame["BALANCE"])
    total_mode = pushed_frame["BALANCE"].mode()[0]
    total_mode_count = pushed_frame["BALANCE"].value_counts()[total_mode]
    total_percent = float(trans_length) / transaction_totals[m]
    print(total_mode)
    print(total_mode_count)
    print(total_percent)

    debit_frame = pushed_frame[pushed_frame["EBT"] == 0.0]
    total_debits = debit_frame.shape[0]
    debit_total_percent = total_debits / transaction_totals[m]
    debit_percent_backs = total_debits / float(trans_length)
    total_sales_debits = debit_frame['Total'].sum()
    total_cashbacks_debits = debit_frame['BALANCE'].sum()
    avg_only_debit = np.average(debit_frame['Debit'])
    avg_only_debit_cashback = np.average(debit_frame['BALANCE'])
    debit_mode = pushed_frame["BALANCE"].mode()[0]
    debit_mode_count = pushed_frame["BALANCE"].value_counts()[debit_mode]
    print(debit_mode)
    print(debit_mode_count)
    print(debit_total_percent)
    print(debit_percent_backs)

    debit_under_ten = debit_frame[(debit_frame['Total'] <= 10.0) & (debit_frame['Total'] > 5.0)]
    total_debits_under_ten = debit_under_ten.shape[0]
    debit_total_percent_under_ten = total_debits_under_ten / transaction_totals[m]
    debit_percent_backs_under_ten = total_debits_under_ten / float(trans_length)
    avg_only_debit_under_ten = np.average(debit_under_ten['Debit'])
    debit_under_ten_avg_cshbk = np.average(debit_under_ten['BALANCE'])
    debit_under_ten_mode = debit_under_ten["BALANCE"].mode()[0]
    debit_under_ten_mode_count = debit_under_ten["BALANCE"].value_counts()[debit_under_ten_mode]
    total_sales_debits_under_ten = debit_under_ten['Total'].sum()
    total_cashbacks_debits_under_ten = debit_under_ten['BALANCE'].sum()
    
    debit_under_five = debit_frame[(debit_frame['Total'] <= 5.0)]
    total_debits_under_five = debit_under_five.shape[0]
    debit_total_percent_under_five = total_debits_under_five / transaction_totals[m]
    debit_percent_backs_under_five = total_debits_under_five / float(trans_length)
    avg_only_debit_under_five = np.average(debit_under_five['Debit'])
    debit_under_five_avg_cshbk = np.average(debit_under_five['BALANCE'])
    debit_under_five_mode = debit_under_five["BALANCE"].mode()[0]
    debit_under_five_mode_count = debit_under_five["BALANCE"].value_counts()[debit_under_five_mode]
    total_sales_debits_under_five = debit_under_five['Total'].sum()
    total_cashbacks_debits_under_five = debit_under_five['BALANCE'].sum()


    ebt_frame = pushed_frame[pushed_frame["Debit"] == 0.0]
    total_ebt = ebt_frame.shape[0]
    ebt_total_percent = total_ebt / transaction_totals[m]
    ebt_percent_backs = total_ebt / float(trans_length)
    avg_only_ebt = np.average(ebt_frame['EBT'])
    avg_only_ebt_cashback = np.average(ebt_frame['BALANCE'])
    ebt_mode = pushed_frame["BALANCE"].mode()[0]
    ebt_mode_count = pushed_frame["BALANCE"].value_counts()[ebt_mode]
    total_sales_ebt = ebt_frame['Total'].sum()
    total_cashbacks_ebt = ebt_frame['BALANCE'].sum()

    ebt_under_ten = ebt_frame[(ebt_frame['Total'] <= 10.0) & (ebt_frame['Total'] > 5.0)]
    ebt_under_ten_count = ebt_under_ten.shape[0]
    ebt_total_percent_under_ten = ebt_under_ten_count / transaction_totals[m]
    ebt_percent_backs_under_ten = ebt_under_ten_count / float(trans_length)
    avg_ebt_under_ten = np.average(ebt_under_ten['EBT'])
    ebt_under_ten_avg_cshbk = np.average(ebt_under_ten['BALANCE'])
    ebt_under_ten_mode = ebt_under_ten["BALANCE"].mode()[0]
    ebt_under_ten_mode_count = ebt_under_ten["BALANCE"].value_counts()[ebt_under_ten_mode]
    total_sales_ebt_under_ten = ebt_under_ten['Total'].sum()
    total_cashbacks_ebt_under_ten = ebt_under_ten['BALANCE'].sum()
    
    ebt_under_five = ebt_frame[(ebt_frame['Total'] <= 5.0)]
    ebt_under_five_count = ebt_under_five.shape[0]
    ebt_total_percent_under_five = ebt_under_five_count / transaction_totals[m]
    ebt_percent_backs_under_five = ebt_under_five_count / float(trans_length)
    avg_ebt_under_five = np.average(ebt_under_five['EBT'])
    ebt_under_five_avg_cshbk = np.average(ebt_under_five['BALANCE'])
    ebt_under_five_mode = ebt_under_five["BALANCE"].mode()[0]
    ebt_under_five_mode_count = ebt_under_five["BALANCE"].value_counts()[ebt_under_five_mode]
    total_sales_ebt_under_five = ebt_under_five['Total'].sum()
    total_cashbacks_ebt_under_five = ebt_under_five['BALANCE'].sum()
    append(cashback_report_id,
        "Summary!A1:B1", "ROWS",
        [[m,
            transaction_totals[m], trans_length, total_percent, total_sales, total_cashbacks, total_avg, average_balance, "%s, Frq: %s" % (str(total_mode), str(total_mode_count)),   "",
            total_debits, debit_total_percent, debit_percent_backs, total_sales_debits, total_cashbacks_debits, avg_only_debit, avg_only_debit_cashback, "%s, Frq: %s" % (str(debit_mode), str(debit_mode_count)),
            total_debits_under_ten, debit_total_percent_under_ten, debit_percent_backs_under_ten, total_sales_debits_under_ten, total_cashbacks_debits_under_ten,avg_only_debit_under_ten, debit_under_ten_avg_cshbk, "%s, Frq: %s" % (str(debit_under_ten_mode), str(debit_under_five_mode_count)),
            total_debits_under_five, debit_total_percent_under_five, debit_percent_backs_under_five, total_sales_debits_under_five, total_cashbacks_debits_under_five,avg_only_debit_under_five, debit_under_five_avg_cshbk, "%s, Frq: %s" % (str(debit_under_five_mode), str(debit_under_five_mode_count)), "",
            total_ebt, ebt_total_percent, ebt_percent_backs, total_sales_ebt, total_cashbacks_ebt, avg_only_ebt, avg_only_ebt_cashback, "%s, Frq: %s" % (str(ebt_mode), str(ebt_mode_count)),
            ebt_under_ten_count, ebt_total_percent_under_ten, ebt_percent_backs_under_ten, total_sales_ebt_under_ten, total_cashbacks_ebt_under_ten,avg_ebt_under_ten, ebt_under_ten_avg_cshbk, "%s, Frq: %s" % (str(ebt_under_ten_mode), str(ebt_under_five_mode_count)),
            ebt_under_five_count, ebt_total_percent_under_five, ebt_percent_backs_under_five, total_sales_ebt_under_five, total_cashbacks_ebt_under_five,avg_ebt_under_five, ebt_under_five_avg_cshbk, "%s, Frq: %s" % (str(ebt_under_five_mode), str(ebt_under_five_mode_count)),
            ]], "RAW")


sheet = batchUpdate(cashback_report_id, body=addSheet(sheetProp(title="ALL TRANSACTIONS", gridProperties=gridProp(frozenRow=1, column=6))))
sheet_id = sheet.get('replies', '')[0].get('addSheet', '').get('properties', '').get('sheetId', '')

trans_frame = trans_frame[["TransID", "Date", "Total", "Debit", "EBT", "BALANCE"]]
append(cashback_report_id, "ALL TRANSACTIONS!A1:B1", "ROWS", [["Trans #", "Date", "Total", "Debit Charge", "EBT", "Balance"]], "RAW")
append(cashback_report_id, "ALL TRANSACTIONS!A1:B1", "ROWS", trans_frame.to_numpy().tolist(), "RAW")
format_updates.extend([
    repeatCell(
        gridRange(sheet_id, 0, 1, 0, 6),
        cellData(userEnteredFormat=cellFormat(
            borders=borders(*all_thick),
            textFormat=textFormat(fontFamily="Arial", fontSize=14, bold=True),
            backgroundColor=getColor(50, 235, 0, 0.01),
            horizontalAlignment="CENTER",
            verticalAlignment="MIDDLE"
            ))
        ),
    repeatCell(
        gridRange(sheet_id, 1, pushed_frame.shape[0]+1, 0, 6),
        cellData(userEnteredFormat=cellFormat(
            borders=borders(*all_solid),
            textFormat=textFormat(fontFamily="Arial", fontSize=12)
            ))
        ),
    repeatCell(
        gridRange(sheet_id, 1, pushed_frame.shape[0]+1, 1, 2),
        cellData(userEnteredFormat=cellFormat(
                                    numberFormat=numberFormat("DATE", "m/d/yy")
                                ))),
    repeatCell(
        gridRange(sheet_id, 1, pushed_frame.shape[0]+1, 2, 6),
        cellData(userEnteredFormat=cellFormat(numberFormat=currency))
        ),
    veryBasicFilter(gridRange(sheet_id, 0, pushed_frame.shape[0]+1, 0, 6)),
    autoSize(sheet_id, "COLUMNS")
    
    ])


batchUpdate(cashback_report_id, body=format_updates)