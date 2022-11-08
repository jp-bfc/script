import pyodbc
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
import re
from mlxtend.frequent_patterns import apriori, association_rules
import configparser
from pathlib import Path


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
    def __init__(self, transaction_dictionary):
        #add back in param for month
        #self.Month = month
        self.TransID = str(transaction_dictionary["Trans.#"][1]) if "Trans.#" in transaction_dictionary else ""
        self.Date = str(transaction_dictionary["Date"][1]) if "Date" in transaction_dictionary else ""
        self.Total = float(transaction_dictionary["TOTAL"][1]) if "TOTAL" in transaction_dictionary else 0.0
        self.Shareholder = str(transaction_dictionary['Customer'][1][1:]) if len(transaction_dictionary['Customer']) > 2 else "Non-Shareholder"
        self.ItemsSold = transaction_dictionary['ANALYSIS ITEMS']

        # self.Debit = float(transaction_dictionary["Debit"][1]) if "Debit" in transaction_dictionary else 0.0
        # self.EBT = (float(transaction_dictionary["EBT"][1]) if transaction_dictionary["EBT"][1] != "stamps" else float(transaction_dictionary["EBT"][2])) if "EBT" in transaction_dictionary else 0.0
        # self.Balance = float(transaction_dictionary["BALANCE"][0]) if "BALANCE" in transaction_dictionary else 0.0
    def print(self):
        #return [self.TransID, self.Date, str(self.Total), str(self.Debit), str(self.EBT), str(self.Balance)]
        print([self.TransID, self.Date, str(self.Total), str(self.Shareholder), str(self.ItemsSold)])

trans_list = []

transaction_totals = {}

#for filename in os.listdir(r"\\bfc-hv-01\SWAP\Transaction Logs By Month\New folder"):
transactions = [{}]
transaction_count = 1
pattern = re.compile("^(\d{13})?$")
#name = " ".join((filename.split(".")[0]).split("-")[1:])                            
with Path(__file__).with_name("TRANSACTIONS-OCTOBER-2022.TXT").open('r') as f:
    for _ in range(4):
        next(f)
    while f:
        line = f.readline()
        if not line: break
        if line.strip() == "----------------------------------------------------------------------------------":
            transactions.append({"ANALYSIS ITEMS" : []})
            transaction_count += 1
        else:
            split_line = line.split()
            if len(split_line) > 0:
                if split_line[0] not in ['0000000002200', '0000000000506', '0000000094046']:
                    if pattern.match(split_line[0]):
                        try:
                            qty = int(split_line[-1])
                        except ValueError:
                            try:
                                qty = int(split_line[-2])
                            except ValueError:
                                qty=1
                                #print("busted bud", split_line)
                        
                        transactions[transaction_count-1]['ANALYSIS ITEMS'].append([split_line[0], qty])
                if split_line[0] in transactions[transaction_count-1]:
                    transactions[transaction_count-1][split_line[0]].append(split_line[1:])
                else:
                    transactions[transaction_count-1][split_line[0]] = split_line[1:]
    
    trs = [y for y in [x for x in transactions if 'Trans.' in x] if "SALE" in y['Trans.']]
    for t in trs:
        trans_list.append(Transaction(t))

with open("trans_target.csv", "a") as out:
    for t in trans_list:
        if len(t.ItemsSold) > 1: 
            for upc in t.ItemsSold:
                out.write("\n%s,%s,%s,%s,%s,%s" % (t.TransID, t.Date, t.Shareholder, t.Total, upc[0], upc[1]))

# #https://medium.com/@jihargifari/how-to-perform-market-basket-analysis-in-python-bd00b745b106

def encode_units(x):
    if x <= 0: return False
    if x >= 1: return True

df = pd.read_csv("trans_target.csv", converters={"UPC":str, "Trans":str, "Quantity":int})

for code in [
    '0000000000052',
    '0000000000055',
    '0000000000556',
    '0000000000725',
    '0000000000765',
    '0000000000768',
    '0000000000769',
    '0000000000769',
    '0000000000825',
    '0000000000925',
    '0000000002201',
    '0000000008750',
    '0000000008757',
    '0000000008763',
    '0000000008798',
    '0000000008798',
    '0000000008798',
    '0000000009002',
    '0008022462800',
    '0008022462800',
    '0008022462800',
    '0097004272021',
    '0097004272022',
    '0097004272023',
    '0097004272024',
    '0097004272025',
    '0097004272026'
]:
    share_number = 0
    non_share_number = 0
    coupon_transactions = [x for x in trans_list for items in x.ItemsSold if code in items]
    #print("coupons", len(coupon_transactions), coupon_transactions)
    for t in (coupon_transactions):
        #t.print()
        #print("Transaction: ", t.TransID)
        if(t.Shareholder != "Non-Shareholder"):
            #print("Shareholder", t.Shareholder)
            share_number+=1
        else:
            #print("Non-Shareholder")
            non_share_number+=1
        #print(t.Total, t.ItemsSold)



    # for t in (coupon_transactions):
    #     if(len(t['Customer']) > 2):
    #         print("Shareholder", t['Customer'])
    #         share_number+=1
    #     else:
    #         print("Non-Shareholder", t['Customer'])
    #         non_share_number+=1
    #     print(t['TOTAL'], t['ANALYSIS ITEMS'])
    #     print("*************************")
    #     # for line in t:
    #     #     print(line, t[line])


    print(code, share_number, non_share_number, share_number+non_share_number)
    # print(coupon_transactions)
