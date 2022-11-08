from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import pyodbc
import tkinter as tk
from tkinter import filedialog
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
#from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient import discovery
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import os

SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

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
# <span class="normal dark-gray mb0 mt1 lh-title f3" data-automation-id="product-title">
#                          Freshness Guaranteed Jazz Apples, 3 lb Bag
# <div aria-hidden="true" class="mr1 mr2-xl lh-copy b black f1">
#                           $5.88
#                          </div>
# <div class="f7 f6-l gray mr1">
#                           $1.96/lb
#                          </div>

with open(r"\\bfc-hv-01\SWAP\PythonScripts\Competitors\Hannafords\Fall Produce in Fresh Produce - Walmart.com.html", 'rb') as fp:
    soup = BeautifulSoup(fp, 'html.parser')
    first = soup.find_all('div', {"class":"mb1 ph1 pa0-xl bb b--near-white w-25 bb b--near-white w-100 w-100 ph2 mb3"})
    i = 0
    for f in first:
        name = f.find_all('span', {"data-automation-id":"product-title"})
        price = f.find_all('div', {"data-automation-id":"product-price"})
        #mr1 mr2-xl lh-copy b black f1
        # current_price = price[0].find_all('span', {"class" : "w_Bq"})
        # avg_price = price[0].find_all('div', {"class" : "gray f5"})
        # prev_price = price[0].find_all('div', {"class" : "gray mr1 strike f5"})
        # unit_price = price[0].find_all('div', {"class" : "f7 f6-l gray mr1"})
        
        # #print(i, f.prettify())
        # print(i, [p.text for p in name], len(current_price), [p.text for p in price],[p.text for p in current_price], [p.text for p in avg_price], [p.text for p in prev_price], [p.text for p in unit_price])
        # i+=1
        print(name[0].text, price[0].text)
        i+=1
    f = open("product_price.txt", 'w')
    f.write(soup.prettify())
    f.close()



# config = configparser.ConfigParser()
# config.read('script_configs.ini')
# username = config['DEFAULT']['user']
# password = config['DEFAULT']['password']
# server = config['DEFAULT']['server']
# port = config['DEFAULT']['port']
# database = config['DEFAULT']['database']
# cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';PORT='+port+';DATABASE='+database+';UID='+username+';PWD='+ password)
# cursor = cnxn.cursor()

# competitor_folder = config['DEFAULT']['competitor_folder']
# book = createBook("Competitor Pricing - October 2022", competitor_folder, 'application/vnd.google-apps.spreadsheet', drive)
# book_id = book.get('id', '')

# items = pd.read_sql("""
# SELECT obj.F01 as 'UPC', obj.F29 as 'Desc', sdp.F1022 as 'Sub-Dept', prc.F30 as 'Price', cos.F1140 as 'Unit Cost',OBJ.F22 as 'Size'
# FROM STORESQL.dbo.OBJ_TAB obj
# inner join STORESQL.dbo.RPC_TAB rpc on rpc.F18 = obj.F18 
# inner join STORESQL.dbo.POS_TAB pos on pos.F01 = obj.F01 
# inner join STORESQL.dbo.SDP_TAB sdp on sdp.F04 = pos.F04
# inner join STORESQL.dbo.COST_TAB cos on OBJ.F01 = cos.f01
# inner join STORESQL.dbo.PRICE_TAB prc on OBJ.F01 = prc.F01
# where rpc.F18 in (2, 18)
# """, cnxn)
# response = batchUpdate(book_id, addSheet(sheetProp(title="Data", gridProperties=gridProp(items.shape[0]+1, items.shape[1], 1))))
# data_id = response.get('replies', '')[0].get('addSheet', '').get('properties', '').get('sheetId', '')
# append(book_id, "Data!A1:B1", "ROWS", [items.columns.tolist()], "RAW")
# append(book_id, "Data!A2:B2", "ROWS", items.astype(str).to_numpy().tolist(), "RAW")
# format_updates = []
# for filename in os.listdir(r"\\bfc-hv-01\SWAP\PythonScripts\Competitors\Hannafords"):
#     if filename.endswith(".html"):
#         print(filename)
#         group = filename.split("_")[0][:-1]
#         print(group)
#         with open(r"\\bfc-hv-01\SWAP\PythonScripts\Competitors\Hannafords\\"+filename, 'rb') as fp:
#             soup = BeautifulSoup(fp, 'html.parser')

#             # all_sizes = soup.find_all("span", {"class":"overline text-truncate"})
#             # all_names = soup.find_all("span", {"class":"real-product-name"})
#             # all_prices = soup.find_all("span", {"class":"price item-unit-price"})
#             # all_unit_price = soup.find_all("p", {"class":"unitPriceDisplay"})
#             # print(len(all_sizes))
#             # print(len(all_names))
#             # print(len(all_prices))
#             # print(len(all_unit_price))
#             # #print(all_names)
#             # ls = []
#             # for i in range(len(all_names)):
#             #     ls.append[[all_sizes[i].text.strip(), all_names[i].text.strip(), all_prices[i].text.strip(), all_unit_price[i].text.strip()]]
#             # df = pd.DataFrame(ls, columns=['Size', 'Name', 'Price', 'Unit Price'])
#             # print(df)
#             k=0
#             all_products = soup.find_all("div", {"class":"productPriceInfoWrap"})
#             table = []
#             for p in all_products:
#                 item_features = []
#                 all_sizes = p.find_all("span", {"class":"overline text-truncate"})
#                 all_names = p.find_all("span", {"class":"real-product-name"})
#                 all_prices = p.find_all("span", {"class":"price item-unit-price"})
#                 all_unit_price = p.find_all("p", {"class":"unitPriceDisplay"})
#                 item_features.append(all_sizes[0].text.strip() if len(all_sizes)>0 else "")
#                 item_features.append(all_names[0].text.strip() if len(all_names)>0 else "")
#                 if len(all_prices) > 0:
#                     #sale price
#                     item_features.append("")
#                     item_features.append(all_prices[0].text.strip())
#                 else:
#                     normal_price = p.find_all("span", {"class":"price item-unit-price strike-price"})
#                     sale_price = p.find_all("span", {"class":"salePrice item-unit-price"})
#                     item_features.append(sale_price[0].text.strip() if len(sale_price)>0 else "")
#                     item_features.append(normal_price[0].text.strip() if len(normal_price)>0 else "")
#                 item_features.append(all_unit_price[0].text.strip() if len(all_unit_price)>0 else "")
#                 # spans = p.find_all("span")
#                 # for columns in spans:
#                 #     # if len(spans) > 4:
#                 #     #     print(columns['class'], columns.text.strip())
#                 #     print(k, columns["class"], columns.text.strip(), len(spans))
#                 #     item_features.append(columns.text.strip())
                
#                 # up = p.find_all("p", {"class":"unitPriceDisplay"})
#                 # print(up)
#                 # if len(up)>0:
#                 #     item_features.append(up[0].text.strip())
#                 # k+=1
#                 # if len(item_features) <= 3:
#                 #     print(item_features)
#                 table.append(item_features)
#             df = pd.DataFrame(data=table, columns=["Size", "Name", "Sale Price", "Normal Price", "Unit Price"])
#             print(df)
#             # for row in table:
#             #     if len(row) == 6: print(row)
#             #     print(len(row))
#             # d = {}
#             # for p in all_products:
#             #     print(p.text)
#             #     nextSoup = BeautifulSoup(p, lxml)
#             #     price = nextSoup.find_all("span", {"class":"price item-unit-price"})
#             #     print(k, price)
#             #     k+=1
#             items = []
#             i = 0
#             # text = [x.text for x in all_products]
#             # text=filter(None, text)

#             # table = [[i.getText(strip=True) for i in row] for row in all_products]
            
#             text=[" ".join(c.text.split()) for c in all_products]
#             for p in text:
#                 i+=1
#                 print(i, p)
#                 #print(i, [p.text.stripped_string])
#             print(all_products[-1])
#             print(table)
#             f = open("product_price.txt", 'w')
#             f.write("\n".join(text))
#             f.close()
#         df = df[['Name', 'Normal Price', 'Sale Price', 'Size','Unit Price']]
#         #df = df.reindex(columns = df.columns.tolist() + ['Similar BFC UPC 1', 'Our Price 1', 'Our Unit Cost 1', 'Our Size 1', 'Sub-Dept 1', 'Similar BFC UPC 2', 'Our Price 2', 'Our Unit Cost 2', 'Our Size 2', 'Sub-Dept 2'])
#         df[['Similar BFC UPC 1', 'Name 1', 'Our Price 1', 'Our Unit Cost 1', 'Our Size 1', 'Sub-Dept 1', 'Price Diff 1', 'Similar BFC UPC 2', 'Name 2', 'Our Price 2', 'Our Unit Cost 2', 'Our Size 2', 'Sub-Dept 2', 'Price Diff 2']] = ""
#         print(df)
#         #=IF(ISBLANK(A3)=FALSE, IFNA(INDEX(Data!C:C, MATCH(A3, Data!B:B, 0)), INDEX(Data!C:C, MATCH(A3, Data!A:A, 0))), )
#         response = batchUpdate(book_id, addSheet(sheetProp(title=group, gridProperties=gridProp(df.shape[0]+1, df.shape[1], 1))))
#         sheet_id = response.get('replies', '')[0].get('addSheet', '').get('properties', '').get('sheetId', '')
#         append(book_id, group+"!A1:B1", "ROWS", [df.columns.tolist()], "RAW")
#         append(book_id, group+"!A2:B2", "ROWS", df.astype(str).to_numpy().tolist(), "USER_ENTERED")
#         format_updates.extend([
#             repeatCell(
#                 gridRange(sheet_id, 0, 1, 0, 5),
#                 cellData(userEnteredFormat=cellFormat(
#                     borders=borders(*all_thick),
#                     textFormat=textFormat(fontFamily="Arial", fontSize=14, bold=True),
#                     backgroundColor=getColor(235, 235, 0, 0.01),
#                     horizontalAlignment="CENTER",
#                     verticalAlignment="MIDDLE"
#                     ))
#             ),
#             repeatCell(
#                 gridRange(sheet_id, 0, 1, 5, 12),
#                 cellData(userEnteredFormat=cellFormat(
#                     borders=borders(*all_thick),
#                     textFormat=textFormat(fontFamily="Arial", fontSize=14, bold=True),
#                     backgroundColor=getColor(0, 235, 0, 0.01),
#                     horizontalAlignment="CENTER",
#                     verticalAlignment="MIDDLE"
#                     ))
#             ),
#             repeatCell(
#                 gridRange(sheet_id, 0, 1, 12, df.shape[1]),
#                 cellData(userEnteredFormat=cellFormat(
#                     borders=borders(*all_thick),
#                     textFormat=textFormat(fontFamily="Arial", fontSize=14, bold=True),
#                     backgroundColor=getColor(0, 235, 235, 0.01),
#                     horizontalAlignment="CENTER",
#                     verticalAlignment="MIDDLE"
#                     ))
#             ),
#             repeatCell(
#                 gridRange(sheet_id, 1, df.shape[0]+1, 0, df.shape[1]),
#                 cellData(userEnteredFormat=cellFormat(
#                     borders=borders(*all_solid),
#                     textFormat=textFormat(fontFamily="Arial", fontSize=12)
#                     ))
#                 ),
#             repeatCell(
#                 gridRange(sheet_id, 1, df.shape[0]+1, 6, 7),
#                 cellData(userEnteredValue=extendedValue(formulaValue="=IF(ISBLANK($F2)=FALSE, IFNA(INDEX(Data!B:B, MATCH($F2, Data!$A:$A, 0)), INDEX(Data!B:B, MATCH($F2, Data!$A:$A*1, 0))), )")),
#                 ),
#             repeatCell(
#                 gridRange(sheet_id, 1, df.shape[0]+1, 7, 8),
#                 cellData(userEnteredValue=extendedValue(formulaValue="=IF(ISBLANK($F2)=FALSE, IFNA(INDEX(Data!D:D, MATCH($F2, Data!$A:$A, 0)), INDEX(Data!D:D, MATCH($F2, Data!$A:$A*1, 0))), )")),
#                 ),
#             repeatCell(
#                 gridRange(sheet_id, 1, df.shape[0]+1, 8, 9),
#                 cellData(userEnteredValue=extendedValue(formulaValue="=IF(ISBLANK($F2)=FALSE, IFNA(INDEX(Data!E:E, MATCH($F2, Data!$A:$A, 0)), INDEX(Data!E:E, MATCH($F2, Data!$A:$A*1, 0))), )")),
#                 ),
#             repeatCell(
#                 gridRange(sheet_id, 1, df.shape[0]+1, 9, 10),
#                 cellData(userEnteredValue=extendedValue(formulaValue="=IF(ISBLANK($F2)=FALSE, IFNA(INDEX(Data!F:F, MATCH($F2, Data!$A:$A, 0)), INDEX(Data!F:F, MATCH($F2, Data!$A:$A*1, 0))), )")),
#                 ),
#             repeatCell(
#                 gridRange(sheet_id, 1, df.shape[0]+1, 10, 11),
#                 cellData(userEnteredValue=extendedValue(formulaValue="=IF(ISBLANK($F2)=FALSE, IFNA(INDEX(Data!C:C, MATCH($F2, Data!$A:$A, 0)), INDEX(Data!C:C, MATCH($F2, Data!$A:$A*1, 0))), )")),
#                 ),
#             repeatCell(
#                 gridRange(sheet_id, 1, df.shape[0]+1, 11, 12),
#                 cellData(userEnteredValue=extendedValue(formulaValue="=IF(ISBLANK($F2)=FALSE, H2-B2, )")),
#                 ),
#             autoSize(sheet_id, "COLUMNS")
#         ])
#         # from selenium import webdriver
#         # from webdriver_manager.chrome import ChromeDriverManager
#         # from selenium.webdriver.common.by import By

#         # driver = webdriver.Chrome(ChromeDriverManager().install())

#         # driver.get("https://www.brattleborofoodcoop.coop/")
#         # drop_down = driver.find_element(by=By.LINK_TEXT, value="Full Calendar")
#         # drop_down.click()
#         # print(driver.title)
#         # #driver.close()
# batchUpdate(book_id, format_updates)