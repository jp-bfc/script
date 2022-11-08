import os
import tkinter as tk
from tkinter import filedialog
import pandas as pd
from reportlab.pdfgen.canvas import Canvas
import barcode
from barcode.writer import ImageWriter


##look at implementing this: https://stackoverflow.com/questions/57968285/remove-number-under-barcode-and-write-text-in-python-3

root = tk.Tk()
root.withdraw()

file_path = filedialog.askopenfilename()

info = pd.read_csv(file_path, usecols=['UPC', 'Description'])

canvas = Canvas("BFC-Deli-1OFF.pdf", pagesize=(612, 792))

deleteList = []
i = 0
fullcount = 0

for item in info.iterrows():
    k = 0
    j = i
    if(i > 9):
        k = 200
        j = i - 10
        if(i > 19):
            k = 400
            j = i - 20
    
    code = ((13 - (len(str(item[1].values[0]))))*'0') + ('%s' % item[1].values[0])
    title = []
    title = item[1].values[1].split()
    
    workingCode = barcode.get_barcode_class('code39')(code, add_checksum=False, writer=ImageWriter(format='JPEG'))
    workingCode.save("working_code%s" % fullcount)

    deleteList.append("working_code%s.jpeg" % fullcount)
    items = [title[e:e+3] for e in range(0, len(title), 3)]

    canvas.drawImage("working_code%s.jpeg" % fullcount, x=10 + k, y=(j*78), width=100, height=75)
    
    itemName = canvas.beginText((10 + k + 110), ((j*78)+51))
    for item in items:
        itemName.textLine("%s" % (" ".join(item)))

    canvas.setFontSize(8)
    canvas.drawText(itemName)
    i += 1
    fullcount += 1

    #page break or finish logic
    if(i == 30 or fullcount == len(info)):
        #vertical lines
        canvas.line(205, 0, 205, 792)
        canvas.line(405, 0, 405, 792)

        #horizontal lines
        canvas.line(0, 80, 612, 80)
        canvas.line(0, 158, 612, 158)
        canvas.line(0, 236, 612, 236)
        canvas.line(0, 314, 612, 314)
        canvas.line(0, 392, 612, 392)
        canvas.line(0, 470, 612, 470)
        canvas.line(0, 548, 612, 548)
        canvas.line(0, 626, 612, 626)
        canvas.line(0, 702, 612, 702)

        #save page finishes the drawing of the current page,
        # and if there is more data, we begin working with the next page after this
        canvas.showPage()
        
        i = 0
        
canvas.save()

#for d in deleteList:
#    os.remove(d)