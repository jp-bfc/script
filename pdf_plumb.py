import pdfplumber as plum
import pandas as pd
import tkinter as tk
from tkinter import filedialog

root = tk.Tk()
root.withdraw()

file_path = filedialog.askopenfilename()

with plum.open(file_path) as pdf:
    next_page_continue=True
    # for i in pdf.pages:
    #     for l in i.extract_tables():
    #         print(l)
    df = pd.DataFrame(data=pdf.chars)
    print(df)
    df.to_csv("test_plumber.csv")
    rect = pd.DataFrame(data=pdf.edges)
    rect.to_csv("test_rect.csv")
    grouped = df.groupby("doctop")['text'].transform(lambda x:''.join(x))
    #print(grouped)
    for g in grouped:
        print(g)
    # ls = [c for c, l in zip(pdf.chars, pdf.lines) if ]
    # print("".join([x['text'] for x in pdf.chars]))
    # print("".join([x['text'] for x in pdf.lines]))
    # for i in pdf.chars:
    #     #for e in i:
    #     print(f"{i['text']}")
    # print("*****************************************")
    # print("OBJECTS", pdf.objects)
    #print("*****************************************")
    #print("CHARS", ["***********\n\n\n********" + str(e) for e in pdf.chars])
    #print("*****************************************")
    # print("LINES", pdf.lines)
    # print("*****************************************")
    # print("RECTS", pdf.rects)
    # print("*****************************************")
    # print("RECT_EDGE",pdf.rect_edges)
    # print("*****************************************")
    # print("CURVES", pdf.curves)
    # print("*****************************************")
    # print("IMAGES", pdf.images)
    # print("*****************************************")
#    print(pdf.pages[0].extract_tables())