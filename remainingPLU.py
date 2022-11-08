#Simple program to take a list of PLUs from a Department in SMS, and return to you, the user, a list of PLUs we have not yet used for that department.
#good for identifying usable ranges of PLUs if you'd like to group new items together.

import pandas as pd
import tkinter as tk
from tkinter import filedialog

root = tk.Tk()
root.withdraw()

file_path = filedialog.askopenfilename()

#file to be read from should be a .csv with the single header "PLU"
info = pd.read_csv(file_path, usecols=['PLU'])

fullrange = [i for i in range(999)]


#the idea is just cycle through the range of possible PLUs - [i in range(999)] - and if that PLU has already been used, remove it from the list
for row in info.iterrows():
    if(row[1].values in fullrange):
        fullrange.remove(row[1].values)

#write non-used numbers to the output file
with open('Remaining_PLUs_For_Use.txt', 'w') as f:
    f.write("PLUs Not Used:\n")
    for p in fullrange:
        f.write(str(p) + "\n")