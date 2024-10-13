# -*- coding: utf-8 -*-
"""
Created on Sun Jan 24 23:17:23 2021

@author: gadaj
"""

import tkinter as tk
    

#def write_slogan():
#    print("Tkinter is easy to use!")


def kgtolbs():  
    x1 = entry1.get()
    label1 = tk.Label(root, text = str(round(float(x1)* 2.20462, 2)) + " lbs")
    canvas1.create_window(200, 230, window = label1)
    
def lbstokg():  
    x1 = entry1.get()
    label1 = tk.Label(root, text = str(round(float(x1)* 0.453592, 2)) + " kg")
    canvas1.create_window(200, 230, window = label1)
    
root = tk.Tk()
#frame = tk.Frame(root)
canvas1 = tk.Canvas(root, width = 400, height = 500)
canvas1.pack()

entry2 = tk.Entry(root, text="Enter Height")
canvas1.create_window(200, 140, window = entry2)
entry2.pack(side=tk.TOP)

entry1 = tk.Entry(root, text="Enter Weight")
canvas1.create_window(200, 140, window = entry1)
entry1.pack(side=tk.BOTTOM)


#button = tk.Button(canvas1, 
#                  text="QUIT", 
#                  fg="red",
#                  command=quit)
#button.pack(side=tk.LEFT)
button1 = tk.Button(text="Convert KG to Pounds",
                   command=kgtolbs)
canvas1.create_window(200, 180, window=button1)
button1.pack(side=tk.LEFT)


button2 = tk.Button(text="Convert Pounds to KG" ,
                   command=lbstokg)
canvas1.create_window(200, 180, window = button2)
button2.pack(side=tk.RIGHT)

root.mainloop()