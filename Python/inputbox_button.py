# -*- coding: utf-8 -*-
"""
Created on Sun Jan 24 23:17:23 2021

@author: gadaj
"""

import tkinter as tk
    

#def write_slogan():
#    print("Tkinter is easy to use!")


def weightconversion():  
    x1 = entry1.get()
    label1 = tk.Label(root, text = str(round(float(x1)* 2.20462, 2)) + " lbs")
    canvas1.create_window(200, 230, window = label1)
    
root = tk.Tk()
#frame = tk.Frame(root)
canvas1 = tk.Canvas(root, width = 400, height = 500)
canvas1.pack()

entry1 = tk.Entry(root)
canvas1.create_window(200, 140, window = entry1)

#button = tk.Button(canvas1, 
#                  text="QUIT", 
#                  fg="red",
#                  command=quit)
#button.pack(side=tk.LEFT)
button1 = tk.Button(text="Enter Weight in KG",
                   command=weightconversion)
canvas1.create_window(200, 180, window=button1)

root.mainloop()