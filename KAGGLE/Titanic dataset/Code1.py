# -*- coding: utf-8 -*-
"""
Created on Sun Aug  6 17:13:38 2017

@author: jigar
"""

import csv
import pandas as pd

#f = open('D:/Github/JCode/JCode/KAGGLE/Titanic dataset/train.csv')
csv_f = pd.read_csv('D:/Github/JCode/JCode/KAGGLE/Titanic dataset/train.csv')

for row in csv_f:
    print (row)
    
print (csv_f.describe())

