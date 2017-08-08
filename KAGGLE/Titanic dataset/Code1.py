# -*- coding: utf-8 -*-
"""
Created on Sun Aug  6 17:13:38 2017

@author: jigar
"""

import csv

f = open('C:/Users/jigar/Desktop/KAGGLE/Titanic dataset/train.csv')
csv_f = csv.reader(f)

for row in csv_f:
    print (row)
    