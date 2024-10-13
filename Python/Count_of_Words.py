# -*- coding: utf-8 -*-
"""
Created on Wed Jan 10 20:18:11 2018

@author: jigar
"""

def wrdcount(strg, wrd):
    """startingPos = 0
    for position in strg.find(wrd, startingPos, len(strg)):
   while position > -1:
        print ("Starting position:", position)
        startingPos+=1 """
    print("Number of words in string is:",strg.count(wrd))
    print (string)
    print(wrd)
    return

string = "This is just a very long string to find the number of occurance of words and we will calculate the occurance count of any given word"
word = input("Enter a word to count in string: ")
wrdcount(string, word)
