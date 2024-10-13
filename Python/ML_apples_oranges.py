# -*- coding: utf-8 -*-
"""
Created on Tue Jan 21 22:35:04 2020

@author: gadaj
"""

from sklearn import tree

# 0 for bumpy 1 for smooth
#0 for apple and 1 for Oragnes

features = [[140,1], [130, 1], [150,0], [170,0]]
label = [0,0,1,1]
clf = tree.DecisionTreeClassifier()
clf = clf.fit(features, label)
print(clf.predict([[150, 0]]))

