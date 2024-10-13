# -*- coding: utf-8 -*-
"""
Created on Tue Jan 21 22:16:32 2020

@author: gadaj
"""
import numpy as np
from sklearn.datasets import load_iris
from sklearn import tree

iris = load_iris()
print(iris.feature_names)
print(iris.target_names)
print(iris.data[0])
print(iris.target[0])

test_idx = [0,50,100]

# training data
train_target = np.delete(iris.target, test_idx)
train_data = np.delete(iris.data, test_idx, axis = 0)

print(train_data)
print(train_target)
# testing_data
test_target = iris.target[test_idx]
test_data = iris.data[test_idx]

clf = tree.DecisionTreeClassifier()
clf.fit(train_data, train_target)

print(test_target)
print(clf.predict(test_data))

import graphviz 
from sklearn.externals.six import StringIO
import pydot
dot_data = StringIO()

tree.export_graphviz(clf, out_file=dot_data, 
                      feature_names=iris.feature_names,  
                      class_names=iris.target_names,  
                      filled=True, rounded=True,  
                      special_characters=True)  
graph = pydot.graph_from_dot_data(dot_data.getvalue())  
graph.write_pdf("iris.pdf")
graph 