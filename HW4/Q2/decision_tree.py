from util import entropy, information_gain, partition_classes
import numpy as np 
import ast

class DecisionTree(object):
    def __init__(self):
        # Initializing the tree as an empty dictionary or list, as preferred
        #self.tree = []
        #self.tree = {}

        self.tree = {}

    def learn(self, X, y):
        # TODO: Train the decision tree (self.tree) using the the sample X and labels y
        # You will have to make use of the functions in utils.py to train the tree
        
        # One possible way of implementing the tree:
        #    Each node in self.tree could be in the form of a dictionary:
        #       https://docs.python.org/2/library/stdtypes.html#mapping-types-dict
        #    For example, a non-leaf node with two children can have a 'left' key and  a 
        #    'right' key. You can add more keys which might help in classification
        #    (eg. split attribute and split value)

        self.tree = self.learn_helper(X, y)
        return self.tree

    def learn_helper(self, X, y):
        # If all the datapoints in X have the same class value y, return a leaf node that predicts y as output
        if y.count(y[0]) == len(y):
            return y[0]
        # If all the datapoints in X have the same attribute value (x1,x2...xm), return a leaf node that predicts the majority of the class values in Y as output
        if X.count(X[0]) == len(X):
            counts = np.bincount(y)
            return np.argmax(counts)

        else:
            tree = {}
            # Find split feature and value
            split_feature, split_val = self.find_feature(X,y)         
            # Do the splitting
            leftX, rightX, lefty, righty = partition_classes(X, y, split_feature, split_val)
            # Store this level's information. Go to next level and continue
            tree[split_feature] = [split_val, self.learn_helper(leftX, lefty), self.learn_helper(rightX, righty)]
            return tree

    def find_feature(self, X, y):
        maxInfoGain = 0
        feature = 0
        value = 0
        for i in range(len(X[0])):
            for index in range(len(X)):
                leftX, rightX, leftY, rightY = partition_classes(X,y,i,X[index][i])
                cur = []
                cur.append(leftY)
                cur.append(rightY)
                temp = information_gain(y, cur)
                if temp > maxInfoGain:
                    feature = i
                    value = X[index][i]
                    maxInfoGain = temp
        return feature, value

    def classify(self, record):
        # TODO: classify the record using self.tree and return the predicted label
        node = self.tree
        while isinstance(node, dict):
            feature = node.keys()[0]
            t = type(record[feature])
            # To determine whether the split attribute is numeric or categorical
            if t is int or t is float or t is long or t is complex:
                if record[feature] <= node[feature][0]:
                    node = node[feature][1]
                else:
                    node = node[feature][2]
            else:
                if record[feature] == node[feature][0]:
                    node = node[feature][1]
                else:
                    node = node[feature][2]                
        return node
