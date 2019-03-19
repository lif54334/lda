#!/usr/bin/env python

# -*- encoding: utf-8 -*-

'''
@Author  :   {lif54334}

@Software:   PyCharm

@File    :   test.py

@Time    :   2019/3/19 18:09

@Desc    :

'''
import itertools

list1=["ssss","sswe","dfaa"]
from itertools import combinations
a = [1, 2, 3, 4]
for i in itertools.combinations(a,2):
    print(type(i))
    print(list(i))
s = [i for i in itertools.combinations(a,2)]
print(s)