#!/usr/bin/env python

# -*- encoding: utf-8 -*-

'''
@Author  :   {lif54334}

@Software:   PyCharm

@File    :   search.py

@Time    :   2019/1/22 19:33

@Desc    :

'''


from py2neo import Node, Relationship, Graph

g = Graph(
    host="127.0.0.1",  # neo4j 搭载服务器的ip地址，ifconfig可获取到
    http_port=7474,  # neo4j 服务器监听的端口号
    user="neo4j",  # 数据库user name，如果没有更改过，应该是neo4j
    password="1234"  # 自己设定的密码
)
