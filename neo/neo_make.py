#!/usr/bin/env python

# -*- encoding: utf-8 -*-

'''
@Author  :   {lif54334}

@Software:   PyCharm

@File    :   neo_test.py

@Time    :   2019/1/21 14:47

@Desc    :

'''
import itertools
from pypinyin import lazy_pinyin, pinyin
import pymysql
import re
from py2neo import Graph, Node, Relationship

g = Graph(
    host="127.0.0.1",
    http_port=7474,
    user="neo4j",
    password="1234"
)
field = ["惯性约束激光核聚变", "脑机接口"]


def use_data(name):
    db = pymysql.connect("localhost", "root", "1234", "xian")
    cursor = db.cursor()
    sql = ("SELECT * FROM  %s " % (name))
    values = list()
    try:
        cursor.execute(sql)
        results = cursor.fetchall()
        for row in results:
            value = dict()
            value["title"] = row[0]
            value["abs"] = row[1]
            value["person"] = row[3]
            value["time"] = row[2]
            value["theme"] = row[4]
            values.append(value)
    except:
        print("Error: unable to fetch data")
    return values
    db.close()


def merge(Node_name_1, Node_att_1, Node_key_1, Node_name_2, Node_att_2, Node_key_2, rel_name,
                      rel_att):
    global g
    st = g.begin()
    if Node_att_1 is None:
        Node_att_1 = "NONE"
    if Node_att_2 is None:
        Node_att_2 = "NONE"
    Create_Node_1 = Node(Node_name_1, value=str(Node_att_1), key=Node_key_1)
    Create_Node_2 = Node(Node_name_2, value=str(Node_att_2), key=Node_key_2)
    st.merge(Create_Node_1, primary_label=Node_name_1, primary_key='value')
    st.merge(Create_Node_2, primary_label=Node_name_2, primary_key='value')
    rel = Relationship(Create_Node_1, rel_name, Create_Node_2)
    if rel_att:
        rel[rel_att] = rel_att
    else:
        pass
    st.merge(rel)
    st.commit()

def Node_merge_classification(items, Node_name_1, Node_att_1, Node_key_1, Node_name_2, Node_att_2, Node_key_2, rel_name,
                      rel_att,Category):

    if Category[0] ==-1:
        Node_att_1 = items[Node_att_1]
        Node_att_2 = items[Node_att_2]
        if rel_att:
            rel_att = items[rel_att]
        else:
            pass
        merge(Node_name_1, Node_att_1, Node_key_1, Node_name_2, Node_att_2, Node_key_2, rel_name,
                      rel_att)
    else:
        if Category[0]==-2:
            mylist=items[(Category[1])].split('; ')
            for i in mylist:
                Node_att_2 = i
                if rel_att:
                    rel_att = items[rel_att]
                else:
                    pass
                merge(Node_name_1, items[Node_att_1], Node_key_1, Node_name_2, Node_att_2, Node_key_2, rel_name,
                      rel_att)
        elif Category[0]==-3:
            if rel_att:
                rel_att = items[rel_att]
            else:
                pass
            mylist=items[(Category[1])].split('; ')
            if len(mylist)==1:
                pass
            else:
                mylist=list(itertools.combinations(mylist,2))
                for i in mylist:
                    Node_att_1=list(i)[0]
                    Node_att_2=list(i)[1]
                    merge(Node_name_1, Node_att_1, Node_key_1, Node_name_2, Node_att_2, Node_key_2, rel_name,
                      rel_att)
        elif Category[0]==-4:
            list1=items[(Category[1])].split('; ')
            list2=items[(Category[2])].split('; ')
            if rel_att:
                rel_att = items[rel_att]
            else:
                pass
            for i in range(0,int(len(list1))):
                Node_att_1 = list1[i]
                Node_att_2 = list2[i]
                merge(Node_name_1, Node_att_1, Node_key_1, Node_name_2, Node_att_2, Node_key_2, rel_name,
                      rel_att)
        else:
            pass

def main():
    global g
    g.delete_all()
    data = use_data(name="zl_bci")
    for items in data:
        Node_key_1=None
        Node_key_2=None
        Node_merge_classification(items, Node_name_1="Title", Node_att_1="title", Node_key_1=Node_key_1, Node_name_2="Theme",
                      Node_att_2="theme", Node_key_2=Node_key_2, rel_name="from", rel_att=None,Category=[-1])
        Node_merge_classification(items, Node_name_1="Title", Node_att_1="title", Node_key_1=Node_key_1, Node_name_2="Person",
                          Node_att_2="person", Node_key_2=Node_key_2, rel_name="published", rel_att=None, Category=[-2,"person"])
        Node_merge_classification(items, Node_name_1="Person", Node_att_1="title", Node_key_1=Node_key_1, Node_name_2="Person",
                          Node_att_2="person", Node_key_2=Node_key_2, rel_name="cooperation", rel_att=None, Category=[-3,"person"])

if __name__ == '__main__':
    main()
