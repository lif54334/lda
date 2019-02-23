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
    host="127.0.0.1",  # neo4j 搭载服务器的ip地址，ifconfig可获取到
    http_port=7474,  # neo4j 服务器监听的端口号
    user="neo4j",  # 数据库user name，如果没有更改过，应该是neo4j
    password="1234"  # 自己设定的密码
)
field = ["惯性约束激光核聚变", "脑机接口"]


def hdata():
    db = pymysql.connect("localhost", "root", "1234", "xian")

    # 使用cursor()方法获取操作游标
    cursor = db.cursor()

    # SQL 查询语句
    sql = "SELECT * FROM light"
    values = list()
    try:
        # 执行SQL语句
        cursor.execute(sql)
        # 获取所有记录列表
        results = cursor.fetchall()
        for row in results:
            value = dict()
            item = list()
            id = row[0]
            title = row[1]
            author = row[2]
            org = row[3]
            abs = row[13]
            key = row[11]
            theme = row[15]
            value["id"] = id
            value["title"] = title
            value["author"] = author
            value["org"] = org
            value["abs"] = abs
            value["key"] = key
            value["theme"] = theme
            values.append(value)
    except:
        print("Error: unable to fetch data")

    # 关闭数据库连接
    return values
    db.close()
    # print(items)


def node_title(data):
    global g
    g.delete_all()
    st = g.begin()

    for items in data:
        # print(items)
        # print(items["key"])
        key = re.sub("\\(.*?\\)|\\{.*?}|\\[.*?]|（.*?）", "", str(items["key"]))
        # print(key)
        field_node = Node("Field", name=field[1])
        article_node = Node("Article", title=items["title"], key=key)
        theme_node = Node("Theme", theme=items["theme"])
        # 属性太长可能会不显示
        # article_node=Node("Article",title=items["title"],author=items["author"],org=items["org"],abs=items["abs"],key=items["key"])
        st.merge(article_node, primary_label="Article", primary_key="title")
        st.merge(theme_node, primary_label="Theme", primary_key="theme")
        st.merge(field_node, primary_label="Field", primary_key="name")
        rel1 = Relationship(theme_node, "have", article_node)
        rel2 = Relationship(theme_node, "belongto", field_node)
        st.merge(rel1)
        st.merge(rel2)
    st.commit()


def node_author2org():
    global g
    # g.delete_all()
    st = g.begin()
    db = pymysql.connect("localhost", "root", "1234", "xian")

    # 使用cursor()方法获取操作游标
    cursor = db.cursor()
    # SQL 查询语句
    sql = "SELECT * FROM author2org"
    values = list()
    try:
        # 执行SQL语句
        cursor.execute(sql)
        # 获取所有记录列表
        results = cursor.fetchall()
        for row in results:
            value = dict()
            author = re.sub("\\(.*?\\)|\\{.*?}|\\[.*?]|（.*?）", "", str(row[0]))
            org = re.sub("\\(.*?\\)|\\{.*?}|\\[.*?]|（.*?）", "", str(row[1]))
            org = org.split(",")
            org = org[0].split("、")
            value["author"] = author
            value["org"] = org[0]
            # value["loc"]=org[1]
            # print(value)
            values.append(value)

    except Exception as e:
        print("Error: unable to fetch data")
        print(e)

    # print(values)
    for items in values:
        author_node = Node("Author", author=items["author"])
        org_node = Node("Org", org=items["org"])
        st.merge(author_node, primary_label="Author", primary_key="author")
        st.merge(org_node, primary_label="Org", primary_key="org")
        rel = Relationship(author_node, "from", org_node)
        st.merge(rel)
    st.commit()


def node_article_theme_au_or(data):
    global g
    # g.delete_all()
    st = g.begin()
    # print(data)
    article2author_l = list()
    theme2author_l = list()
    theme2orgs_l = list()
    for items1 in data:
        authors = re.sub("\\(.*?\\)|\\{.*?}|\\[.*?]|（.*?）", "", items1["author"])
        orgs = re.sub("\\(.*?\\)|\\{.*?}|\\[.*?]|（.*?）", "", items1["org"])
        authors = authors.split(";")
        orgs = re.sub("、", "，", orgs)
        orgs = orgs.split(";")
        for author1 in authors:
            # print (author1)
            article2author_d = dict()
            article2author_d["author"] = author1
            key = re.sub("\\(.*?\\)|\\{.*?}|\\[.*?]|（.*?）", "", str(items1["key"]))
            article2author_d["title"] = items1["title"]
            article2author_d["key"] = key
            article2author_l.append(article2author_d)
            theme2author_d = dict()
            theme2author_d["author"] = author1
            theme2author_d["theme"] = items1["theme"]
            theme2author_l.append(theme2author_d)
        for orgs1 in orgs:
            orgs1 = orgs1.split(",")
            orgs1 = orgs1[0]
            theme2orgs_d = dict()
            theme2orgs_d["theme"] = items1["theme"]
            theme2orgs_d["org"] = orgs1
            theme2orgs_l.append(theme2orgs_d)

    for ele1 in article2author_l:
        article_node = Node("Article", title=ele1["title"], key=ele1["key"])
        author_node = Node("Author", author=ele1["author"])
        st.merge(article_node, primary_label="Article", primary_key="title")
        st.merge(author_node, primary_label="Author", primary_key="author")
        ele1_rel = Relationship(author_node, "write", article_node)
        st.merge(ele1_rel)

    for ele2 in theme2author_l:
        theme_node = Node("Theme", theme=ele2["theme"])
        author_node = Node("Author", author=ele2["author"])
        st.merge(theme_node, primary_label="Theme", primary_key="theme")
        st.merge(author_node, primary_label="Author", primary_key="author")
        ele2_rel = Relationship(theme_node, "include_author", author_node)
        st.merge(ele2_rel)
    for ele3 in theme2orgs_l:
        theme_node = Node("Theme", theme=ele3["theme"])
        org_node = Node("Org", org=ele3["org"])
        st.merge(theme_node, primary_label="Theme", primary_key="theme")
        st.merge(org_node, primary_label="Org", primary_key="org")
        ele3_rel = Relationship(theme_node, "include_orgs", org_node)
        st.merge(ele3_rel)

    st.commit()
    # print (article2author_l)
    # print (theme2author_l)
    # print (theme2orgs_l)
    # print (au2au_l)
    # print (or2or_l)

def node_au2au():
    db = pymysql.connect("localhost", "root", "1234", "xian")

    # 使用cursor()方法获取操作游标
    cursor = db.cursor()

    # SQL 查询语句
    sql = "SELECT * FROM au_clean"
    values = list()
    try:
        # 执行SQL语句
        cursor.execute(sql)
        # 获取所有记录列表
        results = cursor.fetchall()
        for row in results:
            value = dict()
            item = list()
            count = row[0]
            id = row[1]
            content = row[2]
            author1 = row[3]
            author2 = row[4]
            pinyin = row[5]
            value["count"] = count
            value["id"] = id
            value["content"] = content
            value["author1"] = author1
            value["author2"] = author2
            value["pinyin"] = pinyin
            values.append(value)
    except:
        print("Error: unable to fetch data")

    # 关闭数据库连接
    db.close()
    # print(items)
    global g
    # g.delete_all()
    st = g.begin()
    # print (values)
    for ele in values:
        author_node1 = Node("Author", author=ele["author1"])
        author_node2 = Node("Author", author=ele["author2"])
        st.merge(author_node1, primary_label="Author", primary_key="author")
        st.merge(author_node2, primary_label="Author", primary_key="author")
        rel = Relationship(author_node1, "link_au", author_node2, )
        rel["count"]=ele["count"]
        st.merge(rel)
    st.commit()




def node_or2or():
    db = pymysql.connect("localhost", "root", "1234", "xian")

    # 使用cursor()方法获取操作游标
    cursor = db.cursor()

    # SQL 查询语句
    sql = "SELECT * FROM or_clean"
    values = list()
    try:
        # 执行SQL语句
        cursor.execute(sql)
        # 获取所有记录列表
        results = cursor.fetchall()
        for row in results:
            value = dict()
            item = list()
            count = row[0]
            id = row[1]
            content = row[2]
            org1 = row[3]
            org2 = row[4]
            pinyin = row[5]
            value["count"] = count
            value["id"] = id
            value["content"] = content
            value["org1"] = org1
            value["org2"] = org2
            value["pinyin"] = pinyin
            values.append(value)
    except:
        print("Error: unable to fetch data")

    # 关闭数据库连接
    db.close()
    # print(items)
    global g
    # g.delete_all()
    st = g.begin()

    for ele in values:
        org_node1 = Node("Org", org=ele["org1"])
        org_node2 = Node("Org", org=ele["org2"])
        st.merge(org_node1, primary_label="Org", primary_key="org")
        st.merge(org_node2, primary_label="Org", primary_key="org")
        rel = Relationship(org_node1, "link_org", org_node2, )
        rel["count"]=ele["count"]
        st.merge(rel)
    st.commit()

def crt_au_or(data):
    au2au_l = list()
    or2or_l = list()
    for items1 in data:
        authors = re.sub("\\(.*?\\)|\\{.*?}|\\[.*?]|（.*?）", "", items1["author"])
        orgs = re.sub("\\(.*?\\)|\\{.*?}|\\[.*?]|（.*?）", "", items1["org"])
        authors = authors.split(";")
        # print(orgs)
        orgs = re.sub("、", "，", orgs)
        orgs = orgs.split(";")
        au2au = list(itertools.combinations(authors, 2))
        for items2 in au2au:
            au2au_item = list(items2)
            au2au_d = [au2au_item[0], au2au_item[1]]
            # print (au2au_d)
            au2au_l.append(au2au_d)
        or2or = list(itertools.combinations(orgs, 2))
        for items3 in or2or:
            # print (items3)
            or2or_item = list(items3)
            or2or_d = [or2or_item[0].split(",")[0], or2or_item[1].split(",")[0]]
            or2or_l.append(or2or_d)

    """
    尝试建立数据表
    """
    # db = pymysql.connect("localhost", "root", "1234", "xian")
    #
    # # 使用cursor()方法获取操作游标
    # cursor = db.cursor()
    #
    # table_name="au2au"
    # cursor.execute("CREATE TABLE %s(author1 varchar(150),author2 varchar (150),id int (5),content varchar (255),pinyin_au varchar (255))"%table_name)
    #
    # db.commit()
    # cursor.close()
    #
    # db = pymysql.connect("localhost", "root", "1234", "xian")
    #
    # # 使用cursor()方法获取操作游标
    # cursor = db.cursor()
    #
    # table_name="or2or"
    # cursor.execute("CREATE TABLE %s(org1 varchar(150),org2 varchar (150),id int (5),content varchar (255),pinyin_or varchar (255))"%table_name)
    #
    # db.commit()
    # cursor.close()
    """
    尝试插入数据表
    """
    db = pymysql.connect("localhost", "root", "1234", "xian")

    # 使用cursor()方法获取操作游标
    cursor = db.cursor()
    values_author=list()
    nums_au=list(range(0,len(au2au_l)))
    for items1,num in zip(au2au_l,nums_au):
        content=str(items1[0])+str(items1[1])
        pinyin=sorted(content, key=lambda ch: lazy_pinyin(ch))
        pinyin_au="".join(pinyin)
        values_author.append((items1[0],items1[1],num+1,content,pinyin_au))
        # print((items1[0],items1[1],num+1))
    print(values_author)
    cursor.executemany("INSERT INTO au2au values(%s,%s,%s,%s,%s)",values_author)

    values_org=list()
    nums_or = list(range(0, len(or2or_l)))
    for items1, num in zip(or2or_l, nums_or):
        content=str(items1[0])+str(items1[1])
        pinyin=sorted(content, key=lambda ch: lazy_pinyin(ch))
        pinyin_or="".join(pinyin)
        values_org.append((items1[0],items1[1],num+1,content,pinyin_or))
        # print((items1[0],items1[1],num+1))
    print(values_org)
    cursor.executemany("INSERT INTO or2or values(%s,%s,%s,%s,%s)",values_org)

    db.commit()
    cursor.close()

def main():
    data = hdata()
    node_title(data)
    node_author2org()
    node_article_theme_au_or(data)
    # # crt_au_or(data)
    node_or2or()
    node_au2au()

if __name__ == '__main__':
    main()

