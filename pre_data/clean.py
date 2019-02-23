#!/usr/bin/env python

# -*- encoding: utf-8 -*-

'''
@Author  :   {lif54334}

@Software:   PyCharm

@File    :   clean.py

@Time    :   2019/1/20 18:08

@Desc    :

'''
import pymysql


def hdata():
    db = pymysql.connect("localhost", "root", "1234", "xian")

    # 使用cursor()方法获取操作游标
    cursor = db.cursor()

    # SQL 查询语句
    sql = "SELECT * FROM light"
    items=list()
    try:
        # 执行SQL语句
        cursor.execute(sql)
        # 获取所有记录列表
        results = cursor.fetchall()
        with open("abs.txt", "w", encoding="utf8") as f:
            for row in results:
                item=list()
                id = row[0]
                title = row[1]
                author = row[2]
                org = row[3]
                abs=row[13]
                item.append([author,org])
                # print(item)
                item1 = item[0][0].split(";")
                item2 = item[0][1].split(";")
                items.append([item1,item2])
                f.write(abs+"\n")

    except:
        print("Error: unable to fetch data")

    # 关闭数据库连接
    db.close()
    # print(items)
    return items

def clean(data):
    # print(data[0])
    relats=list()
    for items in data:
        item1=items[0]
        item2=items[1]
        # print(item2)
        for ele in item1:
            if "1"in ele:
                rel=[ele,item2[0]]
                relats.append(rel)
            elif "2"in ele:
                rel=[ele,item2[1]]
                relats.append(rel)
            elif "3"in ele:
                rel=[ele,item2[2]]
                relats.append(rel)
            elif "4"in ele:
                rel=[ele,item2[3]]
                relats.append(rel)
            elif "5"in ele:
                rel=[ele,item2[4]]
                relats.append(rel)
            else:
                rel = [ele, item2[0]]
                relats.append(rel)
    return relats

def creat():

    db = pymysql.connect("localhost", "root", "1234", "xian")

    # 使用cursor()方法获取操作游标
    cursor = db.cursor()

    table_name="author2org"
    cursor.execute("CREATE TABLE %s(author varchar(150),org varchar (150))"%table_name)


def insertauthor2org(data):
    values=list()
    for items in data:
        values.append((items[0],items[1]))
    # print(values)
    print(len(values))
    db = pymysql.connect("localhost", "root", "1234", "xian")

    # 使用cursor()方法获取操作游标
    cursor = db.cursor()

    cursor.executemany("INSERT INTO author2org values(%s,%s)",values)
    db.commit()
    cursor.close()



def main():
    data=hdata()
    # data2=clean(data)
    # creat()
    # insertauthor2org(data2)


if __name__ == '__main__':
    main()