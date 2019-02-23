#!/usr/bin/env python

# -*- encoding: utf-8 -*-

'''
@Author  :   {lif54334}

@Software:   PyCharm

@File    :   sql_clean.py

@Time    :   2019/1/22 23:09

@Desc    :

'''
import pymysql

db = pymysql.connect("localhost", "root", "1234", "xian")

# 使用cursor()方法获取操作游标
cursor = db.cursor()

# SQL 查询语句
sql = "SELECT * FROM au2au"
items = list()
try:
    # 执行SQL语句
    cursor.execute(sql)
    # 获取所有记录列表
    results = cursor.fetchall()
    for row in results:
        item = list()
        author1 = row[0]
        author2 = row[1]
        content=str(author1)+str(author2)
        content.strip()
        items.append(content)
except:
    print("Error: unable to fetch data")

# 关闭数据库连接

print(len(items))
items=list(set(items))
print(len(items))

# sql_update="update au2au set content = '%s'where id = '%d'"
# for i in range(0,len(items)):
#     cursor.execute(sql_update % (items[i],i+1))
# # cursor.executemany(sql_update,data)
# db.commit()
# cursor.close()
# db.close()


# neo4j-admin dump --database=graph.db --to=F:\文档\工程\database\graph_bci.db.dump
# neo4j-admin load --from=F:\文档\工程\database\graph_bci.db.dump --force
# neo4j-admin dump --database=graph.db --to=F:\文档\工程\database\graph_light.db.dump
# neo4j-admin load --from=F:\文档\工程\database\graph_light.db.dump --force
