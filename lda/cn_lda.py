#!/usr/bin/env python

# -*- encoding: utf-8 -*-

'''
@Author  :   {lif54334}

@Software:   PyCharm

@File    :   cn_lda.py

@Time    :   2019/1/20 16:14

@Desc    :

'''
import jieba
import codecs
import pymysql
from gensim import corpora, models
from gensim.models import LdaModel
from gensim.corpora import Dictionary
import numpy as np
from collections import Counter

jieba.load_userdict("userdict.txt")


def sdata():
    db = pymysql.connect("localhost", "root", "1234", "xian")

    # 使用cursor()方法获取操作游标
    cursor = db.cursor()

    # SQL 查询语句
    sql = "SELECT * FROM zl_light"
    try:
        # 执行SQL语句
        cursor.execute(sql)
        # 获取所有记录列表
        results = cursor.fetchall()
        with open("abs.txt", "w", encoding="utf8") as f:
            for row in results:
                title = row[0]
                abs = row[1]
                time = row[2]
                person = row[3]
                if u'\u4e00' <= abs[3] <= u'\u9fff':
                    f.write(abs + "\n")
                # 打印结果
                # print("id=%s,title=%s,key=%s,abs=%s" % \
                #       (id,title,key,abs))
    except Exception as e:
        print(e)
        print("Error: unable to fetch data")

    # 关闭数据库连接
    db.close()


def cut():
    with open('abs.txt', 'r', encoding="utf8") as f:
        for line in f:
            seg = jieba.cut(line.strip(), cut_all=False)
            # print(seg)
            s = '/'.join(seg)
            m = list(s)
            # print(m)
            with open('abs_jieba.txt', 'a+', encoding="utf8")as f:
                for word in m:
                    f.write(word)


def lda():
    train = []
    stopwords = codecs.open('stopword.txt', 'r', encoding='utf8').readlines()
    stopwords = [w.strip() for w in stopwords]
    fp = codecs.open('abs_jieba.txt', 'r', encoding='utf8')
    for line in fp:
        line = line.split("/")
        train.append([w for w in line if w not in stopwords])

    dictionary = corpora.Dictionary(train)
    corpus = [dictionary.doc2bow(text) for text in train]
    lda = LdaModel(corpus=corpus, id2word=dictionary, num_topics=5)
    # 打印前20个topic的词分布
    for item in lda.print_topics(100):
        print(item)
    # 打印id为1的topic的词分布
    # print(lda.print_topic(14))
    lda.save('../models/abs_lda.model')


def predict_one():
    lda = models.ldamodel.LdaModel.load("../models/abs_lda.model")
    with open('abs.txt', 'r', encoding="utf8") as f:
        lines = f.readlines()
        num = list()
        for line in lines:
            test_doc = list(jieba.cut(line))
            doc_bow = lda.id2word.doc2bow(test_doc)  # 文档转换成bow
            doc_lda = lda[doc_bow]  # 得到新文档的主题分布
            # 输出新文档的主题分布
            # print(doc_lda)
            # for topic in doc_lda:
            #     print("%s\t%f\n" % (lda.print_topic(topic[0]), topic[1]))
            values = list()
            for items in doc_lda:
                values.append(items[1])
            arr_values = np.array(values)
            arr_max = np.argmax(arr_values)
            # print(doc_lda[arr_max],doc_lda[arr_max][0])
            # print(doc_lda[arr_max][0])
            num.append(doc_lda[arr_max][0])
        print(len(num))
        return num


def predict():
    lda = models.ldamodel.LdaModel.load("../models/abs_lda.model")
    for item in lda.print_topics(10):
        print(item)
    db = pymysql.connect("localhost", "root", "1234", "xian")

    # 使用cursor()方法获取操作游标
    cursor = db.cursor()

    """读取sql"""
    # SQL 查询语句
    sql = "SELECT * FROM zl_light"
    try:
        # 执行SQL语句
        cursor.execute(sql)
        # 获取所有记录列表
        results = cursor.fetchall()
        for row in results:
            abs=row[1]
            test_doc = list(jieba.cut(abs))
            doc_bow = lda.id2word.doc2bow(test_doc)  # 文档转换成bow
            doc_lda = lda[doc_bow]  # 得到新文档的主题分布
            values = list()
            for items in doc_lda:
                values.append(items[1])
            arr_values = np.array(values)
            arr_max = np.argmax(arr_values)
            theme=doc_lda[arr_max][0]
            print(theme)
            cursor = db.cursor()
            sql_update="update zl_light set theme = '%d'where abs = '%s'"
            cursor.execute(sql_update % (theme,abs))
        db.commit()
        cursor.close()

    except Exception as e:
        print(e)
        print("Error: unable to fetch data")


    """读取txt"""
    # with open('abs.txt', 'r', encoding="utf8") as f:
    #     lines = f.readlines()
    #     num = list()
    #     for line in lines:
    #         test_doc = list(jieba.cut(line))
    #         doc_bow = lda.id2word.doc2bow(test_doc)  # 文档转换成bow
    #         doc_lda = lda[doc_bow]  # 得到新文档的主题分布
    #         # 输出新文档的主题分布
    #         # print(doc_lda)
    #         # for topic in doc_lda:
    #         #     print("%s\t%f\n" % (lda.print_topic(topic[0]), topic[1]))
    #         values = list()
    #         for items in doc_lda:
    #             values.append(items[1])
    #         arr_values = np.array(values)
    #         arr_max = np.argmax(arr_values)
    #         # print(doc_lda[arr_max],doc_lda[arr_max][0])
    #         # print(doc_lda[arr_max][0])
    #         num.append(doc_lda[arr_max][0])
    #     print(len(num))
    #     return num
        # max=int(values.index(max(values)))+1
        # print(max)
        # num.append(max)



def insert(data):
    print(data)
    db = pymysql.connect("localhost", "root", "1234", "xian")

    # 使用cursor()方法获取操作游标
    cursor = db.cursor()
    sql_update="update zl_bci set theme = '%s'where id = '%d'"
    for i in range(0,len(data)):
        cursor.execute(sql_update % (data[i],i+1))
    # cursor.executemany(sql_update,data)
    db.commit()
    cursor.close()


def main():
    # sdata()
    # cut()
    # lda()
    # data=predict_one()
    data = predict()
    print(Counter(data))
    # insert(data)
    # # predict_one()


if __name__ == '__main__':
    main()
