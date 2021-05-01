#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
@author:bl
@file:test04.py
@time:2021/04/26
"""

import pandas as pd
import cx_Oracle
import traceback

conn = cx_Oracle.connect('test01/12345678@192.168.122.130:1521/orcl.global', encoding="UTF-8")
cursor = conn.cursor()

# sql = 'SELECT * FROM tj'
# cursor.execute(sql)
# col_name_list1 = [i[0] for i in cursor.description]
sql ='select TABLE_NAME,NUM_ROWS,DATETIME from tj order by datetime'
cursor.execute(sql)
result = cursor.fetchall()
col_name_list = [i[0] for i in cursor.description]
dataframe = pd.DataFrame(result, columns=col_name_list)
# print(dataframe)

df =dataframe.pivot(index ='TABLE_NAME',columns='DATETIME',values='NUM_ROWS')
print(df)


sql = 'select unique datetime from tj order by datetime'
cursor.execute(sql)
col_name_list=[i[0] for i in cursor.fetchall()]
col_name_list.insert(0, 'TABLE_NAME')

sql ='select unique table_name from tj '
cursor.execute(sql)
result = cursor.fetchall()
df = pd.DataFrame(columns=col_name_list)
df['TABLE_NAME'] = dataframe['TABLE_NAME']


# dataframe =pd.DataFrame(result,columns='table_name')
# print(df.groupby(by='TABLE_NAME'))
