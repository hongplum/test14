#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
@author:bl
@file:test01.py
@time:2021/04/22
"""
import cx_Oracle
import traceback

conn = cx_Oracle.connect('test01/12345678@192.168.122.130:1521/orcl.global', encoding="UTF-8")
cursor = conn.cursor()

cursor.execute("SELECT * FROM Persons")
rows = list(cursor.fetchall())  # 得到所有数据集
print(rows)
# sql = 'insert into Persons1 values(:1,:2,:3,:4)'
# try:
#     cursor.executemany(sql, rows)
#     conn.commit()
# except:
#     conn.rollback()
#     traceback.print_exc()



# for row in rows:
#     print("%d, %s, %s, %s" % (row[0], row[1], row[2], row[3]))  # python3以上版本中print()要加括号用了
#
# print("Number of rows returned: %d" % cursor.rowcount)

# sql = "SELECT * FROM Persons"
# cursor.execute(sql)
# # while (True):
#     row = cursor.fetchone()  # 逐行得到数据集
#     if row == None:
#         break
#     print("%d, %s, %s, %s" % (row[0], row[1], row[2], row[3]))
#
# print("Number of rows returned: %d" % cursor.rowcount)


# cursor.execute("INSERT INTO Persons VALUES(5, 'asdfa', 'ewewe', 'sfjgsfg')")
# conn.commit()
