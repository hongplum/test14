#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
@author:bl
@file:test02.py
@time:2021/04/22
"""

import pandas as pd
import cx_Oracle
import os


class getOracle(object):

    def __init__(self, user, pwd, ip, port, sid):
        self.connect = cx_Oracle.connect(user + "/" + pwd + "@" + ip + ":" + port + "/" + sid)
        self.cursor = self.connect.cursor()

    def select(self, sql):
        """查询语句
        :param sql:string
        :return:dataframe
        """
        try:
            self.cursor.execute(sql)
            result = self.cursor.fetchall()
            col_name_list = [i[0] for i in self.cursor.description]
            dataframe = pd.DataFrame(result, columns=col_name_list)

            self.connect.commit()
            return dataframe
        except Exception as e:
            print('数据查询失败!!!', e)
        # finally:
        #     self.disconnect()

    def disconnect(self):
        self.cursor.close()
        self.connect.close()

    def insertDataframe(self, dataframe, tableName):
        """插入Dataframe数据
        :param dataframe:dataframe
        :param tableName:数据库表名
        :return:None
        """
        try:
            os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.utf8'  # 解决编码问题
            dataframe = dataframe.reset_index(drop=True)  # 重置index

            # 1. 拼接dataframe字段数据类型
            dtype_ = []
            for i, j in zip(dataframe.columns, dataframe.dtypes):
                if str(j) in ["float", "float64", "int64"]:
                    dtype_.append(i + ' number(16,4)')
                elif str(j) in ['datetime64[ns]']:
                    dtype_.append(i + ' DATE')
                else:
                    dtype_.append(i + ' varchar2(64)')
            dtype_str = ', '.join(dtype_)

            # 2. 创建表
            sql = 'CREATE TABLE %s (%s)' % (tableName, dtype_str)
            self.create(tableName, sql)

            # 3. 批量插入数据
            sqlvalue = ','.join([':%s' % i for i in range(1, len(dataframe.columns) + 1)])  # 插入语句
            datatuple = [tuple(dataframe.iloc[i].values) for i, r in dataframe.iterrows()]  # 元组列表

            # b. 批量插入
            self.cursor.executemany('insert into %s values(%s)' % (tableName, sqlvalue), datatuple)
            self.connect.commit()
            print('**************** 成功入库:', len(datatuple), '条数据 ****************')
        except Exception as e:
            print(e)
        finally:
            self.disconnect()

    def insert_of_sql(self, sql, list_param):
        """ sql 语句插入
        :param sql:string
        :param list_param:list
        :return:None
        """
        try:
            os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.utf8'  # 解决编码问题
            self.cursor.executemany(sql, list_param)
            self.connect.commit()
            print("插入ok")
        except Exception as e:
            print(e)
        finally:
            self.disconnect()

    def update(self, sql):
        """更新表
        :param sql:
        :return:
        """
        try:
            self.cursor.execute(sql)
            self.connect.commit()
        except Exception as e:
            print(e)
        finally:
            self.disconnect()

    def create(self, tableName, sql):
        """循环创建，删除历史重名表，再进行创建
        :param tableName:string
        :param sql:string
        :return:None
        """
        while 1:
            try:
                self.cursor.execute(sql)
                self.connect.commit()
                print("create ok")
                break
            except:
                self.delete(tableName)

    def delete(self, tableName):
        """删除表
        :param tableName:string
        :return:None
        """
        try:
            self.cursor.execute('DROP TABLE {}'.format(tableName))
            self.connect.commit()
            print("delete ok")
        except Exception as e:
            print(e)
        # finally: # 因为create函数中有while循环，此时不关闭连接
        #     self.disconnect()

if __name__=="__main__":
    OrclObj = getOracle('test01', '12345678', '192.168.122.130', '1521', 'orcl.global')
    result = OrclObj.select('SELECT * FROM Persons order by personid')
    print(result)



