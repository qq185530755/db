import pymysql
import pandas as pd

class dbmanage(object):

    def __init__(self, db, host='127.0.0.1', port=3306, user='root', passwd='', charset="utf8"):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.db = db
        self.charset = charset
        self.conn = None
        self.cur = None

    def connect_db(self):
        self.conn = pymysql.connect(host=self.host, port=self.port, user=self.user, passwd=self.passwd,
                                    db=self.db, charset=self.charset)
        self.cur = self.conn.cursor()

    def close(self):
        if self.conn and self.cur:
            self.cur.close()
            self.conn.close()

    def excute_cmd(self, cmd):
        self.connect_db()
        self.cur.execute(cmd)
        res = self.cur.fetchall()
        self.conn.commit()
        self.close()
        return res

    def creat_table(self, tablename, attrdict):
        """创建数据库表

            args：
                tablename  ：表名字
                attrdict   ：属性键值对,{'id':'varchar(200) primary key'...} 主键需要直接在类型后面标明
        """

        sql_mid = ''
        comma = ''
        # comma 的意义在于， 当存在n个字段， 字符串拼接时就只需要n-1个逗号
        for attr,value in attrdict.items():
            sql_mid = sql_mid + comma + attr + ' ' + value
            comma = ','
        sql = 'CREATE TABLE %s (' % tablename
        sql = sql + sql_mid
        sql = sql + ') DEFAULT CHARSET=utf8'
        print('creatTable:'+sql)
        self.excute_cmd(sql)

    def insert(self, tablename, params):
        """插入数据库

            args：
                tablename  ：表名字
                key        ：属性键
                value      ：属性值
        """
        key = []
        value = []
        for tmpkey, tmpvalue in params.items():
            key.append(tmpkey)
            if isinstance(tmpvalue, str):
                value.append('\"' + tmpvalue + '\"')
            else:
                value.append(str(tmpvalue))
        attrs_sql = '(' + ','.join(key) + ')'
        values_sql = ' values(' + ','.join(value) + ')'
        sql = 'insert into %s' % tablename
        sql = sql + attrs_sql + values_sql
        print('_insert:' + sql)
        self.excute_cmd(sql)

    def delete(self, tablename, cond_dict):
        """删除数据

            args：
                tablename  ：表名字
                cond_dict  ：删除条件字典

            example：
                params = {"name" : "caixinglong", "age" : "38"}
                mydb.delete(table, params)

        """
        consql = ' '
        if isinstance(cond_dict, dict):
            for k, v in cond_dict.items():
                if isinstance(v, str):
                    v = '"' + v + '"'
                consql = consql + k + '=' + str(v) + ' and '
        consql = consql + ' 1=1 '
        sql = 'DELETE FROM %s where%s' % (tablename, consql)
        print (sql)
        self.excute_cmd(sql)

    def update(self, tablename, attrs_dict, cond_dict):
        """更新数据

            args：
                tablename  ：表名字
                attrs_dict  ：更新属性键值对字典
                cond_dict  ：更新条件字典

            example：
                params = {"name" : "caixinglong", "age" : "38"}
                cond_dict = {"name" : "liuqiao", "age" : "18"}
                mydb.update(table, params, cond_dict)

        """
        attrs_list = []
        consql = ' '
        for tmpkey, tmpvalue in attrs_dict.items():
            if isinstance(tmpvalue, str):
                tmpvalue = '"' + tmpvalue + '"'
            attrs_list.append(tmpkey + '=' + str(tmpvalue))
        attrs_sql = ','.join(attrs_list)

        if isinstance(cond_dict, dict):
            for k, v in cond_dict.items():
                if isinstance(v, str):
                    v = '"' + v + '"'
                consql = consql + k + '=' + str(v) + ' and '
        consql = consql + ' 1=1 '
        sql = 'update %s set %s where%s' % (tablename, attrs_sql, consql)
        print(sql)
        self.excute_cmd(sql)

    def select(self, tablename, cond_dict='', fields='*', order='', pds = True):
        """查询数据

                   args：
                       tablename  ：表名字
                       cond_dict  ：查询条件(字典数据类型) eg: {'dev': '=k30-1'}
                       order      ：排序条件
                   example：
                       print mydb.select(table)
                       print mydb.select(table, fields=["name"])
                       print mydb.select(table, fields=["name", "age"])
        """
        consql = ''
        if isinstance(cond_dict, dict):
            for k, v in cond_dict.items():
                column_tpye = self.excute_cmd('select data_type from information_schema.columns '
                                              'where table_name = "%s" and column_name="%s"' % (tablename, k))[0][0]
                # 先确定要查询的列是什么数据类型，如果是char类型，查询时，需要在传入的条件前后加上""
                if 'char' in column_tpye:
                    consql = ' ' + k + v[0] + '"' + v[1:] + '"' + ' and'
                else:
                    consql = ' ' + k + v + ' and'
        consql = consql + ' 1=1 '  # 由于不清楚具体几个条件，加上1=1 为了防止cond_dict只有一条数据时，加上and会报错

        if isinstance(fields, list):
            fields = ','.join(fields)
        field_sql = 'select %s from %s where ' % (fields, tablename)
        cmd = field_sql + consql + order
        print(cmd)
        if pds:
            self.connect_db()
            res = pd.read_sql(cmd, con=self.conn)
            self.close()
            return res
        else:
            res = self.excute_cmd(cmd)
            return res

    def add_column(self, table_name, column_name, column_type, dft_val='0'):
        if isinstance(dft_val,str):
            dft_val = '"' + dft_val + '"'
        cmd = 'alter table %s add %s %s default %s'%(table_name, column_name, column_type, dft_val)
        print(cmd)
        self.excute_cmd(cmd)


