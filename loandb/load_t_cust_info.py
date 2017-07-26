#! /root/anaconda2/bin python
# coding=gbk
import time
import datetime
import sys
from time import ctime
import os
import ssh

class view:
    def __init__(self, begdate, endate):
        self.begdate = begdate
        self.endate = endate

    def importmodule(self):
        module_path = os.path.join(parent_abspath, 'module')
        sys.path.append(module_path)
    def dbconnect2(self):
        self.importmodule()
        import db
        global X1
        global X2
        global X3
        global X4
        global Y
        X1 = db.DB_M("gongfudaird.mysql.rds.aliyuncs.com", 3306, "readonly", "dashu1234", "loandb")
        X1.connect()
        X2 = db.DB_M("dataplatformrd.mysql.rds.aliyuncs.com", 3306, "readonly", "dashu1234", "basisdata")
        X2.connect()
        Y = db.DB_P('10.253.44.6', 5432, 'postgres', '123456', 'credit')
        Y.connect()

    def remote_login(self,command):
        myclient = ssh.SSHClient()  # 新建一个ssh客户端对象
        myclient.set_missing_host_key_policy(ssh.AutoAddPolicy())  # 设置成默认自动接受密钥
        myclient.connect("10.139.49.79", port=22, username="hadoop", password="dashu0701")  # 连接远程主机
        print command
        stdin, stdout, stderr = myclient.exec_command(command)
        print stdout.read()
        myclient.close()
    def main(self):
        command = "/alidata1/zs/sqoop-1.4.6-cdh5.6.0/bin/sqoop import " \
                  "--connect  jdbc:mysql://gongfudaird.mysql.rds.aliyuncs.com/loandb?tinyInt1isBit=false " \
                  "--username loandb_rd --password AdUH4FLrhWXCrKxj  " \
                  "--table t_cust_info --hive-import --hive-table loandb.t_cust_info  " \
                  "--hive-drop-import-delims --map-column-hive GraduateDate=date,LastApplyTime=timestamp,CreateDate=timestamp,LastUpdateDate=timestamp " \
                  "--null-string '\\\N' --null-non-string '\\\N' --hive-overwrite"
        self.remote_login(command)

if __name__ == '__main__':
    global parent_abspath
    parent_abspath = os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir))
    oldStdout = sys.stdout
    endate = datetime.datetime.now().strftime('%Y-%m-%d')
    begdate = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    logfile = os.path.join(parent_abspath, 'log', 'load_t_cust_info.log')
    f=open(logfile,"a+")
    sys.stdout =f
    print '-------------------------------------------'
    print "programe begins %s" % ctime()
    data = view(begdate, endate)
    data.main()
    print data.begdate, data.endate
    print "programe ends %s" % ctime()
    print '-------------------------------------------'
    f.close()
    sys.stdout =oldStdout
