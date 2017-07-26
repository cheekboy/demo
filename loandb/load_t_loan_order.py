#! /root/anaconda2/bin python
# coding=utf-8
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
        import hive_db
        global H
        H = hive_db.DB_H('10.139.49.79', 10000, "PLAIN", 'hadoop', 'dashu0701', 'loandb')
        H.connect()

    def remote_login(self,command):
        myclient = ssh.SSHClient()  # 新建一个ssh客户端对象
        myclient.set_missing_host_key_policy(ssh.AutoAddPolicy())  # 设置成默认自动接受密钥
        myclient.connect("10.139.49.79", port=22, username="hadoop", password="dashu0701")  # 连接远程主机
        print command
        stdin, stdout, stderr = myclient.exec_command(command)
        print stdout.read()
        myclient.close()
    def main(self):
        self.dbconnect2()
        H.sqlquery("TRUNCATE TABLE loandb.t_loan_order")
        command = "/alidata1/zs/sqoop-1.4.6-cdh5.6.0/bin/sqoop import " \
                  "--connect  jdbc:mysql://gongfudaird.mysql.rds.aliyuncs.com/loandb?tinyInt1isBit=false " \
                  "--username loandb_rd --password AdUH4FLrhWXCrKxj  " \
                  "--table t_loan_order --hcatalog-database loandb --hcatalog-table t_loan_order  " \
                  "--hcatalog-storage-stanza 'stored as orc tblproperties (\"orc.compress\"=\"SNAPPY\")' " \
                  "--hive-drop-import-delims --map-column-hive " \
                  "PlatReceivedAt=timestamp,PlatPublishedAt=timestamp,PlatFullfilledAt=timestamp,PlatTransferedAt=timestamp,CreateDate=timestamp,LastUpdateDate=timestamp,PayAt=timestamp,UserId=int " \
                  "--hive-overwrite --split-by id --null-string '\\\N' --null-non-string '\\\N'"
        self.remote_login(command)


if __name__ == '__main__':
    global parent_abspath
    parent_abspath = os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir))
    oldStdout = sys.stdout
    endate = datetime.datetime.now().strftime('%Y-%m-%d')
    begdate = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    logfile = os.path.join(parent_abspath, 'log', 'load_t_loan_order.log')
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
