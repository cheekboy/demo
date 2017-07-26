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
        global parent_abspath
        parent_abspath = os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir))
        self.begdate = begdate
        self.endate = endate

    def importmodule(self):
        module_path = os.path.join(parent_abspath, 'module')
        sys.path.append(module_path)
    def dbconnect2(self):
        self.importmodule()
        import db
        import hive_db
        global X1
        global X2
        global X3
        global X4
        global Y
        X3 = hive_db.DB_H('10.139.49.79', 10000, "PLAIN", 'hadoop', 'dashu0701', 'loandb')
        X3.connect()
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
        sqlcreate='''

CREATE TABLE if not exists `loandb.t_point_channel`(
  `id` bigint,
  `stepid` string,
  `userid` int,
  `provincename` string,
  `cityname` string,
  `positiondata` string,
  `positionx` double,
  `positiony` double,
  `channelsource` string,
  `platformid` string,
  `appversion` string,
  `phonebrand` string,
  `phonemodel` string,
  `operatorname` string,
  `phoneversion` string,
  `netmodel` string,
  `ipaddress` string,
  `ipposition` string,
  `idfa` string,
  `openudid` string,
  `imei` string,
  `macaddress` string,
  `bsdevicekey` string,
  `createdate` bigint,
  `lastupdatedate` bigint,
  `comment` string,
  `operatorcode` string,
  `cpuabi` string,
  `isemulator` int,
  `isjailbreak` tinyint,
  `imsi` string)
partitioned BY (DAYKEY STRING)
ROW FORMAT SERDE
              'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
            STORED AS INPUTFORMAT
              'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
            OUTPUTFORMAT
              'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
        '''
        self.dbconnect2()
        X3.sqlquery(sqlcreate)
        sqldroppardition = "alter table loandb.t_point_channel drop IF EXISTS partition (daykey='%s')" % self.begdate
        X3.sqlquery(sqldroppardition)
        sqladdpardition = "alter table loandb.t_point_channel add partition (daykey='%s')" % self.begdate
        X3.sqlquery(sqladdpardition)
        command = "/alidata1/zs/sqoop-1.4.6-cdh5.6.0/bin/sqoop import " \
                  "--connect  jdbc:mysql://gongfudaird.mysql.rds.aliyuncs.com/loandb?tinyInt1isBit=false " \
                  "--username loandb_rd --password AdUH4FLrhWXCrKxj " \
                  "--hcatalog-database loandb --hcatalog-table t_point_channel " \
                  "--hcatalog-storage-stanza 'stored as orc tblproperties (\"orc.compress\"=\"SNAPPY\")' " \
                  "--table t_point_channel " \
                  "--where \"lastupdatedate>='%s' and lastupdatedate<'%s'\" " \
                  "--hcatalog-partition-keys daykey --hcatalog-partition-values \"%s\" " \
                  "--hive-overwrite --split-by id  --null-string '\\\N' --null-non-string '\\\N'" % (
                  self.begdate, self.endate, self.begdate)
        self.remote_login(command)
if __name__ == '__main__':
    oldStdout = sys.stdout
    endate = datetime.datetime.now().strftime('%Y-%m-%d')
    begdate = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    #logfile = os.path.join(parent_abspath, 'log', 'load_t_point_channel.log')
    #f=open(logfile,"a+")
    #sys.stdout =f
    print '-------------------------------------------'
    print "programe begins %s" % ctime()
    data = view(begdate, endate)
    data.main()
    print data.begdate, data.endate
    print "programe ends %s" % ctime()
    print '-------------------------------------------'
    #f.close()
    sys.stdout =oldStdout
