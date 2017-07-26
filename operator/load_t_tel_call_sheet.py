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
        global parent_abspath
        parent_abspath = os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir))
        self.begdate = begdate
        self.endate = endate

    def importmodule(self):
        module_path = os.path.join(parent_abspath, 'module')
        sys.path.append(module_path)
    def dbconnect2(self):
        self.importmodule()
        import hive_db
        global H
        H = hive_db.DB_H('10.139.49.79', 10000, "PLAIN", 'hadoop', 'dashu0701', 'engine')
        H.connect()

    def unconnect(self):
        H.unconnect()
    def remote_login(self,command):
        commandBeginTime = ctime()
        myclient = ssh.SSHClient()  # 新建一个ssh客户端对象
        myclient.set_missing_host_key_policy(ssh.AutoAddPolicy())  # 设置成默认自动接受密钥
        myclient.connect("10.139.49.79", port=22, username="hadoop", password="dashu0701")  # 连接远程主机
        print command
        stdin, stdout, stderr = myclient.exec_command(command)
        print stdout.read()
        print stderr.read()
        myclient.close()
        print "command begin [%s], end [%s]" % (commandBeginTime, ctime())
    def main(self):
        sqlcreate='''
            CREATE TABLE if not exists  `operator.t_tel_call_sheet`(
              `id` bigint,
              `userid` bigint,
              `telbaseinfoid` bigint,
              `telnum` string,
              `serialnum` bigint,
              `businesstype` string,
              `callmonth` timestamp,
              `callstarttime` timestamp,
              `callduration` bigint,
              `calltype` tinyint,
              `othertelnum` string,
              `otherfulltelnum` string,
              `calllocation` string,
              `calltypedetail` string,
              `basefee` double,
              `roamingfee` double,
              `landfee` double,
              `otherfee` double,
              `totalfee` double,
              `iseffectivecall` tinyint,
              `isfamiliaritymembercall` tinyint,
              `iscallinghome` tinyint,
              `iscallingusuallocation` tinyint,
              `status` tinyint,
              `othertelstatus` tinyint,
              `createdate` timestamp,
              `lastupdatedate` timestamp)
            partitioned BY (DAYKEY STRING)
            ROW FORMAT SERDE
              'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
            STORED AS INPUTFORMAT
              'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
            OUTPUTFORMAT
              'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
        '''

        self.dbconnect2()
        H.sqlquery(sqlcreate)
        sqldroppardition="alter table operator.t_tel_call_sheet drop partition (daykey='%s')" %self.begdate
        H.sqlquery(sqldroppardition)
        sqladdpardition="alter table operator.t_tel_call_sheet add partition (daykey='%s')" %self.begdate
        H.sqlquery(sqladdpardition)
        commands = []
        for i in range(80):
            command = "/alidata1/zs/sqoop-1.4.6-cdh5.6.0/bin/sqoop import " \
                      "--connect jdbc:mysql://operatorrd.mysql.rds.aliyuncs.com/operator%s " \
                      "--username op_readonly --password ioJW789bJPObuJWF " \
                      "--table t_tel_call_sheet  --hcatalog-database operator --hcatalog-table t_tel_call_sheet " \
                      "--hcatalog-storage-stanza 'stored as orc tblproperties (\"orc.compress\"=\"SNAPPY\")'  " \
                      "--split-by id  --null-string '\\\N' --null-non-string '\\\N' " \
                      "--hive-drop-import-delims --map-column-hive CallMonth=timestamp,CallStartTime=timestamp,CreateDate=timestamp,LastUpdateDate=timestamp --null-string '\\\N' --null-non-string '\\\N' --where \"lastupdatedate>='%s' and lastupdatedate<'%s'\" " \
                      "--hcatalog-partition-keys daykey --hcatalog-partition-values \"%s\"  "% (i+1,self.begdate, self.endate, self.begdate)
            commands.append(command)
        for command in commands:
            self.remote_login(command)


if __name__ == '__main__':
    #global parent_abspath
    #parent_abspath = os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir))
    oldStdout = sys.stdout
    endate = datetime.datetime.now().replace(day=1).strftime('%Y-%m-%d')
    begdate = ((datetime.datetime.now().replace(day=1) - datetime.timedelta(days=1)).replace(day=1)).strftime('%Y-%m-%d')
    logfile = os.path.join(os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)), 'log', 'load_tel_call_sheet.log')
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
