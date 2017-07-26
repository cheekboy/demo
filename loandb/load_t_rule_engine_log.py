#! /root/anaconda2/bin python
# coding=utf-8
import time
import datetime
import calendar
import sys
from time import ctime
import os
import ssh

class view:
    def __init__(self, calcDay):
        global parent_abspath
        parent_abspath = os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir))
        date_time = datetime.datetime.strptime(calcDay, '%Y-%m-%d')
        self.firstDayOfMonth = '%s-01' % date_time.strftime('%Y-%m')
        self.lastDayOfMonth = '%s-%s' % (
        date_time.strftime('%Y-%m'), calendar.monthrange(date_time.year, date_time.month)[1])

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
        commandBeginTime = ctime()
        myclient = ssh.SSHClient()  # 新建一个ssh客户端对象
        myclient.set_missing_host_key_policy(ssh.AutoAddPolicy())  # 设置成默认自动接受密钥
        myclient.connect("10.139.49.79", port=22, username="hadoop", password="dashu0701")  # 连接远程主机
        print command
        stdin, stdout, stderr = myclient.exec_command(command)
        print stdout.read()
        myclient.close()
        print "command begin %s and ends %s" % (commandBeginTime, ctime())
    def main(self):
        sqlcreate = '''
            CREATE TABLE if not EXISTS `loandb.t_rule_engine_log`(
              `id` bigint,
              `userid` bigint,
              `batchid` bigint,
              `stepcode` string,
              `status` tinyint,
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
        sqldroppardition = "alter table loandb.t_rule_engine_log drop IF EXISTS partition (daykey='%s')" % self.firstDayOfMonth
        H.sqlquery(sqldroppardition)
        sqladdpardition = "alter table loandb.t_rule_engine_log add partition (daykey='%s')" % self.firstDayOfMonth
        H.sqlquery(sqladdpardition)
        command = "/alidata1/zs/sqoop-1.4.6-cdh5.6.0/bin/sqoop import " \
                  "--connect  jdbc:mysql://gongfudaird.mysql.rds.aliyuncs.com/loandb?tinyInt1isBit=false " \
                  "--username loandb_rd --password AdUH4FLrhWXCrKxj " \
                  "--hcatalog-database loandb --hcatalog-table t_rule_engine_log " \
                  "--hcatalog-storage-stanza 'stored as orc tblproperties (\"orc.compress\"=\"SNAPPY\")' " \
                  "--query \"select * from t_rule_engine_log where CreateDate >= '%s 00:00:00' and CreateDate <= '%s 23:59:59' and \$CONDITIONS\" --hcatalog-partition-keys daykey --hcatalog-partition-values \"%s\" " \
                  "--hive-overwrite --split-by id  --null-string '\\\N' --null-non-string '\\\N'" % (self.firstDayOfMonth, self.lastDayOfMonth, self.firstDayOfMonth)
        self.remote_login(command)


if __name__ == '__main__':
    #global parent_abspath
    #parent_abspath = os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir))
    oldStdout = sys.stdout
    calcDay = datetime.datetime.now().strftime('%Y-%m-%d')
    logfile = os.path.join(os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)), 'log', 'load_t_rule_engine_log.log')
    f=open(logfile,"a+")
    sys.stdout =f
    print '-------------------------------------------'
    print "programe begins %s" % ctime()
    data = view(calcDay)
    data.main()
    print "calcDay: %s" % calcDay
    print "programe ends %s" % ctime()
    print '-------------------------------------------'
    f.close()
    sys.stdout =oldStdout
