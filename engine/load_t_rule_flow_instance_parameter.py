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

    def remote_login(self,command):
        commandBeginTime = ctime()
        myclient = ssh.SSHClient()  # 新建一个ssh客户端对象
        myclient.set_missing_host_key_policy(ssh.AutoAddPolicy())  # 设置成默认自动接受密钥
        myclient.connect("10.139.49.79", port=22, username="hadoop", password="dashu0701")  # 连接远程主机
        print command
        stdin, stdout, stderr = myclient.exec_command(command)
        print stdout.read()
        myclient.close()
        print "command begin [%s], end [%s]" % (commandBeginTime, ctime())
    def main(self):
        sqlcreate = '''
            CREATE TABLE if not exists `engine.t_rule_flow_instance_parameter`(
                  `id` bigint,
                  `batchid` bigint,
                  `ruleflowinstanceid` bigint,
                  `parametername` string,
                  `parametervalue` string,
                  `parametertype` string,
                  `createddatetime` timestamp,
                  `lastupdateddatetime` timestamp)
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
        sqldroppardition = "alter table engine.t_rule_flow_instance_parameter drop partition (daykey='%s')" % self.begdate
        H.sqlquery(sqldroppardition)
        sqladdpardition = "alter table engine.t_rule_flow_instance_parameter add partition (daykey='%s')" % self.begdate
        H.sqlquery(sqladdpardition)
        command = "/alidata1/zs/sqoop-1.4.6-cdh5.6.0/bin/sqoop import " \
                  "--connect  jdbc:mysql://rulesplatform.mysql.rds.aliyuncs.com/rules_engine " \
                  "--username hujingxin --password YhPD89lV7Y   " \
                  "--hcatalog-database engine --hcatalog-table t_rule_flow_instance_parameter " \
                  "--hcatalog-storage-stanza 'stored as orc tblproperties (\"orc.compress\"=\"SNAPPY\")' " \
                  "--hive-overwrite --query \"SELECT * FROM t_rule_flow_instance_parameter WHERE BatchId>= CAST(CONCAT(CAST(UNIX_TIMESTAMP('%s 00:00:00') AS CHAR),'0000000') AS SIGNED) AND BatchId < CAST(CONCAT(CAST(UNIX_TIMESTAMP('%s 00:00:00') AS CHAR),'0000000') AS SIGNED) AND  \$CONDITIONS\"  " \
                  "--hcatalog-partition-keys daykey --hcatalog-partition-values \"%s\" " \
                  "--split-by id  --null-string '\\\N' --null-non-string '\\\N'"%(self.begdate, self.endate, self.begdate)
        self.remote_login(command)

if __name__ == '__main__':
    #global parent_abspath
    #parent_abspath = os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir))
    oldStdout = sys.stdout
    endate = datetime.datetime.now().strftime('%Y-%m-%d')
    begdate = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    logfile = os.path.join(os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)), 'log', 'load_t_rule_flow_instance_parameter.log')
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
