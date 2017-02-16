#!/usr/bin/env python
# encoding: utf-8
from fabric.api import *
env.roledefs = {
	    'channeltest' : ['root@172.16.2.34'],#0
            'testserver': ['root@10.253.41.65',], #1 
            'collection': ['root@10.253.42.120',],#2
	    'clearing':   ['root@10.139.48.28',],#3
	    'audit':      ['root@10.139.48.216',],#4
	    'appview-test':['root@10.253.40.40','root@10.139.48.158'],#5
	    'appview':    ['root@10.253.40.40','root@10.139.48.158'],#6
	    'yfbh5' : ['root@10.253.40.170'],#7
	    'gongfubao-sina': ['root@10.139.49.82','root@10.253.41.212'],#8
	    'sails-ds-risk-management.tgz':['root@10.139.36.76'],#9 自研风控前端预发布
	    'channel' : ['root@10.139.49.155'],#10
            'gongfudai':['root@10.253.43.67'],#11
            'bankbill-search' :['root@10.139.49.236'],#12
            'shengchangongfudai' :['root@10.139.48.31','root@10.253.44.218'],#13		
            'shengchan-sails-ds':['root@10.253.40.76'],#14 自研风控前端生产
	    'mall':['root@10.253.43.67'],#15
 	    'shengchanmall' :['root@10.139.48.31','root@10.253.44.218'],#16
            'sails-mall':['root@10.253.43.67'],#17
	    'sails-mall-shengchan':['root@10.139.48.31','root@10.253.44.218'],#18
            'sails-operating-web':['10.139.49.187'], #19
            'shengchane-sails-operating-web':['10.139.49.187'], #20
            'webrobotceshi':['Administrator@121.43.168.242'],#21测试webrobot
            'webrobotyfb':['Administrator@10.139.35.38'],#22预发布webrobot
            'webrobotshengchan':['Administrator@10.253.46.103','Administrator@10.139.52.158','Administrator@10.139.56.191','Administrator@10.253.44.5'],#23生产webrobot
            }
#env.password = '这里不要用这种配置了，不可能要求密码都一致的，明文编写也不合适。打通所有ssh就行了'
@roles('channeltest')
def task0(apply):
    run('ls')
    #run('cd /dashu/application ;yes|mv channel.zip bak;wget http://gongfu-test.vpc100-oss-cn-hangzhou.aliyuncs.com/%s --referer=http://www.dashu.ds;yes|unzip %s -d channel;cd channel;sh start.sh' % (apply,apply))
@roles('testserver')
def task1(apply):
    run('env  ')
    #run('echo "更新datacenter_alarm_war.war"')
    #run('cd /dashu/application;yes|mv data-center-alarm-*.war  bak/;wget http://gongfu-test.oss-cn-hangzhou-internal.aliyuncs.com/%s --referer=http://www.dashu.ds; yes|unzip %s -d data-center-alarm;/etc/init.d/tomcat2 stop;sleep 10;/etc/init.d/tomcat2 start' % (apply, apply))
@roles('collection')
def task2(apply):
    run('cd /dashu/application ;wget http://gongfu-test.oss-cn-hangzhou-internal.aliyuncs.com/%s --referer=http://www.dashu.ds;yes|unzip %s -d collection' % (apply,apply))
@roles('clearing')
def task3(apply):
    run('cd /dashu/application ;wget http://gongfu-test.oss-cn-hangzhou-internal.aliyuncs.com/%s --referer=http://www.dashu.ds;yes|unzip %s -d  clearing-web' % (apply, apply))
@roles('audit')
def task4(apply):
    run('cd /dashu/application ;wget http://gongfu-test.oss-cn-hangzhou-internal.aliyuncs.com/%s --referer=http://www.dashu.ds;yes|unzip %s -d audit/loan-audit-manage' % (apply, apply))
@roles('appview-test')
def task5(apply):
    run('cd /dashu/application ;yes|mv appview.zip appview_bak;wget http://gongfu-test.oss-cn-hangzhou-internal.aliyuncs.com/%s --referer=http://www.dashu.ds;yes|unzip %s  -d test ' % (apply,apply))
@roles('appview')
def task6(apply):
    run('cd /dashu/application ;yes|mv appview.zip appview_bak;wget http://gongfu-test.oss-cn-hangzhou-internal.aliyuncs.com/%s --referer=http://www.dashu.ds;yes|unzip %s ' % (apply,apply)) 
@roles('yfbh5')
def task7(apply):
    run('cd /dashu/application ;yes|mv gongfubao-sina.zip bak;wget http://gongfu-test.oss-cn-hangzhou-internal.aliyuncs.com/%s --referer=http://www.dashu.ds;yes|unzip %s -d gongfubao-sina;sh sina-restart.sh' % (apply,apply))
@roles('gongfubao-sina')
def task8(apply):
    run('cd /dashu/application ;yes|mv gongfubao-sina.zip bak;wget http://gongfu-test.oss-cn-hangzhou-internal.aliyuncs.com/%s --referer=http://www.dashu.ds;yes|unzip %s -d gongfubao;sh restart.sh' % (apply,apply))
@roles('sails-ds-risk-management.tgz')
def task9(apply):
    run('cd /dashu/node-application;yes|mv sails-ds-risk-management.zip  bak;wget http://gongfu-test.oss-cn-hangzhou-internal.aliyuncs.com/%s --referer=http://www.dashu.ds;unzip  %s -d sails-ds-risk-management ;cd /sails-ds-risk-management;sh start.sh' % (apply,apply))
@roles('channel')
def task10(apply):
    run('cd /dashu/application ;yes|mv channel.zip bak;wget http://gongfu-test.vpc100-oss-cn-hangzhou.aliyuncs.com/%s --referer=http://www.dashu.ds;yes|unzip %s -d channel;cd channel;sh start.sh' % (apply,apply))
    #run('ls')
@roles('gongfudai')
def task11(apply):
    run('cd /dashu/application ;yes|mv gongfudai-h5.zip gongfudai bak;wget http://gongfu-test.oss-cn-hangzhou-internal.aliyuncs.com/%s --referer=http://www.dashu.ds;yes|unzip %s -d gongfudai;sh restart.sh' % (apply,apply))
@roles('bankbill-search')
def task12(apply):
    run('cd /dashu/application ;yes|mv bankbill-search-web.zip  bak;wget http://gongfu-test.oss-cn-hangzhou-internal.aliyuncs.com/%s --referer=http://www.dashu.ds;rm -rf bankbill-search-web ;yes|unzip %s ;cd bankbill-search-web;sh start.sh' % (apply,apply))
@roles('shengchangongfudai')
def task13(apply):
    run('cd /dashu/application ;yes|mv gongfudai-h5.zip gongfudai bak/;wget http://gongfu-test.oss-cn-hangzhou-internal.aliyuncs.com/%s --referer=http://www.dashu.ds;yes|unzip %s -d gongfudai;sh restart.sh; sleep 10 ' % (apply,apply))
@roles('shengchan-sails-ds')
def task14(apply):
    run('cd /dashu/application/node-application/;yes|mv sails-ds-risk-management.zip bak;wget http://gongfu-test.oss-cn-hangzhou-internal.aliyuncs.com/%s --referer=http://www.dashu.ds;yes| unzip %s -d sails-ds-risk-management ;cd sails-ds-risk-management;sh start.sh' % (apply,apply))
@roles('mall')
def task15(apply):
 run('cd /dashu/application ;yes|mv mall.zip bak;wget http://gongfu-test.oss-cn-hangzhou-internal.aliyuncs.com/%s --referer=http://www.dashu.ds; yes|unzip %s -d mall;cd mall;sh start.sh' % (apply,apply))
@roles('shengchanmall')
def task16(apply):
 run('cd /dashu/application ;yes|mv mall.zip bak;wget http://gongfu-test.oss-cn-hangzhou-internal.aliyuncs.com/%s --referer=http://www.dashu.ds; yes|unzip %s -d mall;cd mall;sh start.sh' % (apply,apply))
@roles('sails-mall')
def task17(apply):
 run('cd /dashu/application ;yes|mv sails-mall.zip bak;wget http://gongfu-test.oss-cn-hangzhou-internal.aliyuncs.com/%s --referer=http://www.dashu.ds;yes|unzip %s -d sails-mall;cd sails-mall;sh start.sh' % (apply,apply))
@roles('sails-mall-shengchan')
def task18(apply):
 run('cd /dashu/application ;yes|mv sails-mall.zip bak;wget http://gongfu-test.oss-cn-hangzhou-internal.aliyuncs.com/%s --referer=http://www.dashu.ds;yes|unzip %s -d sails-mall;cd sails-mall;sh start.sh' % (apply,apply))
@roles('sails-operating-web')
def task19(apply):
 run('cd /dashu/application ;yes|mv sails-operating-web.zip bak;wget http://gongfu-test.oss-cn-hangzhou-internal.aliyuncs.com/%s --referer=http://www.dashu.ds;yes|unzip %s -d sails-operating-web;cd sails-operating-web;sh start.sh' % (apply,apply))
@roles('shengchan-sails-operating-web')
def task20(apply):
 run('cd /dashu/application ;yes|mv sails-operating-web.zip bak;wget http://gongfu-test.oss-cn-hangzhou-internal.aliyuncs.com/%s --referer=http://www.dashu.ds;yes|unzip %s -d sails-operating-web;cd sails-operating-web;sh start.sh' % (apply,apply))
@roles('webrobotceshi')
def task21(apply):
 run('cd /cygdrive/c/Program\ Files\ \(x86\)/GnuWin32/bin/ ;./wget.exe http://gongfu-test.oss-cn-hangzhou-internal.aliyuncs.com/%s --referer=http://www.dashu.ds; mv %s /cygdrive/c/dashu/application/;yes|unzip %s -d sails-operating-web;cd sails-operating-web;sh start.sh' % (apply,apply))
@roles('webrobotyfb')
def task22(apply):
 run('cd /dashu/application ;yes|mv sails-operating-web.zip bak;wget http://gongfu-test.oss-cn-hangzhou-internal.aliyuncs.com/%s --referer=http://www.dashu.ds;yes|unzip %s -d sails-operating-web;cd sails-operating-web;sh start.sh' % (apply,apply))
@roles('webrobotshengchan')
def task23(apply):
 run('cd /dashu/application ;yes|mv sails-operating-web.zip bak;wget http://gongfu-test.oss-cn-hangzhou-internal.aliyuncs.com/%s --referer=http://www.dashu.ds;yes|unzip %s -d sails-operating-web;cd sails-operating-web;sh start.sh' % (apply,apply))
def dotask():
    execute(task0)
    #execute(task1)
    execute(task2)
    execute(task3)
    execute(task4)
    execute(task5)
    execute(task6)
    execute(task7)
    execute(task8)
    execute(task9)
    execute(task10)
    execute(task11)
    execute(task12)
    execute(task13)
    execute(task14)
    execute(task15)
    execute(task16)
    execute(task17) 
    execute(task18)
    execute(task19)
    execute(task20)
    execute(task21)
    execute(task22)
    execute(task23)
