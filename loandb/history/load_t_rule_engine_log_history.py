#! /root/anaconda2/bin python
# coding=utf-8
import sys
import datetime
import calendar
import os
parent_abspath = os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir))
sys.path.append(parent_abspath)
import load_t_rule_engine_log
reload(load_t_rule_engine_log)
def dateRange(beginDate, endDate):
    dates = []
    dt = datetime.datetime.strptime(beginDate, "%Y-%m-%d")
    date = beginDate[:]
    while date <= endDate:
          dates.append(date)
          dt = add_months(dt, 1)
          date = dt.strftime("%Y-%m-%d")
    return dates
def add_months(dt,months):
    month = dt.month - 1 + months
    year = dt.year + month / 12
    month = month % 12 + 1
    day = min(dt.day,calendar.monthrange(year,month)[1])
    return dt.replace(year=year, month=month, day=day)
#注意：如果初始日期的天大于结束日期的天，不包含结束日期的月份
alldate=dateRange('2017-01-10','2017-05-12')
for begdate in alldate:
    print(begdate)
    x=load_t_rule_engine_log.view(begdate)
    x.main()