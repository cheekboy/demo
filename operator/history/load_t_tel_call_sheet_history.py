import sys
import datetime
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))
import load_t_tel_call_sheet
reload(load_t_tel_call_sheet)
def dateRange(beginDate, endDate):
    dates = []
    dt = datetime.datetime.strptime(beginDate, "%Y-%m-%d")
    date = beginDate[:]
    while date <= endDate:
          dates.append(date)
          dt = dt + datetime.timedelta(1)
          date = dt.strftime("%Y-%m-%d")
    return dates

alldate=dateRange('2017-05-01','2017-05-02')
for begdate in alldate:
    endate=(datetime.datetime.strptime(begdate,"%Y-%m-%d")+datetime.timedelta(1)).strftime("%Y-%m-%d")
    print(begdate,endate)
    x=load_t_tel_call_sheet.view(begdate,endate)
    x.main()