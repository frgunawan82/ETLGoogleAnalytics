import sys, time, json, os
sys.path.append('.')
from datetime import datetime, timedelta, date
from db_connector import pg_connector
from google.google_analytics import ga_model, ga_engine

config = json.load(open(os.path.relpath(__file__).replace('.py','_config.json')))

class dbinfo:
  hostname = config['Database']['host']
  username = config['Database']['user']
  password = config['Database']['pwd']
  database = config['Database']['database']
DB = dbinfo()

class week:
  def getWeek(self, date):
    firstday = datetime(date.year, 1, 1) + timedelta(days=+ 5 - datetime(date.year, 1, 1).weekday())
    week_index = 1
    while (firstday + timedelta(days=+week_index*7)).date() < date:
      week_index += 1
    return week_index

  def nextWeek(self):
    #print(self.start_date + timedelta(days=+7))
    self.__init__(ddate = self.start_date + timedelta(days=+7))

  def setYearWeek(self, yearweek):
    while self.yearweek != yearweek:
      self.nextWeek()

  def __init__(self, ddate):
    #This needed to convert all date format
    ddate = date(ddate.year, ddate.month, ddate.day)
    self.start_date = ddate - timedelta(days=+ (ddate.weekday()+1)%7) #Sunday
    self.end_date = self.start_date + timedelta(days=+ 6) #Saturday
    self.year = self.end_date.year
    self.week = self.getWeek(ddate)
    self.yearweek = int(str(self.year)+ str(self.week).zfill(2))
    #print(ddate, " - ", self.start_date, " - ", self.end_date, " - ", self.yearweek)



end_week = week(datetime.now())

if __name__ == '__main__':
#    query = open('./query/table/bbmdiscover_calculatable.sql','r+').read()
#    pg_connector.executeQuery(DB, str(query))
  config_files = []
  for (dirpath, dirnames, filenames) in os.walk(os.path.relpath(__file__).replace('.py', '_config/')):
    for filename in filenames:
      if filename[-5:] == '.json':
        config_files.append(dirpath + filename)

  for config_file in config_files:
    cfg = json.load(open(config_file), strict=False)
    ids = cfg["view_ids"]
    TableName = cfg["table_name"]
    metrics = cfg["metrics"]
    dimensions = cfg["dimensions"]
    table_query = cfg["table_query"]
    
    pg_connector.executeQuery(DB, table_query)

    for view in ids:
      backfill_week = week(datetime.strptime(cfg["backfill_date"], '%Y-%m-%d').date())

      #Get Last Week on Database
      lastyearweek = pg_connector.getData(DB, 
                    "select coalesce(max(yearweek),'" + 
                    str(week(datetime.strptime(cfg["backfill_date"],'%Y-%m-%d')).yearweek) +
                    "') from " + TableName + " where view_id='" +
                    view['view_id'] + "'" )[0][0]
      backfill_week.setYearWeek(lastyearweek)

      #Delete Last Data so we could update and insert it from there
      pg_connector.executeQuery(DB, 
             "delete from " + TableName + " where view_id='" + str(view['view_id']) + 
             "' and yearweek='" + str(backfill_week.yearweek) + "'")

      #Start Filling Data
      print("filling " + TableName + " with service " + view["view_name"] + " data . . .")

      while backfill_week.yearweek < end_week.yearweek:
        print("filling yearweek:" + str(backfill_week.yearweek))
        v = ga_model.View_Template()
        v.ids = view['view_id']
        v.start_date = str(backfill_week.start_date)
        v.end_date = str(backfill_week.end_date)
        v.metrics = metrics
        v.dimensions = dimensions
        v.orderBys = [{'fieldName': 'ga:yearweek', 'orderType': 1, 'sortOrder': 1}]
        BulkDATA = ga_engine.dumpData(v)
        for DATA in BulkDATA:
          pg_connector.storeManyData(DB, view, TableName, DATA)
        time.sleep(1)
        backfill_week.nextWeek()

      print('filling service ' + view['view_name'] + " data is completed!")
