import sys, time, json, os
sys.path.append('.')
from datetime import datetime, timedelta
from db_connector import pg_connector
from google.google_analytics import ga_model, ga_engine

config = json.load(open(os.path.relpath(__file__).replace('.py','_config.json')))

class dbinfo:
  hostname = config['Database']['host']
  username = config['Database']['user']
  password = config['Database']['pwd']
  database = config['Database']['database']
DB = dbinfo()

yesterday = (datetime.now() - timedelta(days=+ 1)).date()


if __name__ == '__main__':
  config_files = []
  for (dirpath, dirnames, filenames) in os.walk(os.path.relpath(__file__).replace('.py', '_config/')):
    for filename in filenames:
      if filename[-5:] == '.json':
        config_files.append(dirpath + filename)
  
  for config_file in config_files:
    print("Processing config file: " + config_file)
    cfg = json.load(open(config_file), strict=False)
    ids = cfg["view_ids"]
    TableName = cfg["table_name"]
    metrics = cfg["metrics"]
    dimensions = cfg["dimensions"]
    table_query = cfg["table_query"]
    pg_connector.executeQuery(DB, table_query)
    info = cfg["additional_inf"]

    for view in ids:
      backfill_date = pg_connector.getData(DB, 
                      "select coalesce(max(date),'" + cfg['backfill_date'] + "'::date) from " + 
                      TableName + " where view_id='" + view['view_id'] + "'" )[0][0]
      pg_connector.executeQuery(DB, "delete from " + TableName + " where view_id='" + 
                                str(view['view_id']) + "' and date='" + str(backfill_date) + "'")
      print("filling " + TableName + " with service " + view["view_name"] + " data . . .")
      while backfill_date <= yesterday:
        print("filling date:" + str(backfill_date))
        v = ga_model.View_Template()
        v.ids = view['view_id']
        v.start_date = str(backfill_date)
        v.end_date = str(backfill_date)
        v.metrics = metrics
        v.dimensions = dimensions
        v.orderBys = [{'fieldName': 'ga:date', 'orderType': 1, 'sortOrder': 1}]
        BulkDATA = ga_engine.dumpData(v)
        for key, value in view.items():
          info[key] = value
        
        for DATA in BulkDATA:
          pg_connector.storeManyData(DB, info, TableName, DATA)
        time.sleep(1)
        backfill_date += timedelta(days=+1)

      print('filling service ' + view['view_name'] + " data is completed!")
