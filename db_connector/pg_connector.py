import psycopg2

def storeManyData(DBinfo, AditionalData, TableName, Data):
  reports = Data.get('reports')[0]


  #Form Query
  columnHeader = reports.get('columnHeader')
  header = []
  header.extend(AditionalData.keys())
  header.extend([i[3:] for i in columnHeader.get('dimensions')])
  header.extend([i.get('name') for i in columnHeader.get('metricHeader').get('metricHeaderEntries')])
  Query = 'insert into ' + TableName + '(' + (', ').join(header) + ') values (' + (', ').join(['%s' for i in header]) + ')'

  data = reports.get('data')
  #Form Bulk Data
  DATA = []
  if data.get('rows') == None:
    return

  for i in data.get('rows'):
    d = []
    d.extend(AditionalData.values())
    d.extend(i.get('dimensions'))
    d.extend(i.get('metrics')[0].get('values'))
    DATA.append(d)


  conn = psycopg2.connect(host=DBinfo.hostname, user=DBinfo.username, password=DBinfo.password, dbname=DBinfo.database)
  try:
    cur = conn.cursor()
    cur.executemany(Query, DATA)
    conn.commit()
  except psycopg2.DatabaseError as e:
    if conn:
      conn.rollback()
      print("Error %s" %e)
      exit()
  finally:
    if conn:
      cur.close()
      conn.close()

def getData(DBinfo, Query):
  conn = psycopg2.connect(host=DBinfo.hostname, user=DBinfo.username, password=DBinfo.password, dbname=DBinfo.database)
  try:
    cur = conn.cursor()
    cur.execute(Query)
    return cur.fetchall()
  except psycopg2.DatabaseError as e:
    if conn:
      conn.rollback()
      print("Error %s" %e)
      exit()
  finally:
    if conn:
      cur.close()
      conn.close()

def executeQuery(DBinfo, Query):
  conn = psycopg2.connect(host=DBinfo.hostname, user=DBinfo.username, password=DBinfo.password, dbname=DBinfo.database)
  try:
    cur = conn.cursor()
    cur.execute(Query)
    conn.commit()
  except psycopg2.DatabaseError as e:
    if conn:
      conn.rollback()
      print("Error %s" %e)
      exit()
  finally:
    if conn:
      cur.close()
      conn.close()

def createTable(DBinfo, TableName):
  conn = psycopg2.connect(host=DBinfo.hostname, user=DBinfo.username, password=DBinfo.password, dbname=DBinfo.database)
  try:
    cur = conn.cursor()
    cur.execute(open("./query/table/" + TableName + ".sql", "r").read())
    conn.commit()
  except psycopg2.DatabaseError as e:
    if conn:
      conn.rollback()
      print("Error %s" %e)
      exit()
  finally:
    if conn:
      cur.close()
      conn.close()

