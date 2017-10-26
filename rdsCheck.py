import sys
import threading
import mysql.connector
import concurrent.futures
from time import sleep

def checkDates(datetype, db):
  cnx = mysql.connector.connect(user='pbx', password='********',
                              host='rds-cdr.coredial.com',
                              database=db)
  query = "SELECT "+datetype+"(calldate) FROM cdr"
  cursor = cnx.cursor()
  cursor.execute(query)
  cnx.close()
  return True

def checkCDR(db):
  cnx = mysql.connector.connect(user='pbx', password='************',
                              host='rds-cdr.coredial.com',
                              database=db)
  checkDates('MIN', db)
  checkDates('MAX', db)
  query = "show table status"
  cursor = cnx.cursor()
  cursor.execute(query)
  result = cursor.fetchall()
  cnx.close()
  if result[0][17]:
    print "%s" % (db)
  return result[0][17]

def main(argv):
  cnx = mysql.connector.connect(user='pbx', password='*********',
                              host='rds-cdr.coredial.com',
                              database='information_schema')
  query = "SELECT SCHEMA_NAME FROM SCHEMATA WHERE SCHEMA_NAME LIKE 'cdr%'";
  cursor = cnx.cursor()
  threads = []
  cursor.execute(query)
  results = cursor.fetchall()
  cnx.close()
  cdrs = []
  for row in results:
    db = row[0]
    cdrs.append(db)

  with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
    # Start the load operations and mark each future with its URL
    status = {executor.submit(checkCDR, cdr): cdr for cdr in cdrs}
    for future in concurrent.futures.as_completed(status):
        cdrstatus = status[future]
        try:
            data = future.result()
        except Exception as exc:
            print('%r generated an exception: %s' % (cdrstatus, exc))
        else:
            print('%s' % (cdrstatus))
    

if __name__ == "__main__":
  main(sys.argv[1:])
