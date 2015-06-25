import sys
import os
import time
import random
import json

PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
path = os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT))
print(path)
sys.path.append(path)

from ddb.helper import DDBConn, DDBItem

from datetime import datetime

# ddbconn = DDBConn(host="http://localhost:8011")
ddbconn = DDBConn()

if len(sys.argv) < 2:
    sys.exit("You must supply the item_number argument")

documents_number = int(sys.argv[1])

job_name = 'Job'

start = datetime.now();

for index in range(documents_number):
    try:
        date = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        value = random.random()

        item = DDBItem()
        item.objectid = value
        item.value1 = value

        ddbconn.put_item(TableName="DynamoPerf2", Item=item)

        #print document
        index += 1;
        if index % 100 == 0:
            print(job_name + ' inserted ' + str(index) + ' documents in ' + str((datetime.now() - start).total_seconds()) + 's' +  ' bytes ' + str(len(json.dumps(item))))
    except:
        print('Unexpected error:' + sys.exc_info()[0] + '  for index ' + str(index))
        raise
print(job_name + ' inserted ' +  str(documents_number) + ' in ' + str((datetime.now() - start).total_seconds()) + 's')
