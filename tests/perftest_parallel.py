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
import concurrent.futures

# ddbconn = DDBConn(host="http://localhost:8011")
ddbconn = DDBConn()

if len(sys.argv) < 2:
    sys.exit("You must supply the item_number argument")

documents_number = int(sys.argv[1])

job_name = 'Job'

start = datetime.now();

with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
    future_to_index = {}
    for index in range(documents_number):
        date = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        value = random.random()

        item = DDBItem()
        item.objectid = value
        item.value1 = value

        future_result = executor.submit(ddbconn.put_item, TableName="DynamoPerf2", Item=item)
        future_to_index[future_result] = index

    for future in concurrent.futures.as_completed(future_to_index):
            index = future_to_index[future]
            if future.exception() is not None:
                print('%d generated an exception: %s' % (index,
                                                         future.exception()))

print(job_name + ' inserted ' +  str(documents_number) + ' in ' + str((datetime.now() - start).total_seconds()) + 's')
