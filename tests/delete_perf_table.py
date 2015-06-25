import sys
import os

PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
path = os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT))
print(path)
sys.path.append(path)

from ddb.helper import DDBConn, DDBItem
# ddbconn = DDBConn(host="http://localhost:8011")
ddbconn == DDBConn()

tables = ddbconn.list_tables()
print(tables)

if "DynamoPerf2" in tables:
    ddbconn.delete_table(TableName="DynamoPerf2")
    print("Deleting table '" + "DynamoPerf2" + "'")