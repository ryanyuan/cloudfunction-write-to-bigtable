import uuid
import json

from google.cloud import bigtable
from google.cloud.bigtable import column_family

BT_PROJECT = 'gcp-bt-project'
BT_INSTANCE = 'instance'
BT_TABLE = 'transactions'

COLUMNS = [
    'timestamp',
    'device_id',
    'merchant_name',
    'total_amount']


def write_to_bt_http(request):
    req = json.loads(request.form['json'])
    row_key = insert_to_bt(req)
    return ('Inserted row_key `%s` to BigTable successfully' % row_key, 200)


def insert_to_bt(request):
    client = bigtable.Client(project=BT_PROJECT, admin=True)
    instance = client.instance(BT_INSTANCE)
    table = instance.table(BT_TABLE)

    max_versions_rule = column_family.MaxVersionsGCRule(2)
    column_family_id = 'cf1'
    column_families = {column_family_id: max_versions_rule}

    if not table.exists():
        table.create(column_families=column_families)

    rows = []
    row_key = str(uuid.uuid4())
    row_key_encoded = row_key.encode('utf-8')
    row = table.row(row_key_encoded)

    for column in COLUMNS:
        row.set_cell(column_family_id, column.encode(),
                     str(request.get(column)))

    rows.append(row)
    table.mutate_rows(rows)

    return row_key
