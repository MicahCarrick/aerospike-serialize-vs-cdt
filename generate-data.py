import sys
import json
import zlib
from random import random, randint, choice
from string import ascii_uppercase
from datetime import timedelta, date
from time import time
import aerospike

def main():
    # variables to connect to Aerospike
    host = '127.0.0.1'
    port = 3000
    namespaces = {'ns1': "blob", 'ns2': "cdt"}
    set_name = 'example'

    # varaibles to generate example data model
    start_date = date(2019, 1, 1)
    total_days = 2
    txn_per_day = 2
    acct_count = 2
    location_cardinality = 5
    zlib_level = -1

    # connect to Aerospike
    client = aerospike.client({'hosts': [(host, port)]}).connect()

    objects = {}
    for namespace, object_type in namespaces.items():
        for acct in [f'{i:05}' for i in range(1, acct_count + 1)]:
            for day in (start_date + timedelta(n) for n in range(total_days)):
                pk = "monthly:{}:{}".format(day.strftime("%Y%m"), acct)
                if pk not in objects:
                    objects[pk] = {
                        'acct': acct,
                        'loc': randint(1, location_cardinality),
                        'txns': {}
                    }

                map_key = day.strftime("%Y%m%d")
                objects[pk]['txns'][map_key] = []

                for txn in range(1, txn_per_day + 1):
                    ts =randint(0, 86399999)
                    objects[pk]['txns'][map_key].append({
                        'txn': txn,
                        'ts':  ts,
                        'sku': randint(1000, 9999),
                        'cid': ''.join(choice(ascii_uppercase) for i in range(12)),
                        'amt': randint(10, 999999),
                        'qty': randint(1, 99),
                        'code': 'USD'
                    })
        
        # write each record
        print(objects)
        start = time()
        for pk, record in objects.items():
            if object_type == 'cdt':
                record_data = record

            elif object_type == 'blob':
                record_data = {'object': zlib.compress(json.dumps(record).encode("utf-8"), zlib_level)}
        
            key = (namespace, set_name, pk)
            client.put(key, record_data, 
                    policy={'exists': aerospike.POLICY_EXISTS_CREATE_OR_REPLACE}
            )
        elapsed = time() - start

        # get metrics for this namespace
        res = client.info_node("namespace/{}".format(namespace), (host, port)).rstrip()
        metrics = dict((k, v) for k, v in [token.split('=') for token in res.split(';')])

        print("Aerospike:          {}:{} {}.{}".format(host, port, namespace, set_name))
        print("Run time:           {:.3f} seconds".format(elapsed))
        print("Object type:        {}".format(object_type))
        print("Object count:       {}".format(metrics['master_objects']))
        print("Avg object size:    {} KiB".format(round(int(metrics['device_used_bytes']) / int(metrics['master_objects']) / 1024, 1)))
        print("Compression ratio:  {}".format(metrics.get('device_compression_ratio', '-')))
        print("---\n")

if __name__ == "__main__":
    main()
