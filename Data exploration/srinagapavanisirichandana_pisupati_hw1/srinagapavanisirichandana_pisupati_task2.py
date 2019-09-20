from pyspark import SparkContext
from pyspark import StorageLevel
import os
import json
import sys
import time

#os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3'
#os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3'


def parse_review(review):
    r = json.loads(review)
    return r["business_id"]


def main(input_file, output_file, n_partition):
    sc = SparkContext('local[*]', 'srinagapavanisirichandana_pisupati_task2')

    textRDD = sc.textFile(input_file).repartition(2)

    reviews = textRDD.map(parse_review)  # .persist(StorageLevel(True, True, False, False, 1))

    common = reviews.map(lambda bid: (bid, 1)).reduceByKey(lambda a, b: a + b)

    top_businesses_default = common.sortBy(lambda x: (-1 * x[1], x[0]))
    default_init = time.time()
    top_businesses_default.take(10)
    default_done = time.time() - default_init

    top_businesses_custom = common.partitionBy(int(n_partition), lambda x: hash(x[0])) \
        .sortBy(lambda x: (-1 * x[1], x[0]))

    custom_init = time.time()
    top_businesses_custom.take(10)
    custom_done = time.time() - custom_init

    default_partitions = top_businesses_default.getNumPartitions()
    default = {
        "n_partition": default_partitions
        , "n_items": top_businesses_default.glom().map(len).collect()
        , "exe": round(default_done, 1)
    }

    custom_partitions = top_businesses_custom.getNumPartitions()
    custom = {
        "n_partition": custom_partitions
        , "n_items": top_businesses_custom.glom().map(len).collect()
        , "exe_time": round(custom_done, 1)
    }

    if default_done < custom_done:
        explanation = 'The default partitioner with ' + \
                     str(default_partitions) + \
                     ' partitions seems to be performing better than our custom partitioner with ' + \
                     n_partition + ' partitions'
    else:
        explanation = 'The custom partitioner with ' + n_partition + ' partitions seems ' + \
                     'to be performing better than the default partition with ' + str(default_partitions) + ' partitions.'

    result = {"default": default, "customized": custom, "explanation": explanation}
    fp = open(output_file, "w")
    fp.write(json.dumps(result))
    fp.close()


if __name__ == "__main__":
    argc = len(sys.argv)
    print(sys.argv)
    if argc != 4:
        print("Usage: program <input_file_name> <output_file_name> n_partition")

    main(sys.argv[1], sys.argv[2], sys.argv[3])
