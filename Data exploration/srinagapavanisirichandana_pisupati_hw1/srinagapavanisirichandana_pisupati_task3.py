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
    return r["business_id"], r["stars"]


def parse_business(business):
    r = json.loads(business)
    return r["business_id"], r["city"]


def main(input_file_review, input_file_business, output_file_a, output_file_b):
    sc = SparkContext('local[*]', 'srinagapavanisirichandana_pisupati_task3')

    review_rdd = sc.textFile(input_file_review).repartition(2)
    business_rdd = sc.textFile(input_file_business).repartition(2)

    parsed_reviews = review_rdd.map(parse_review)
    parsed_businesses = business_rdd.map(parse_business)

    city_stars = parsed_businesses.leftOuterJoin(parsed_reviews).map(lambda x: x[1]) \
        .filter(lambda x: x[1] is not None) \
        .combineByKey(
        lambda v: (v, 1)
        , lambda x, c: (x[0] + c, x[1] + 1)
        , lambda x, y: (x[0] + y[0], x[1] + y[1])) \
        .map(lambda bac: (bac[0], round(bac[1][0] / bac[1][1], 1))) \
        .sortBy(lambda x: (-1 * x[1], x[0].lower()), numPartitions=1) \
        .map(lambda b: b[0] + ',' + str(b[1])).persist(StorageLevel(True, True, False, False, 1))

    fp = open(output_file_a, "w")
    fp.write('city,stars\n')
    ress = city_stars.collect()

    # city_stars.map(lambda b: b[0] + ',' + str(b[1])).saveAsTextFile(output_file_a)
    for r in ress:
        fp.write(r + '\n')
    fp.close()

    collect_init_seconds = time.time()
    print('city,stars')
    collect_stars = (city_stars.collect())[:10]
    for r in collect_stars:
        print(r)
    m1 = time.time() - collect_init_seconds

    take_init_seconds = time.time()
    print('city,stars')
    take_stars = city_stars.take(10)
    for r in take_stars:
        print(r)
    m2 = time.time() - take_init_seconds

    if m1 > m2:
        explanation = "Method 2 i.e. taking the first 10 cities is faster than Method 1"
    else:
        explanation = "Method 1 i.e.collecting all the data and printing the first 10 cities is faster than Method 2"

    result = {"m1": round(m1, 1), "m2": round(m2, 1), "explanation": explanation}
    fp = open(output_file_b, "w")
    fp.write(json.dumps(result))
    fp.close()


if __name__ == "__main__":
    argc = len(sys.argv)
    print(sys.argv)
    if argc != 5:
        print("Usage: program <input_file_name1> <input_file_name2> <output_file_name1> <output_file_name2>")

    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
