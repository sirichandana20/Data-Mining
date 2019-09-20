from pyspark import SparkContext
from pyspark import StorageLevel
import os
import json
import sys

#os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python3'
#os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python3'


def parse_review(review):
    r = json.loads(review)
    return {"user_id": r["user_id"], "business_id": r["business_id"], "date": r["date"]}


def is_year(r, year):
    return r["date"][:4] == year


def main(input_file, output_file):
    sc = SparkContext('local[*]', 'srinagapavanisirichandana_pisupati_task1')

    textRDD = sc.textFile(input_file).repartition(2)

    reviews = textRDD.map(parse_review).persist(StorageLevel(True, True, False, False, 1))

    review_counts_2018 = reviews.filter(lambda r: is_year(r, '2018'))

    distinct_users = reviews.map(lambda r: r["user_id"]).distinct()

    top_users = reviews.map(lambda review: (review["user_id"], 1)).reduceByKey(lambda a, b: a + b) \
        .sortBy(lambda x: (-1 * x[1], x[0]),numPartitions=1).take(10)

    distinct_businesses = reviews.map(lambda r: r["business_id"]).distinct()

    top_businesses = reviews.map(lambda review: (review["business_id"], 1)).reduceByKey(lambda a, b: a + b) \
        .sortBy(lambda x: (-1 * x[1], x[0]),numPartitions=1).take(10)

    result = {"n_review": reviews.count(),
              "n_review_2018": review_counts_2018.count(),
              "n_user": distinct_users.count(),
              "top10_user": top_users,
              "n_business": distinct_businesses.count(),
              "top10_business": top_businesses
              }

    fp = open(output_file, "w")
    fp.write(json.dumps(result))
    fp.close()


if __name__ == "__main__":
    argc = len(sys.argv)
    print(sys.argv)
    if argc != 3:
        print("Usage: program <input_file_name> <output_file_name>")

    main(sys.argv[1], sys.argv[2])
