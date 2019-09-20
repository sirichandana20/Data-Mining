from pyspark import SparkContext, StorageLevel, AccumulatorParam
import itertools
import sys


def generate_signature(x, b, r):
    y = []
    for i in range(b):
        t = x[1][i * r:(i + 1) * r]
        y.append((i, tuple(t), x[0]))
    return y


def generate_pairs(x):
    y = []
    l = len(x[1])
    g = sorted(x[1])

    for i in range(l):
        for j in range(i + 1, l):
            y.append(((g[i], g[j]), 1))
    return y

def jaccard_similarity(x, mat, bids):
    a = set(mat[bids[x[0]]][1])
    b = set(mat[bids[x[1]]][1])
    sim = len(a & b)/len(a | b)
    return (x[0], x[1], sim)

def main(input_file, output_file):
    sc = SparkContext('local[*]', 'srinagapavanisirichandana_pisupati_task1')
    sc.setLogLevel("OFF")

    textRDD = sc.textFile(input_file)
    csv_header = textRDD.first()

    raw_data = textRDD.filter(lambda l: l != csv_header).map(lambda l: l.split(','))

    businesses = sorted(raw_data.map(lambda x: (x[1], 1)).reduceByKey(lambda x, y: x).map(lambda x: x[0]).toLocalIterator())

    businesses_ids = {}
    for i, e in enumerate(businesses):
        businesses_ids[e] = i

    users = sorted(raw_data.map(lambda x: (x[0], 1)).reduceByKey(lambda x, y: x).map(lambda x: x[0]).toLocalIterator())

    users_ids = {}
    for i, e in enumerate(users):
        users_ids[e] = i

    broadcasted_uids = sc.broadcast(users_ids)

    business_users_ids = raw_data.map(lambda x: (x[1], [broadcasted_uids.value[x[0]]])) \
                                .reduceByKey(lambda x, y: x + y) \
                                .repartition(1) \
                                .sortBy(lambda x: x[0])

    business_users_matrix = business_users_ids.collect()

    bins = len(users_ids)

    def min_hash(constant, x):
        # (((ax + b) % p) % m)
        a = constant[0]
        b = constant[1]
        p = constant[2]
        return min([((a * y + b) % p) % bins for y in x])

    constants = [(913, 901, 24593), (14, 23, 769), (1, 101, 193), (17, 91, 1543),
              (387, 552, 98317), (11, 37, 3079), (2, 63, 97), (41, 67, 6151),
              (91, 29, 12289), (3, 79, 53), (73, 803, 49157), (8, 119, 389)]

    n = len(constants)
    b = 7
    r = int(n / b)

    signatures = business_users_ids.map(lambda x: (x[0], [min_hash(constant, x[1]) for constant in constants]))

    ### LSH starts here

    sigs = signatures.flatMap(lambda x: generate_signature(x, b, r)).reduceByKey(lambda x, y: x + y)

    pairs = sigs.filter(lambda r: len(r[1]) > 1).flatMap(generate_pairs)\
        .reduceByKey(lambda x, y: x).map(lambda x: x[0]).collect()

    # similarities = pairs.map(lambda x: jaccard_similarity(x, business_users_matrix, businesses_ids))
    #
    # limit = 0.5

    # similarities_threshold = similarities.filter(lambda x: x[2] >= limit) \
    #                             .sortBy(lambda x: x[1]).sortBy(lambda x: x[0]).collect()
    #
    # fp = open(output_file, "w")
    # fp.write("business_id_1, business_id_2, similarity")
    # for s in similarities_threshold:
    #     fp.write(s[0] + ','+ s[1] + ',' + str(s[2] + '\n'))
    # fp.close()


if __name__ == "__main__":
    argc = len(sys.argv)
    if argc != 3:
        print("Usage: program <input_file_name> <output_file_name>")

    main(sys.argv[1], sys.argv[2])
