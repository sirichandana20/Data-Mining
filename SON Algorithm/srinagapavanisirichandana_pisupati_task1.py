from pyspark import SparkContext, StorageLevel, AccumulatorParam
import itertools
import sys


def add_dicts(d1, d2):
    for k in d1:
        d1[k] = d1[k] + (d2[k] if k in d2 else 0)
    for k in d2:
        if k not in d1:
            d1[k] = d2[k]
    return d1


def main(case_number, support, input_file, output_file):
    sc = SparkContext('local[*]', 'srinagapavanisirichandana_pisupati_task1')
    sc.setLogLevel("OFF")
    counts = {}


    def freq_product(a, b, k):
        # aka join
        res = []
        joined = map(lambda x: tuple(sorted(x)), itertools.combinations(dict.fromkeys(itertools.chain.from_iterable(a)), k))
        for r in joined:
             if all_pairs_frequent(r, k):
                 res += [r]
        return res


    def all_pairs_frequent(r, i):
        # tell which pairs to prune
        if i < 3:
            return True
        for x in itertools.combinations(r, i - 1):
            if filter_combinations(x):
                return False
        return True


    def filter_combinations(item_combination):
        cs = counts#.value
        if item_combination in cs:
            return cs[item_combination] < support
        else:
            return True


    textRDD = sc.textFile(input_file)

    if case_number == 1:
        baskets_items = textRDD.filter(lambda l: l != "user_id,business_id").map(
            lambda l: l.split(',')).map(lambda x: (x[0], x[1])).groupByKey().map(
            lambda x: dict.fromkeys(x[1])).persist(StorageLevel(True, True, False, False, 1))
    else:
        baskets_items = textRDD.filter(lambda l: l != "user_id,business_id").map(
            lambda l: l.split(',')).map(lambda x: (x[1], x[0])).groupByKey().map(
            lambda x: dict.fromkeys(x[1])).persist(StorageLevel(True, True, False, False, 1))

    basket_items_with_sizes = baskets_items.flatMap(lambda x: x).map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
    C = {1: sorted(basket_items_with_sizes.map(lambda x: (x[0],)).toLocalIterator())}
    freq_set = sorted(basket_items_with_sizes.filter(lambda x: x[1] >= support).map(lambda x: (x[0],)).toLocalIterator())
    L = {1: freq_set}
    k = 1


    def cmap(item_set, cs):
        cnts = {}
        for c in cs:
            cnts[c] = 0
            if all(elem in item_set for elem in c):
                cnts[c] += 1
        return cnts

    while len(L[k]) > 1:
        k += 1
        C[k] = freq_product(L[k - 1], L[k - 1], k)
        counts.update(baskets_items.map(lambda itemset: cmap(itemset, C[k])).fold({},add_dicts))
        L[k] = list(itertools.filterfalse(filter_combinations, C[k]))

    fp = open(output_file, "w")
    fp.write("Candidates:\n")
    for c in C:
        if C[c]:
            if (c == 1):
                print(*["('"+elem[0]+"')" for elem in C[c]], sep=",", file=fp)
            else:
                print(*(sorted(C[c])), sep=",", file=fp)

    fp.write("\nFrequent Itemsets:\n")
    for l in L:
        if L[l]:
            if (l == 1):
                print(*["('"+elem[0]+"')" for elem in L[l]], sep=",", file=fp)
            else:
                print(*(sorted(L[l])), sep=",", file=fp)

    fp.close()


if __name__ == "__main__":
    argc = len(sys.argv)
    if argc != 5:
        print("Usage: program case_number support <input_file_name> <output_file_name>")

    main(int(sys.argv[1]), int(sys.argv[2]), sys.argv[3], sys.argv[4])
