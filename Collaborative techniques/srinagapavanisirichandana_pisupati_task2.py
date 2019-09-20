from pyspark import SparkContext, StorageLevel, AccumulatorParam
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
import itertools
import collections
import sys


def stabilize_pred(x):
    if x[2] > 5:
        r = 5
    elif x[2] < 1:
        r = 1
    else:
        r = x[2]
    return (x[0], x[1]), r


def pearson_correlation_coefficient(v, u, key):
    ru = 0
    rv = 0
    rc = 0
    businesses = []
    for business in u[1][key]:
        if business in v[1][key]:
            ru += u[1][key][business]
            rv += v[1][key][business]
            rc += 1
            businesses.append(business)
    if rc == 0:
        ru = rv = 0
    else:
        ru /= rc
        rv /= rc

    numerator = 0
    d1 = 0
    d2 = 0

    for business in businesses:
        uside = (u[1][key][business] - ru)
        vside = (v[1][key][business] - rv)
        numerator += (uside * vside)
        d1 += pow(uside, 2)
        d2 += pow(vside, 2)
    denominator = pow(d1 * d2, 0.5)

    if denominator == 0.0:
        wuv = 0
    else:
        wuv = numerator / denominator

    # return v[0], {'businesses': v[1]['businesses'], 'avg': v[1]['avg'], 'wuv': wuv}
    return wuv


def main(training_file, testing_file, case_id, output_file):
    sc = SparkContext('local[*]', 'srinagapavanisirichandana_pisupati_task2')
    sc.setLogLevel("OFF")

    trainRDD = sc.textFile(training_file)
    testRDD = sc.textFile(testing_file)

    train_csv_header = trainRDD.first()
    test_csv_header = testRDD.first()

    training_data = trainRDD.filter(lambda l: l != train_csv_header).map(lambda l: l.split(','))
    testing_rows = testRDD.filter(lambda l: l != test_csv_header).map(lambda l: l.split(','))

    if case_id == 1 or case_id == 4:

        training_ids = {'users': {}, 'business': {}}
        train_id = 0
        testing_ids = {'users': {}, 'business': {}}
        test_id = 0

        for e in training_data.toLocalIterator():
            if e[0] not in training_ids['users']:
                training_ids['users'][e[0]] = train_id
                train_id += 1
            if e[1] not in training_ids['business']:
                training_ids['business'][e[1]] = train_id
                train_id += 1

        for e in testing_rows.toLocalIterator():
            if e[0] not in testing_ids['users']:
                if e[0] in training_ids['users']:
                    testing_ids['users'][e[0]] = training_ids['users'][e[0]]
                else:
                    testing_ids['users'][e[0]] = test_id
                    test_id += 1
            if e[1] not in testing_ids['business']:
                if e[1] in training_ids['business']:
                    testing_ids['business'][e[1]] = training_ids['business'][e[1]]
                else:
                    testing_ids['business'][e[1]] = test_id
                    test_id += 1

        testing_no_stars = testing_rows.map(lambda r: (testing_ids['users'][r[0]], testing_ids['business'][r[1]]))

        rank = 7
        num_iterations = 10

        ratings = training_data.map(lambda x: Rating(training_ids['users'][x[0]], training_ids['business'][x[1]], x[2]))
        model = ALS.train(ratings, rank, num_iterations)
        predictions = model.predictAll(testing_no_stars).map(stabilize_pred)
        real_and_preds = testing_rows.map(
            lambda r: ((testing_ids['users'][r[0]], testing_ids['business'][r[1]]), (r[0], r[1], float(r[2])))).join(
            predictions)
        mse = real_and_preds.map(lambda r: (r[1][0][2] - r[1][1]) ** 2).mean()
        rmse = pow(mse, 0.5)
        print(rmse)

        fp = open(output_file, "w")

        fp.write("user_id, business_id, prediction\n")
        for s in real_and_preds.collect():
            v = s[1][0]
            fp.write(v[0] + ','+v[1]+','+str(v[2]) + '\n')
        fp.close()


    elif case_id == 2:
        def avg_user_rating(u, business_id):
            count = 0
            n = 0
            for business in u:
                if business != business_id:
                    count += u[business]
                    n += 1
            if n == 0:
                avg = 0
            else:
                avg = count / n
            # return u[0], {'businesses': u[1], 'avg': avg}
            return avg

        def pearson_prediction(u):
            pass

        testing_ids = testing_rows.collect()

        user_business_ids = training_data.map(lambda r: (r[0], {r[1]: float(r[2])})) \
            .reduceByKey(lambda x, y: dict(collections.ChainMap(y, x))) \
            .collectAsMap()
        # .persist(StorageLevel(True, True, False, False, 1))

        business_user_ids = training_data.map(lambda r: (r[1], [r[0]])) \
            .reduceByKey(lambda x, y: x + y) \
            .collectAsMap()

        fp = open(output_file, "w")
        fp.write("user_id, business_id, prediction\n")

        almost_there = {}
        se = 0
        c = 0
        for user_id, business_id, stars in testing_ids:
            # user_avgs = user_business_ids.filter(lambda u: (business_id in u[1]) or (u[0] == user_id))\
            #                 .map(lambda u: avg_user_rating(u, business_id))
            #
            # this_user = (user_avgs.filter(lambda u: u[0] == user_id).take(1))[0]
            # # print(this_user)
            # # user_avgs.foreach(print)
            # almost_there = user_avgs.filter(lambda u: u[0] != user_id)\
            #                     .map(lambda u: pearson_correlation_coefficient(u, this_user))\
            #                     .take(10)
            #                     # .repartition(1).sortBy(lambda u: u[1]['wuv'], ascending=False).take(10)
            y = []
            this_user = (user_id, {'businesses': user_business_ids[user_id],
                                   'avg': avg_user_rating(user_business_ids[user_id], business_id)})
            # for u in user_business_ids:
            #     if movie_id in user_business_ids[u]:

            if business_id not in business_user_ids:
                pred = 0
                fp.write(user_id + ',' + business_id + ',' + str(pred) + '\n')
                almost_there[(user_id, business_id)] = y
                se += pow(float(stars) - pred, 2)
                c += 1
                continue

            for u in business_user_ids[business_id]:
                ag = avg_user_rating(user_business_ids[u], business_id)
                uu = (u, {'businesses': user_business_ids[u]})
                uv = pearson_correlation_coefficient(uu, this_user, 'businesses')
                uwuv = (u, {'businesses': user_business_ids[u], 'avg': ag, 'wuv': uv})
                y.append(uwuv)  # pearson_correlation_coefficient(uu, this_user))

            almost_there[(user_id, business_id)] = (sorted(y, key=lambda u: u[1]['wuv'], reverse=True))[:10]

            # for user_id, business_id, _ in testing_ids:

            numerator = 0
            denominator = 0

            for user in almost_there[(user_id, business_id)]:
                numerator += ((user[1]['businesses'][business_id] - user[1]['avg']) * user[1]['wuv'])
                denominator += abs(user[1]['wuv'])
            if denominator == 0:
                pred = this_user[1]['avg']
            else:
                pred = this_user[1]['avg'] + (numerator / denominator)
            if pred < 0:
                pred = 0
            elif pred > 5:
                pred = 5
            # print(pred)
            se += pow(float(stars) - pred, 2)
            c += 1
            fp.write(user_id + ',' + business_id + ',' + str(pred) + '\n')

        rmse = pow(se / c, 0.5)
        print(rmse)

        fp.close()

    elif case_id == 3:

        def avg_item_rating(users, user_id):
            count = 0
            n = 0
            for user in users:
                if user != user_id:
                    count += users[user]
                    n += 1
            if n == 0:
                avg = 0
            else:
                avg = count / n
            # return u[0], {'users': u[1], 'avg': avg}
            return avg

        testing_ids = testing_rows.collect()

        business_user_ids = training_data.map(lambda r: (r[1], {r[0]: float(r[2])})) \
            .reduceByKey(lambda x, y: dict(collections.ChainMap(y, x))) \
            .collectAsMap()

        user_business_ids = training_data.map(lambda r: (r[0], [r[1]])) \
            .reduceByKey(lambda x, y: x + y) \
            .collectAsMap()

        fp = open(output_file, "w")
        fp.write("user_id, business_id, prediction\n")

        almost_there = {}
        se = 0
        c = 0

        for user_id, business_id, stars in testing_ids:
            bb = {}
            if business_id in business_user_ids:
                bb = business_user_ids[business_id]
                if user_id in bb:
                    pred = stars
                    fp.write(user_id + ',' + business_id + ',' + str(pred) + '\n')
                    almost_there[(user_id, business_id)] = []
                    c += 1
                    continue

            this_business = (business_id, {'users': bb,
                                           'avg': avg_item_rating(bb, user_id)})

            y = []

            if user_id not in user_business_ids:
                pred = 0
                fp.write(user_id + ',' + business_id + ',' + str(pred) + '\n')
                almost_there[(user_id, business_id)] = y
                se += pow(float(stars) - pred, 2)
                c += 1
                continue

            for b in user_business_ids[user_id]:
                ag = avg_item_rating(business_user_ids[b], user_id)
                bb = (b, {'users': business_user_ids[b]})
                uv = pearson_correlation_coefficient(bb, this_business, 'users')
                uwuv = (b, {'users': business_user_ids[b], 'avg': ag, 'wuv': uv})
                y.append(uwuv)

            almost_there[(user_id, business_id)] = (sorted(y, key=lambda u: u[1]['wuv'], reverse=True))[:10]

            numerator = 0
            denominator = 0

            for user in almost_there[(user_id, business_id)]:
                numerator += (user[1]['users'][user_id] * user[1]['wuv'])
                denominator += abs(user[1]['wuv'])
            if denominator == 0:
                pred = this_business[1]['avg']
            else:
                pred = (numerator / denominator)
            if pred < 0:
                pred = 0
            elif pred > 5:
                pred = 5
            # print(pred)
            se += pow(float(stars) - pred, 2)
            c += 1
            fp.write(user_id + ',' + business_id + ',' + str(pred) + '\n')

        rmse = pow(se / c, 0.5)
        print(rmse)

        fp.close()


if __name__ == "__main__":
    argc = len(sys.argv)
    if argc != 5:
        print("Usage: program <training_file_name> <testing_file_name> <case_id> <output_file_name>")

    main(sys.argv[1], sys.argv[2], int(sys.argv[3]), sys.argv[4])

