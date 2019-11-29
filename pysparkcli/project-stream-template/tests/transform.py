def transformfunc(dataRDD):
    results = dataRDD.collect()
    favCount = 0
    user = None
    for result in results:
        #print("main", result)
        if result['favorite_count'] != None:
            print("result", result['favorite_count'])
            if result['favorite_count'] > favCount:
                favCount = result['favorite_count']
                user = result["user"]["name"]
    return user

