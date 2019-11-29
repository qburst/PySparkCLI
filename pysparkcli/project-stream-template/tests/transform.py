def transformfunc(dataRDD):
    results = dataRDD.collect()
    favCount = 0
    user = None
    for result in results:
        #print("main", result)
        if result["user"]["followers_count"]:
            if result["user"]['followers_count'] > favCount:
                favCount = result["user"]['followers_count']
                print(favCount)
                user = result["user"]["name"]
    return {"user":user, "favcount":favCount}

