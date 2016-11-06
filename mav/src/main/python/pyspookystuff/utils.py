from math import radians, cos, sin, asin, sqrt

def haversineGroundDist(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6371 # Radius of earth in kilometers. Use 3956 for miles
    return c * r

def airDist(p1, p2):
    # type: (LocationGlobal, LocationGlobal) -> double
    haversine = haversineGroundDist(p1.lng, p1.lat, p2.lng, p2.lat)
    altDist = p2.alt - p1.alt
    result = sqrt(haversine*haversine + altDist*altDist)
    return result

def retry(maxTrial=3, name=''):

    def decorate(fn):
        def retryFn(*args, **kargs):
            for i in range(1, 100):
                try:
                    print(name, " trial ", str(i))

                    result = fn(*args, **kargs)
                    return result
                except:
                    if i >= maxTrial:
                        raise
                    else:
                        continue
        return retryFn
    return decorate
