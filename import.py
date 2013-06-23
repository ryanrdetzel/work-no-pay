import simplejson as json
import redis

lines = [line.strip() for line in open('ryan_data.txt')]
r = redis.StrictRedis(host='localhost', port=6379, db=14)

for line in lines:
    (query, lat, lng) = line.split("---")
    job = {
        'function': 'google_places_search',
        'query': query,
        'latitude': lat,
        'longitude': lng
    }

    r.rpush('worker_queue', json.dumps(job))
