import time
import redis
import simplejson as json
import requests

REDIS_QUEUE = 'worker_queue'
REDIS_GOOGLE_DATA = 'data:google'

SLEEP = 1

class Worker():
    def __init__(self):
        self.r = redis.StrictRedis(host='localhost', port=6379, db=14)
        self.loop()

    def loop(self):
        while True:
            job = self.r.lpop(REDIS_QUEUE)
            if job:
                try:
                    data = json.loads(job)
                except Exception as e:
                    print "Failed: %s" % job
                    print e
                    continue

                print "Processing %s" % data
                function = data.get('function', None)
                if not function:
                    print "No function in payload"
                    continue
                if callable(getattr(self, function)):
                    getattr(self, function)(data)
            time.sleep(SLEEP)

    def google_places_detail(self, job):
        """
            Gets the google places detail results
        """
        API_KEY = 'AIzaSyCJLA40JKAB9ImEI75qqDT4AmGxM8UsMf8'

        reference = job.get('reference', None)
        if not reference:
            print "No reference found"
            return

        url = "https://maps.googleapis.com/maps/api/place/details/json"
        args = {
            'key': API_KEY,
            'sensor': 'false',
            'reference': reference,
        }

        arg_list = []
        for k,v in args.items():
            arg_list.append("%s=%s" % (k, v))

        url = "%s?%s" % (url, '&'.join(arg_list))

        r = requests.get(url)
        data = r.json()
        result = data['result']

        id = result['id']
        self.r.set("%s:%s" % (REDIS_GOOGLE_DATA, id), json.dumps(data))
        print "Stored: %s" % id

    def google_places_search(self, job):
        '''
            Gets data from google places
        '''
        API_KEY = 'AIzaSyCJLA40JKAB9ImEI75qqDT4AmGxM8UsMf8'

        url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
        query = job.get('query', None)
        if not query:
            print "No query in job"
            return
        args = {
            'key': API_KEY,
            'sensor': 'false',
            'query': query
        }
        if 'next_page_token' in job:
            args.update({'pagetoken': job['next_page_token']})

        if 'longitude' in job and 'latitude' in job:
            args.update({
                'location': "%s,%s" % (job['latitude'], job['longitude']),
                'radius': job.get('radius', 10),
            })

        arg_list = []
        for k,v in args.items():
            arg_list.append("%s=%s" % (k, v))

        url = "%s?%s" % (url, '&'.join(arg_list))

        r = requests.get(url)
        data = r.json()

        next_page_token = data.get('next_page_token', None)
        results = data.get('results', None)
        status = data.get('status', None)

        if status == "OK":
            print "Found %d results" % len(results)
            for result in results:
                # Pull out the ref and queue up the detail search
                ref = result.get('reference', None)
                id = result.get('id', None)
                if ref and id:
                    if self.r.exists("%s:%s" % (REDIS_GOOGLE_DATA, id)):
                        print "Already have that id"
                    else:
                        new_job = {
                            'function': 'google_places_detail',
                            'reference': ref,
                        }
                        self.r.rpush(REDIS_QUEUE, json.dumps(new_job))
            if next_page_token:
                print "Has more pages, queue those up"
                job.update({
                    'next_page_token': next_page_token,
                })
                self.r.rpush(REDIS_QUEUE, json.dumps(job))
        elif status == "OVER_QUERY_LIMIT":
            print "over limit, sleep?"
            time.sleep(24*60*60)
if __name__ == "__main__":
    worker = Worker()
