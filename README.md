A simple Python script that listens for items in a redis queue and works those items (if it knows how). The data is then stored back into redis.

Since redis is single threaded you can start many workers at once. Each will pop items off the queue and work them so if you add more 'functions' it's possible you want more workers too


# need to set some env variables
export GOOGLE_API_KEY=you_key_here
