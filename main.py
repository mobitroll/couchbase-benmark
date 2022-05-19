import argparse
from datetime import timedelta
import sys
import time
import uuid

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import (ClusterOptions, ClusterTimeoutOptions, UpsertOptions)
from multiprocessing.pool import ThreadPool as Pool

parser = argparse.ArgumentParser()
parser.add_argument('-u', '--username', help='username')
parser.add_argument('-p', '--password', help='password')
parser.add_argument('-b', '--bucket', help='bucket')
parser.add_argument('-c', '--collection', help='collection')
parser.add_argument('-s', '--scope', help='scope')
parser.add_argument('-n', '--num-of-requests', help='number of requests', type=int)
parser.add_argument('-t', '--num-of-threads', help='number of threads', type=int)
parser.add_argument('-o', '--optimistic-lock', help='optimistic lock', action='store_true')

args = parser.parse_args(sys.argv[1:])
bucket_name = args.bucket
collection = args.collection
scope = args.scope
pool_size = args.num_of_threads
num_of_requests = args.num_of_requests
opt_lock = args.optimistic_lock

timeout_opts = ClusterTimeoutOptions(kv_timeout=timedelta(seconds=10))

auth = PasswordAuthenticator(
    args.username,
    args.password
)
cluster = Cluster('couchbase://localhost', ClusterOptions(auth, timeout_options=timeout_opts))
cluster.wait_until_ready(timedelta(seconds=5))
cb = cluster.bucket(bucket_name)
cb_coll = cb.scope(scope).collection(collection)
cb_coll_default = cb.default_collection()

challenge_id = 'a75d9637-01d5-4714-6ef1-7144a70e537f'
challenge_answer = {
  'uuid': challenge_id,
  'quizId':'',
  'quizTitle':'',
  'quizVisibility':'',
  'quizType':'',
  'gameMode':'',
  'quizCreator':'',
  'quizCoverMetadata':'',
  'quizMaster':{},
  'organisationId':'',
  'hostOrganisationId':'',
  'sessionId':'',
  'startTime':'',
  'smartPracticeUnlockTimes':'',
  'numQuestions':'',
  'inGameIndex':'',
  'device':'',
  'question':'',
  'kickedPlayers':[],
  'metadata':{
      'location':''
    },
  'ghostAnswersId':'',
  'teamMode':'',
  'collaborationMode':'',
  'liveChallengeId':'',
  'liveGameId':''
}


def mutate_document_with_optilock(retry_time=0):
    try:
        result = cb_coll.get(challenge_id)
        res = result.content_as[dict]
        res['quizId']=str(uuid.uuid4())
        cb_coll.replace(res['uuid'], res, cas=result.cas)
    except Exception as e:
        # retry until we make it
        if retry_time < 10:
           mutate_document_with_optilock(retry_time+1)
        print(f'F:{e}')
        return False

    return True

def mutate_document_with_mutex_lock(retry_time=0):
    try:
        result = cb_coll.get_and_lock(challenge_id, timedelta(seconds=200))
    except Exception as e:
        if retry_time < 10:
           if mutate_document_with_mutex_lock(retry_time+1):
               return True

        print(f'F:{e}')
        return False

    try:
        res = result.content_as[dict]
        res['quizId']=str(uuid.uuid4())
        cb_coll.replace(res['uuid'], res, cas=result.cas)
    except Exception as e:
        print(f'F:{e}')
        return False

    return True

def worker(opt_lock):
    res = mutate_document_with_optilock() if opt_lock else mutate_document_with_mutex_lock()
    return 1 if res else 0

pool = Pool(pool_size)

start = time.time_ns()
results = [pool.apply_async(worker, (opt_lock,)) for i in range(num_of_requests)]
pool.close()
pool.join()
succeed = sum([res.get(timeout=1) for res in results])
time_lapse = time.time_ns() - start
print(f'Send with {pool_size} threads, finished in {time_lapse/1000000000}\n{succeed}/{num_of_requests} succeeded, with througput {succeed * 1000000000/time_lapse} ops')