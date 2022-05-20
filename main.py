import argparse
import copy
from datetime import timedelta
import json
from multiprocessing import freeze_support
import random
import sys
import time
import uuid

from scipy import rand

from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import (ClusterOptions, ClusterTimeoutOptions, UpsertOptions)
from multiprocessing.pool import ThreadPool as Pool

parser = argparse.ArgumentParser()
parser.add_argument('-n', '--num-of-requests', help='number of requests', type=int)
parser.add_argument('-t', '--num-of-threads', help='number of threads', type=int)
parser.add_argument('-d', '--debug', help='debug', action='store_true')
parser.add_argument('-r', '--retry', help='number of retries', type=int)
parser.add_argument('-a', '--retry-random-delay', help='random delay for retry', action='store_true')

args = parser.parse_args(sys.argv[1:])

with open('./config.json') as config_file:
    configs = json.load(config_file)

bucket_name = configs['bucket']
collection = configs['collection']
scope = configs['scope']

pool_size = args.num_of_threads
num_of_requests = args.num_of_requests

print(f"""start with:
- {pool_size} threads
- {num_of_requests} requests
- debug: {args.debug}
- retry: {args.retry}
- retry random delay: {args.retry_random_delay}
""")


timeout_opts = ClusterTimeoutOptions(kv_timeout=timedelta(seconds=10))

auth = PasswordAuthenticator(
    configs['username'],
    configs['password']
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

cb_coll.upsert(challenge_id, challenge_answer)

random.seed()

def mutate_document_with_optilock(retry_time=0):
    try:
        result = cb_coll.get(challenge_id)
        res = result.content_as[dict]
        res['quizId']=str(uuid.uuid4())
        cb_coll.replace(res['uuid'], res, cas=result.cas)
    except Exception as e:
        if retry_time < args.retry:
           if args.retry_random_delay:
                time.sleep(random.randint(0, 300)/100)

           if mutate_document_with_optilock(retry_time+1):
               return True

        if args.debug:
            print(f'F:{e}')
        return False

    return True

def mutate_document_with_mutex_lock(retry_time=0):
    try:
        result = cb_coll.get_and_lock(challenge_id, timedelta(seconds=200))
    except Exception as e:
        if retry_time < args.retry and mutate_document_with_mutex_lock(retry_time+1):
            return True

        if args.debug:
            print(f'F:{e}')
        return False

    try:
        res = result.content_as[dict]
        res['quizId']=str(uuid.uuid4())
        cb_coll.replace(res['uuid'], res, cas=result.cas)
    except Exception as e:
        if args.debug:
            print(f'F:{e}')
        return False

    return True

def write_unique_doc(retry_time=0):
    try:
        doc = copy.deepcopy(challenge_answer)
        doc['uuid']=str(uuid.uuid4())
        cb_coll.upsert(doc['uuid'], doc)
    except Exception as e:
        if retry_time < args.retry:
           write_unique_doc(retry_time+1)
        return False

    return True

def  read_doc(retry_time=0):
    try:
        cb_coll.get(challenge_id)
    except Exception as e:
        if retry_time < args.retry:
           read_doc(retry_time+1)
        return False

    return True

def worker_opt():
    res = mutate_document_with_optilock()
    return 1 if res else 0

def worker_mutex():
    res = mutate_document_with_mutex_lock()
    return 1 if res else 0

def shared_doc_readonly():
    res = read_doc()
    return 1 if res else 0

def none_shared_doc_writeonly():
    res = write_unique_doc()
    return 1 if res else 0

benchmark_func = {
    'shared_doc_opt': worker_opt,
    'shared_doc_mutex': worker_mutex,
    'shared_doc_readonly': shared_doc_readonly,
    'none_shared_doc_writeonly': none_shared_doc_writeonly
}


def benchmark(title, worker):
    print(f'{title} started ...')
    pool = Pool(pool_size)
    start = time.time_ns()
    results = [pool.apply_async(worker, ()) for i in range(num_of_requests)]
    pool.close()
    pool.join()
    succeed = sum([res.get(timeout=1) for res in results])
    time_lapse = time.time_ns() - start
    print(f'Send with {pool_size} threads, finished in {time_lapse/1000000000}\n{succeed}/{num_of_requests} succeeded, with througput {succeed * 1000000000/time_lapse} ops')


if __name__ == '__main__':
   for key, value in benchmark_func.items():
       benchmark(key, value)

