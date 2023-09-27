import argparse
import traceback
from itertools import groupby
from utils.logging import Logger
from utils.queries import ESConnection
from sys import exit

import nltk
from nltk.collocations import *


logger = Logger("FnGram")

def computeBigramCollocations(files):
    bigram_measures = nltk.collocations.BigramAssocMeasures()
    finder = BigramCollocationFinder.from_words(files)
    scored = finder.score_ngrams(bigram_measures.raw_freq)
    return scored

def computeTrigramCollocations(files):
    trigram_measures = nltk.collocations.TrigramAssocMeasures()
    finder = TrigramCollocationFinder.from_words(files)
    scored = finder.score_ngrams(trigram_measures.raw_freq)
    return scored

def computeQuadgramCollocations(files):
    fourgram_measures = nltk.collocations.QuadgramAssocMeasures()
    finder = QuadgramCollocationFinder.from_words(files)

    scored = finder.score_ngrams(fourgram_measures.raw_freq)
    return scored

def sendToElasticSearch(es_conn, session, scored, mode, tid):
    bulk = []
    bulk_start_index = 0
    bulk_end_index = 0

    if mode == "bigram":
        window_size = 2
    elif mode == "trigram":
        window_size = 3
    elif mode == "quadgram":
        window_size = 4

    for bigram, score in scored:

        if len(bulk) >= 1000:
            es_conn.bulkIndex(bulk, bulk_start_index, "criba_ngrams", session+"_{}_{}".format(mode,tid))
            logger.debug("bulking {} records ({} to {})...".format(len(bulk), bulk_start_index, bulk_end_index))
            bulk_start_index = bulk_end_index
            bulk = []

        doc = {
            "session_name": session,
            "tid": tid,
            "mode": mode
        }


        for i in range(window_size):
            doc["doc%d" % (i+1)] = bigram[i]
        doc["score"] = "{:.2e}".format(score)

        bulk.append(doc)
        bulk_end_index += 1

    if len(bulk) > 0:
        es_conn.bulkIndex(bulk, bulk_start_index, "criba_ngrams", session+"_{}_{}".format(mode,tid))
        logger.debug("bulking {} records ({} to {})...".format(len(bulk), bulk_start_index, bulk_end_index))
        bulk_start_index = bulk_end_index
        bulk = []

    logger.info("Sent {} {} for tid {} to ElasticSearch".format(bulk_end_index, mode, tid))

def print_collocations(scored, mode):
    logger.info("Top 10 {} collocations (out of {}):".format(mode, len(scored)))
    for bigram, score in scored[0:10]:
        print("\t{}\t{}".format(bigram, score))

def _start():
    logger.info("FnGram Started!")

def _finish():
    logger.info("FnGram Finished!")
    exit(0)

def main():

    parser = argparse.ArgumentParser(description='Search for syscalls sequences in ElasticSearch.')
    parser.add_argument('--url', metavar='url', default="http://cloud124:31111", type=str,
                        help='elasticSearch URL')
    parser.add_argument('--session', metavar='session', default="dtbebd_19.01.2023-15.57.51", type=str,
                        help='Session name')
    parser.add_argument('--comm', metavar='comm', default="dtbebd_19.01.2023-15.57.51", type=str,
        help='Command name')
    parser.add_argument('--size', metavar='size', default=1000, type=int,
                        help='size of ElasticSearch query')
    parser.add_argument('--show_sessions', action='store_true', help='show all sessions in (syscall sequences) index')


    parser = argparse.ArgumentParser(description='Computes the ngram collocations for a given session.')
    parser.add_argument('-u', '--url',  default="http://cloud124:31111", type=str, help='elasticSearch URL')
    parser.add_argument('-sz', '--size', default=1000, type=int, help='size of elasticsearch query')
    parser.add_argument('-mq', '--max_queries', default=100, type=int, help='max queries to ES')
    parser.add_argument('--show_sessions', action='store_true', help='show all sessions in DIO indexes')
    parser.add_argument('-l', '--local', action='store_true', default=False, help='Run locally only (no ES)')
    parser.add_argument('-d', '--debug', action='store_true', default=False, help='Debug mode')
    parser.add_argument('session', help='session name', default=None, nargs='?')


    args = parser.parse_args()
    index = "criba_trace_%s" % args.session

    _start()

    try:

        if args.debug:
            logger.setLevel("debug")

        es_conn = ESConnection(args.url)

        if args.show_sessions:
            sessions, _ = es_conn.getSessions()
            logger.info("Available sessions:")
            for s in sessions:
                logger.info("\t%-30s - %8d" % (s, sessions[s]))
            _finish()

        if args.session == None:
            logger.error("A valid session name must be provided. Use --show_sessions to see all sessions in DIO indexes.")
            _finish()

        tids, _ = es_conn.getTIDsInIndex(index)
        for tid in tids:
            newfiles = []
            logger.info("Processing TID: {}".format(tid))
            resp = es_conn.getTIDFilesEventsSorted(index, tid, size=args.size, max_queries=args.max_queries)
            newfiles += [key for key, _ in groupby(resp)]

            scored = computeBigramCollocations(newfiles)
            print_collocations(scored, "bigram")
            if not args.local:
                sendToElasticSearch(es_conn, args.session, scored, "bigram", tid)

            scored = computeTrigramCollocations(newfiles)
            print_collocations(scored, "trigram")
            if not args.local:
                sendToElasticSearch(es_conn, args.session, scored, "trigram", tid)

            scored = computeQuadgramCollocations(newfiles)
            print_collocations(scored, "quadgram")
            if not args.local:
                sendToElasticSearch(es_conn, args.session, scored, "quadgram", tid)


    except Exception as e:

        logger.error("Got an unexpected error: %s" % e)
        traceback.print_exception(type(e), e, e.__traceback__)

    _finish()

if __name__ == "__main__":
    main()