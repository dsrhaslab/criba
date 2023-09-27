import argparse
import traceback
import pandas as pd
from utils.logging import Logger
from utils.queries import ESConnection
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import TfidfVectorizer
from sys import exit

logger = Logger("tfidfFam")

def identity_tokenizer(text):
    return text

def compute_tdidf(corpus, term, families, es_conn, send_to_es=True):
    logger.info("Computing %s tf-idf..." % term)

    tr_idf_model  = TfidfVectorizer(tokenizer=identity_tokenizer, token_pattern=None, lowercase=False)
    tf_idf_vector = tr_idf_model.fit_transform(corpus)
    tf_idf_array = tf_idf_vector.toarray()
    words_set = tr_idf_model.get_feature_names_out()
    df_tf_idf = pd.DataFrame(tf_idf_array, columns = words_set)

    print(df_tf_idf)

    if send_to_es:
        logger.info("Sending %s-based tf-idf to ES..." % term)
        bulk = []
        bulk_start_index = 0
        bulk_end_index = 0
        syscall_sequences_index = "criba_tfidf_families"
        id = 0
        for w in words_set:
            for i in range(len(families)):
                if df_tf_idf[w][i] == 0:
                    continue
                if len(bulk) >= 1000:
                        es_conn.bulkIndex(bulk, bulk_start_index, syscall_sequences_index, "_%s_sim" % (term))
                        logger.debug("\tbulking {} records ({} to {})...".format(len(bulk), bulk_start_index, bulk_end_index))
                        bulk_start_index = bulk_end_index
                        bulk = []

                id+=1
                doc = {
                    "familiy": families[i],
                    "tf-idf": df_tf_idf[w][i]
                }
                doc[term] = w
                bulk.append(doc)
                bulk_end_index += 1

        if len(bulk) > 0:
            es_conn.bulkIndex(bulk, bulk_start_index, syscall_sequences_index, "_%s_sim" % (term))
            logger.debug("\tbulking {} records ({} to {})...".format(len(bulk), bulk_start_index, bulk_end_index))
            bulk_start_index = bulk_end_index
            bulk = []
        logger.info("\tsent %d records to ES" % id)

    return tf_idf_vector

def compute_cosine_similarity(tf_idf_vector, families, term, es_conn, send_to_es=True):
    logger.info("Computing %s-based cosine similarity..." % term)
    cosine_sim = cosine_similarity(tf_idf_vector, tf_idf_vector)

    print(cosine_sim)

    if send_to_es:
        logger.info("Sending %s-based cosine similarity to ES..." % term)
        ndocs = 0
        for i in range(len(cosine_sim)):
            for j in range(len(cosine_sim[i])):
                res = {
                    "src": families[i],
                    "dst": families[j],
                    "similarity": cosine_sim[i][j],
                    "term": term
                }
                es_conn.docIndex("criba_tfidf_families", res, "_%s_sim_%s_%s" % (term, i, j))
                ndocs+=1
        logger.info("\tsent %d records to ES" % ndocs)


def getFamilySyscalls(es_conn, index, size, max_queries):
    syscalls = es_conn.getSyscallsByPath(index, size=size, max_queries=max_queries)
    family_syscalls = " ".join(syscalls)
    return family_syscalls, len(syscalls)

def compute_syscall_tfidf(families, es_conn, size, max_queries, send_to_es=True):

    corpus = []
    term = "syscall"

    logger.info("Computing %s-based tf-idf..." % term)

    for f in families:
        logger.debug("\tGetting %s from family %s..." % (term, f))
        index = "criba_trace_%s" % f
        vals, exec_time = es_conn.getSyscallsEvents(index, size, max_queries)
        corpus.append(vals)
        logger.info("\tGot %s %s for %s in %d ms" % (len(vals), term, f, exec_time))

    tf_idf_vector = compute_tdidf(corpus, term, families, es_conn, send_to_es)
    compute_cosine_similarity(tf_idf_vector, families, term, es_conn, send_to_es)

def compute_fextension_tfidf(families, es_conn, size, max_queries, fexts, fnames, send_to_es=True):
    term = "fextension"
    corpus_ext = []
    corpus_name = []

    logger.info("Building %s-based corpus..." % term)
    for f in families:
        index = "criba_trace_%s" % f
        exts, names = es_conn.getFNamesAndExtensionsEvents(index, size, max_queries)
        str_vals = []
        for list in exts:
            str_vals.extend(list)
        corpus_ext.append(str_vals)
        corpus_name.append(names)
        logger.info("\tgot %d events for family %s" % (len(names), f))

    if fexts:
        term = "fextension"
        tf_idf_vector = compute_tdidf(corpus_ext, term, families, es_conn, send_to_es)
        compute_cosine_similarity(tf_idf_vector, families, term, es_conn, send_to_es)

    if fnames:
        term = "fname"
        tf_idf_vector = compute_tdidf(corpus_name, term, families, es_conn, send_to_es)
        compute_cosine_similarity(tf_idf_vector, families, term, es_conn, send_to_es)


def _start():
    logger.info("tfidfFam Started!")

def _finish():
    logger.info("tfidfFam Finished!")
    exit(0)

def main():

    parser = argparse.ArgumentParser(description='Computes TF-IDF (syscall, fextension, and fnames -based) for DIO sessions')
    parser.add_argument('--url', metavar='url', default="http://cloud124:31111", type=str, help='elasticSearch URL')
    parser.add_argument('--show_sessions', action='store_true', help='show all sessions in DIO indexes')
    parser.add_argument('--size', metavar='size', default=1000, type=int, help='size of ElasticSearch query')
    parser.add_argument('--max_queries', metavar='max_queries', default=100, type=int, help='max queries to ES')
    parser.add_argument('--syscalls', action='store_true', default=False, help='compute TF-IDF for syscalls')
    parser.add_argument('--fextensions', action='store_true', default=False, help='compute TF-IDF for file extensions')
    parser.add_argument('--fnames', action='store_true',default=False, help='compute TF-IDF for file names')
    parser.add_argument('-l', '--local', action='store_true', default=False, help='Run locally only (no ES)')
    parser.add_argument('-d', '--debug', action='store_true', default=False, help='Debug mode')

    parser.add_argument('sessions', nargs='*', type=str, default=[])

    args = parser.parse_args()

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

        if len(args.sessions) == 0:
            logger.error("A list of valid session names must be provided. Use --show_sessions to see all sessions in DIO indexes.")
            _finish()
        else:
            logger.debug("Using sessions:")
            for s in args.sessions:
                logger.debug("\t%s" % s)


        if not args.syscalls and not args.fextensions and not args.fnames:
            args.syscalls = True
            args.fextensions = True
            args.fnames = True

        if args.syscalls:
            compute_syscall_tfidf(args.sessions, es_conn, args.size, args.max_queries, not args.local)

        if args.fextensions or args.fnames:
            compute_fextension_tfidf(args.sessions, es_conn, args.size, args.max_queries, args.fextensions, args.fnames, not args.local)

    except Exception as e:

        logger.error("Got an unexpected error: %s" % e)
        traceback.print_exception(type(e), e, e.__traceback__)

    _finish()

if __name__ == "__main__":
    main()