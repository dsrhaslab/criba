import argparse
import traceback
import multiprocessing
from itertools import repeat

from utils.logging import Logger
from utils.queries import ESConnection
from utils.tree import splitFile, buildTree
from utils.segment_tree import SegmentTree
from sys import exit

def subTreeToExplore(old, new):
    oldS = splitFile(old)
    newS = splitFile(new)

    l = min(len(oldS), len(newS))

    i = 0

    while i < l and oldS[i] == newS[i]:
        i = i + 1

    return oldS[0 : i + 1]


def isBFS(listFiles):
    order = list(map(lambda f: len(f.split("/")), listFiles))

    greatest = -1
    for i in order:
        if i < greatest:
            return False
        greatest = i

    return True


def isDFS(listFiles):
    tree = buildTree(listFiles)
    n = tree.size()
    list = [None] * (n + 1)
    i = 0

    for file in listFiles:
        s = splitFile(file)
        t = tree.getTree(s, len(s))
        idx = t.start
        list[idx] = i
        i += 1

    st = SegmentTree(n, list)

    for file in listFiles:
        s = splitFile(file)
        t = tree.getTree(s, len(s))
        b = st.minInRange(t.end + 1, n - 1)
        a = st.maxInRange(t.start, t.end)
        st.update(t.start, None)

        if (a is not None and b is not None) and a > b:
            return False

    return True


def remove_consecutive_duplicates(lst):
    last_seen = None
    res = []
    for x in lst:
        if x != last_seen:
            res.append(x)
            last_seen = x
    return res


def foldersFromFiles(files):
    files = list(filter(lambda f: f.startswith("/app/files"), files))
    folders = list(map(lambda s: s.rsplit("/", 1)[0], files))
    return remove_consecutive_duplicates(folders)


def classification(folders):
    res = ""
    if isDFS(folders):
        res = "DFS"

    if isBFS(folders):
        if res == "":
            res = "BFS"
        else:
            res = "DFS/BFS"

    if res == "":
        res = "Unknown"

    if folders == []:
        res = "No transversal"

    return res


def treeToArrayDict(tree, parent=None):
    res = (
        [{"id": tree.start, "name": tree.node}]
        if parent == None
        else [{"id": tree.start, "name": tree.node, "parent": parent}]
    )

    for st in tree.subtrees:
        res = res + treeToArrayDict(tree.subtrees[st], parent=tree.start)

    return res


def generate_tid_transversal_classification(files, tid):
    folders = foldersFromFiles(files)

    return {"tid": tid, "transversal": classification(folders)}


def generate_transversal_classification(es_conn, args, logger, index, transversalIndex):
    index = "criba_trace_" + args.session
    tids = list(map(lambda x: x, es_conn.getTIDsInIndex(index)[0]))
    nThreads = len(tids)

    logger.info(f"Found {nThreads} threads")

    files = {}
    for tid in tids:
        files[tid] = es_conn.getOpenPaths(index, tid, args.size, args.maxQueries)

    # Create a list of tuples corresponding to the arguments to be passed to
    # generate_tid_transversal_classification in each subprocess
    functionArgs = [(v, k) for k, v in files.items()]
    with multiprocessing.Pool(args.nProc) as pool:
        result = list(
            pool.starmap(generate_tid_transversal_classification, functionArgs)
        )

    id=0
    for doc in result:
        if args.debug:
            logger.info(doc)

        doc["session_name"] = args.session
        es_conn.docIndex(transversalIndex, doc, args.session + "_tid_" + str(doc["tid"]))
        id += 1


def generate_file_system_tree(es_conn, args, logger, transversalIndex, bulk_size=1000):
    index = "criba_trace_" + args.session

    logger.info(f"Beginning filesystem tree generation on index {args.session}")

    files = es_conn.getAbsoluteFilePaths(index, args.size)[0]

    logger.info(f"File paths gathered")

    tree = buildTree(files)
    tree.numerate(0)

    arr = treeToArrayDict(tree)

    logger.info(f"Tree computed. Saving to Elastic Search")

    id = 0

    bulk = []
    bulk_start_index = 0
    bulk_end_index = 0
    if not args.local:
        for node in arr:

            id += 1

            if len(bulk) == bulk_size:
                es_conn.bulkIndex(bulk, bulk_start_index, transversalIndex, args.session)
                logger.debug("bulking {} records ({} to {})...".format(len(bulk), bulk_start_index, bulk_end_index))
                bulk_start_index = bulk_end_index
                bulk = []

            doc = {}
            if "parent" in node:
                doc = {"id": node["id"], "session_name": args.session, "name": node["name"], "parent": node["parent"]}
            else:
                doc = {"id": node["id"], "session_name": args.session, "name": node["name"]}

            bulk.append(doc)
            bulk_end_index += 1

        if len(bulk) > 0:
            es_conn.bulkIndex(bulk, bulk_start_index, transversalIndex, args.session)
            logger.info("bulking {} records ({} to {})...".format(len(bulk), bulk_start_index, bulk_end_index))
            bulk_start_index = bulk_end_index
            bulk = []


def main():
    parser = argparse.ArgumentParser(
        description="Search for syscalls sequences in ElasticSearch."
    )
    parser.add_argument(
        '-u',
        "--url",
        metavar="url",
        default="http://192.168.112.77:31111/",
        type=str,
        help="elasticSearch URL",
    )
    parser.add_argument(
        '-d',
        "--debug",
        action="store_true",
        help="Whether to send results to stdout (debug) or to ES (not debug)",
    )
    parser.add_argument(
        "--omit-index",
        action="store_true",
        help="Whether to omit the creation of the new index",
    )
    parser.add_argument(
        '-p',
        "--nProc",
        metavar="nProc",
        default=1,
        type=int,
        help="Number of processes to use when classifying transversals",
    )
    parser.add_argument(
        '-sz',
        "--size",
        metavar="size",
        default=1000,
        type=int,
        help="Number of events to fetch at once",
    )
    parser.add_argument(
        "-mq",
        "--maxQueries",
        metavar="maxQueries",
        default=1000,
        type=int,
        help="Maximum number of queries to make for one operation",
    )
    parser.add_argument(
        '-l',
        '--local',
        action='store_true',
        default=False,
        help='Run locally only (no ES)'
    )
    parser.add_argument(
        'session',
        help='session name',
        default=None,
        nargs='?'
    )

    logger = Logger("TRANSVERSALS")

    args = parser.parse_args()

    try:

        if args.debug:
            logger.setLevel("debug")

        es_conn = ESConnection(args.url)

        transversalIndex = "criba_transversals"

        if not args.omit_index and not args.debug:
            logger.info(f"Creating new index {transversalIndex}")
            es_conn.createNewIndex(transversalIndex)

        generate_file_system_tree(es_conn, args, logger, transversalIndex, args.size)

        logger.info("Saved to Elastic. Beginning transversal classification")

        generate_transversal_classification(
            es_conn, args, logger, args.session, transversalIndex
        )

        logger.info("Script finished")
    except Exception as e:
        logger.error("Got an unexpected error: %s" % e)
        traceback.print_exception(type(e), e, e.__traceback__)


if __name__ == "__main__":
    main()
