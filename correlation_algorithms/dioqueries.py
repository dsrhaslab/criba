import argparse
import traceback
from utils.logging import Logger
from utils.queries import ESConnection
from sys import exit

logger = Logger("DIOQueries")

def exec_query(args):
    index = "criba_trace_%s" % args.session
    es_conn = ESConnection(args.url)
    if args.query == "get_sessions":
        sessions, _ = es_conn.getSessions()
        logger.info("Available sessions:")
        for s in sessions:
            logger.info("\t%-30s - %8d" % (s, sessions[s]))
        _finish()

    else:
        if args.session == None:
            logger.error("A valid session name must be provided. Use --show_sessions to see all sessions in DIO indexes.")
            _finish()

        if args.query == "get_commands":
            logger.info("Getting commands from index %s..." % index)
            commands, exec_time = es_conn.getCommandsInIndex(index, args.size, timeout=args.timeout)
            logger.info("Got %s commands in %d ms" % (len(commands), exec_time))
            for c in commands:
                logger.info("\t- %s: %d" % (c, commands[c]))
            _finish()
        elif args.query == "get_pids":
            logger.info("Getting pids from index %s..." % index)
            pids, exec_time = es_conn.getPIDsInIndex(index, size=args.size, timeout=args.timeout)
            logger.info("Got %s pids in %d ms" % (len(pids), exec_time))
            for p in pids:
                logger.info("\t- %s: %d" % (p, pids[p]))
            _finish()
        elif args.query == "get_tids":
            logger.info("Getting tids from index %s..." % index)
            tids, exec_time = es_conn.getTIDsInIndex(index, size=args.size, timeout=args.timeout)
            logger.info("Got %s tids in %d ms" % (len(tids), exec_time))
            for t in tids:
                logger.info("\t- %s: %d" % (t, tids[t]))
            _finish()
        elif args.query == "get_file_types":
            logger.info("Getting file types from index %s..." % index)
            fileTypes, exec_time = es_conn.getFileTypesInIndex(index, args.size, timeout=args.timeout)
            logger.info("Got %s file types in %d ms" % (len(fileTypes), exec_time))
            for f in fileTypes:
                logger.info("\t- %s: %d" % (f, fileTypes[f]))
            _finish()
        elif args.query == "get_file_paths":
            logger.info("Getting file paths from index %s..." % index)
            filePaths, exec_time = es_conn.getFilePaths(index, args.size, timeout=args.timeout)
            logger.info("Got %s file paths in %d ms." % (len(filePaths), exec_time))
            for fp in filePaths:
                print(fp)
            _finish()
        elif args.query == "get_relative_file_paths":
            logger.info("Getting relative file paths from index %s..." % index)
            filePaths, exec_time = es_conn.getRelativeFilePaths(index, args.size, timeout=args.timeout)
            logger.info("Got %s relative file paths in %d ms." % (len(filePaths), exec_time))
            for fp in filePaths:
                print(fp)
            _finish()
        elif args.query == "get_file_extensions":
            logger.info("Getting file extensions from index %s..." % index)
            fileExtensions, exec_time = es_conn.getFileExtensionsInIndex(index, args.size, timeout=args.timeout)
            logger.info("Got %s file extensions in %d ms" % (len(fileExtensions), exec_time))
            _finish()
        elif args.query == "get_unique_file_paths":
            logger.info("Getting unique file paths from index %s..." % index)
            uniquePaths, exec_time = es_conn.countUniquePaths(index, args.size, timeout=args.timeout)
            logger.info("Got %s unique file paths in %d ms" % (uniquePaths, exec_time))
            _finish()
        elif args.query == "get_unique_file_names":
            logger.info("Getting unique file names from index %s..." % index)
            uniqueNames, exec_time = es_conn.countUniqueFileNames(index, args.size, timeout=args.timeout)
            logger.info("Got %s unique file names in %d ms" % (uniqueNames, exec_time))
            _finish()
        elif args.query == "get_unique_file_extensions":
            logger.info("Getting unique file extensions from index %s..." % index)
            uniqueExtensions, exec_time = es_conn.countUniqueExtensions(index, args.size, timeout=args.timeout)
            logger.info("Got %s unique file extensions in %d ms" % (uniqueExtensions, exec_time))
            _finish()
        elif args.query == "get_file_tags":
            logger.info("Getting file tags from index %s..." % index)
            fileTags, exec_time = es_conn.getFileTagsInIndex(index, args.size, timeout=args.timeout)
            logger.info("Got %s file tags in %d ms" % (len(fileTags), exec_time))
            for ft in fileTags:
                print("%s" % ft["key"])
            _finish()
        elif args.query == "get_dataset_files":
            logger.info("Getting dataset files from index %s..." % index)
            query_darkside = {
                "bool": {
                    "must_not": [
                        {
                            "match_phrase": {
                                "fdata.file_path": "*darkside_readme.txt"
                            }
                        },
                        {
                            "match_phrase": {
                                "fdata.file_extension": "*.darkside"
                            }
                        }
                    ],
                    "must": [
                        {
                            "match_phrase": {
                                "fdata.file_path": "/app/files"
                            }
                        }
                    ]
                }
            }
            query_defray777 = {
                "bool": {
                    "must_not": [
                        {
                            "match_phrase": {
                                "fdata.file_path": "*!NEWS_FOR_EIGSI!.txt"
                            }
                        },
                        {
                            "match_phrase": {
                                "fdata.file_extension": "*.31gs1"
                            }
                        }
                    ],
                    "must": [
                        {
                            "match_phrase_prefix": {
                                "fdata.file_path": "/app/files"
                            }
                        }
                    ]
                 }
            }
            query_revil = {
                "bool": {
                    "must_not": [
                        {
                            "match_phrase": {
                                "fdata.file_path": "*qoxaq-readme.txt"
                            }
                        },
                        {
                            "match_phrase": {
                                "fdata.file_extension": "*.qoxaq"
                            }
                        }
                    ],
                    "must": [
                        {
                            "match_phrase": {
                                "fdata.file_path": "/app/files"
                            }
                        }
                    ]
                }
            }
            query=None
            if args.session == "darkside":
                query = query_darkside
                logger.info("Using query query_darkside")
            elif args.session == "revil":
                query = query_revil
                logger.info("Using query query_revil")

            logger.info("Getting nº files from index %s..." % index)
            res, took = es_conn.getNoUniquePaths(index, "fdata.file_path.keyword")
            logger.info("Got %s unique files in %d ms." % (res, took))

            datasetFiles, exec_time = es_conn.getFilePaths(index, args.size, query=query, timeout=args.timeout)
            logger.info("Got %s dataset files in %d ms." % (len(datasetFiles), exec_time))
            for df in datasetFiles:
                print(";%s" % df)
            _finish()
        elif args.query == "fixFilesPaths":
            logger.info("Fixing file paths for index %s... OLD: %s, NEW: %s" % (index, args.oldp, args.newp))
            es_conn.fixFilesPaths(index, args.oldp, args.newp,  timeout=args.timeout)
            _finish()
        else:
            logger.info("Unknown query: %s" % args.query)

def _print_args(args):
    logger.debug("Arguments:")
    for arg in vars(args):
        logger.debug("\t%s: %s" % (arg, getattr(args, arg)))

def _start():
    logger.info("DIOQueries Started!")

def _finish():
    logger.info("DIOQueries Finished!")
    exit(0)

def main():

    parser = argparse.ArgumentParser(description='Executes queries to ElasticSearch.')
    parser.add_argument('-u', '--url',  default="http://cloud124:31111", type=str, help='elasticSearch URL')
    parser.add_argument('-s', '--session',  default=None, type=str, help='session name')
    parser.add_argument('-sz', '--size', default=1000, type=int, help='size of elasticsearch query')
    parser.add_argument('-mq', '--max_queries', default=100, type=int, help='max queries to ES')
    parser.add_argument('-d', '--debug', action='store_true', default=False, help='Debug mode')
    parser.add_argument('-t', '--timeout', default=120, type=int, help='elasticsearch query timeout')
    parser.add_argument('query', help='query name', default=None, nargs='?')
    parser.add_argument('--oldp', default=None, type=str, help='Old path')
    parser.add_argument('--newp', default=None, type=str, help='new path')

    args = parser.parse_args()

    _start()

    try:

        if args.debug:
            logger.setLevel("debug")
            _print_args(args)

        exec_query(args)


    except Exception as e:

        logger.error("Got an unexpected error: %s" % e)
        traceback.print_exception(type(e), e, e.__traceback__)

    _finish()

if __name__ == "__main__":
    main()