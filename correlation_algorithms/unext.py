import elasticsearch
import traceback
import argparse
from utils.logging import Logger
from utils.queries import ESConnection
from sys import exit

logger = Logger("UNExt")

def update_name_and_extensions(es_conn, index, timeout, print_ext=False, size=10000):
    logger.info("Updating file names and extensions...")
    try:
        res = es_conn.updateFileNamesAndExtensions(index, timeout)
        logger.info("Updated %s documents (total=%s) in %d ms." % (res["updated"], res["total"], res["took"]))

        if print_ext:
            logger.info("Getting file extensions...")
            extensions_buckets, took = es_conn.getFileExtensionsInIndex(index, size)
            logger.info("Got %d extensions in %d ms." % (len(extensions_buckets), took))
            logger.info("Extensions:")
            for bucket in extensions_buckets:
                file_extention = bucket["key"]
                count = bucket["doc_count"]
                logger.info("\t- %20s : %s" % (file_extention, count))

    except elasticsearch.ConflictError as e:
        logger.error("ConflictError. There is another update in progress.")
        return
    except elasticsearch.TransportError as e:
        if isinstance(e, elasticsearch.ConnectionTimeout):
            logger.error("Network connection timeed out. Use the flag --timeout to increase the timeout.")
        else:
            logger.error("Failed (%s)" % e)
        return

def _start():
    logger.info("UNExt Started!")

def _finish():
    logger.info("UNExt Finished!")
    exit(0)


def main():

    parser = argparse.ArgumentParser(description='Extracts the name and extension from file paths.')
    parser.add_argument('-u', '--url',  default="http://cloud124:31111", type=str, help='elasticSearch URL')
    parser.add_argument('-sz', '--size', default=10000, type=int, help='size of elasticsearch query')
    parser.add_argument('-t', '--timeout', default=120, type=int, help='elasticsearch query timeout')
    parser.add_argument('--print', action='store_true', help='print extensions')
    parser.add_argument('--show_sessions', action='store_true', help='show all sessions in DIO indexes')
    parser.add_argument('-l', '--local', action='store_true', default=False, help='Run locally only')
    parser.add_argument('-d', '--debug', action='store_true', default=False, help='Enable debug mode')
    parser.add_argument('session', help='session name', default=None, nargs='?')


    args = parser.parse_args()
    index = "criba_trace_%s" % args.session

    _start()

    try:

        if args.debug:
            global logger
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

        update_name_and_extensions(es_conn, index, args.timeout, args.print, args.size)

    except Exception as e:

        logger.error("Got an unexpected error: %s" % e)
        traceback.print_exception(type(e), e, e.__traceback__)

    _finish()

if __name__ == "__main__":
    main()