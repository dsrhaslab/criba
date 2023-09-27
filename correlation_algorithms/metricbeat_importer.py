import argparse
import traceback
from utils.logging import Logger
from utils.queries import ESConnection
import json
from sys import exit

logger = Logger("MetricbeatImporter")

def _start():
    logger.info("MetricbeatImporter Started!")

def _finish():
    logger.info("MetricbeatImporter Finished!")
    exit(0)


def main():

    parser = argparse.ArgumentParser(description='Search for syscalls sequences in ElasticSearch.')
    parser.add_argument('-u', '--url',  default="http://cloud124:31111", type=str, help='elasticSearch URL')
    parser.add_argument('-sz', '--size', default=1000, type=int, help='size of elasticsearch query')
    parser.add_argument('-s', '--session', metavar='session', default=None, type=str, help='Session name')
    parser.add_argument('file', help='metricbeat file path', default=None, nargs='?')

    args = parser.parse_args()
    _start()

    try:

        if args.session is None:
            print("Needs a session name")
            _finish()

        if args.file is None:
            print("Needs a file path")
            _finish()

        es_conn = ESConnection(args.url)

        print("Importing file %s to session %s" % (args.file, args.session))

        i = 0
        f = open(args.file, 'r')
        bulk = []
        bulk_start_index = 0
        bulk_end_index = 0

        for line in f:
            if len(bulk) >= 1000:
                es_conn.bulkIndex(bulk, bulk_start_index, 'criba_metricbeat_'+str(args.session), args.session)
                logger.debug("bulking {} records ({} to {})...".format(len(bulk), bulk_start_index, bulk_end_index))
                bulk_start_index = bulk_end_index
                bulk = []


            i = i + 1
            event = json.loads(line)
            event["session_name"] = args.session
            bulk.append(event)
            bulk_end_index += 1

        if len(bulk) > 0:
            es_conn.bulkIndex(bulk, bulk_start_index, 'criba_metricbeat_'+str(args.session), args.session)
            logger.debug("bulking {} records ({} to {})...".format(len(bulk), bulk_start_index, bulk_end_index))
            bulk_start_index = bulk_end_index
            bulk = []

        print("Imported %d events" % (i))

    except Exception as e:

        logger.error("Got an unexpected error: %s" % e)
        traceback.print_exception(type(e), e, e.__traceback__)

    _finish()

if __name__ == "__main__":
    main()