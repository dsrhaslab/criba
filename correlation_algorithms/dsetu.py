import argparse
import traceback
import json

from utils.logging import Logger
from utils.queries import ESConnection


def get_dataset_info(file, delimiter=","):
    with open(file) as f:
        # Ignore CSV header
        lines = f.readlines()[1:]

    res = []

    for line in lines:
        # Remove trailing '\n'
        line = line[:-1]
        sp = line.split(delimiter)
        res = res + [{"path": sp[0], "size": int(sp[1])}]

    return res


def get_extension(file):
    if "." in file:
        return file.split(".")[-1]
    return "(empty)"

def send_data_to_es(logger, es_conn, data, session, type, used):
    bulk = []
    bulk_start_index = 0
    bulk_end_index = 0
    dataset_usage_index = "criba_dataset_usage"
    for val in data:
        if len(bulk) >= 1000:
            es_conn.bulkIndex(bulk, bulk_start_index, dataset_usage_index, session+"_"+str(used))
            logger.info("bulking {} records ({} to {})...".format(len(bulk), bulk_start_index, bulk_end_index))
            bulk_start_index = bulk_end_index
            bulk = []

        doc = {
            "session_name": session,
            "used": used,
        }

        if type == "paths":
            doc["path"] = val
        elif type == "extensions":
            doc["extension"] = val

        bulk.append(doc)
        bulk_end_index += 1

    if len(bulk) > 0:
        es_conn.bulkIndex(bulk, bulk_start_index, dataset_usage_index, session+"_"+str(used))
        logger.info("bulking {} records ({} to {})...".format(len(bulk), bulk_start_index, bulk_end_index))
        bulk_start_index = bulk_end_index
        bulk = []


def main():
    parser = argparse.ArgumentParser(description="Check dataset file paths and extensions usage")
    parser.add_argument('-u', '--url',  default="http://cloud124:31111", type=str, help='elasticSearch URL')
    parser.add_argument('-sz', '--size', default=10000, type=int, help='size of elasticsearch query')
    parser.add_argument('-l', '--local', action='store_true', default=False, help='Run locally only (no ES)')
    parser.add_argument('-d', '--debug', action='store_true', default=False, help='Debug mode')
    parser.add_argument("--file", metavar="file", default="res.csv", type=str, help="the csv with the info of the dataset")
    parser.add_argument("--output", metavar="output", default=".", type=str, help="The file to write the output to")
    parser.add_argument('session', help='session name', default=None, nargs='?')

    logger = Logger("DATASET_USAGE")

    args = parser.parse_args()

    try:
        logger.info("Connecting to ES")
        es_conn = ESConnection(args.url)
        logger.info("Getting file paths")
        paths = es_conn.getFilePaths("criba_trace_" + args.session, args.size)[0]
        logger.info("Gathering dataset info")
        dataset = get_dataset_info(args.file)

        logger.info("Computing used files")
        used_files = [x for x in list(map(lambda f: f["path"], dataset)) if x in paths]

        logger.info("Computing unused files")
        not_used_files = [
            x for x in list(map(lambda f: f["path"], dataset)) if x not in paths
        ]

        logger.info(f"{len(used_files)} used files")
        logger.info(f"{len(not_used_files)} unused files")
        logger.info("Computing dataset extensions")
        extensions = set(map(lambda f: get_extension(f), paths))

        logger.info("Computing used extensions")
        used_extensions = list(set(map(lambda f: get_extension(f), used_files)))
        logger.info("Computing unused extensions")
        not_used_extensions = list(set(map(lambda f: get_extension(f), not_used_files)))

        logger.info(f"{len(used_extensions)} used extensions")
        logger.info(f"{len(not_used_extensions)} not used extensions")

        if args.local:
            logger.info("Saving to file")
            with open(args.output, "w") as f:
                json.dump(
                    {
                        "used": {
                            "files_count": len(used_files),
                            "files": used_files,
                            "extensions_count": len(used_extensions),
                            "extensions": used_extensions,
                        },
                        "not_used": {
                            "files_count": len(not_used_files),
                            "files": not_used_files,
                            "extensions_count": len(not_used_extensions),
                            "extensions": not_used_extensions,
                        },
                    },
                    f,
                    indent=4,
                )
        else:
            logger.info("Saving to ES")
            send_data_to_es(logger, es_conn, used_files, args.session, "paths", True)
            send_data_to_es(logger, es_conn, not_used_files, args.session, "paths", False)

            send_data_to_es(logger, es_conn, used_extensions, args.session, "extensions", True)
            send_data_to_es(logger, es_conn, not_used_extensions, args.session, "extensions", False)



        logger.info("Script finished")
    except Exception as e:
        logger.error("Got an unexpected error: %s" % e)
        traceback.print_exception(type(e), e, e.__traceback__)


if __name__ == "__main__":
    main()
