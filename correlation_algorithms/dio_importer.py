import argparse
import traceback
import json
from utils.logging import Logger
from utils.queries import ESConnection
from sys import exit

logger = Logger("DoParser")

def prepare_indices(es_conn, session):
	index = "criba_trace_{}".format(session)
	es_conn.createNewDIOTracerIndex(index)
	es_conn.createNewIndex(index+"-paths")
	es_conn.createDIOIngestPipeleine()
	return index

def bulk_data(es_conn, bulk, bulk_size, bulk_start_index, bulk_end_index, index, session):
	errors, took = es_conn.bulkIndex(bulk, bulk_start_index, index, session, "split-events-pipeline")
	if errors:
		logger.error("Got following errors while bulking: ")
		for error in errors:
			logger.error("\t- {} (x{})".format(error, errors[error]))
		_finish()
	else:
		logger.debug("bulked {} records ({} to {}) in {} ms".format(len(bulk), bulk_start_index, bulk_end_index, took))
	bulk_start_index = bulk_end_index
	bulk = []
	return bulk, bulk_start_index, bulk_end_index

def parse_tracer(es_conn, session, filename, bulk_size=1000):
	logger.info("Parsing file: {}".format(filename))

	# parse file and save records to ES
	bulk = []
	bulk_start_index = 0
	bulk_end_index = 0
	min_time_str = ""
	max_time_str = ""
	time = {"min_t": -1, "max_t": 0, "duration": 0}
	index = None

	if session is not None:
		newSessionName = True
		logger.info("Session name: {}".format(session))
	else:
		newSessionName = False

	try:
		with open(filename, 'r') as file:

			# read file line by line
			for line in file:

				# flush records if size equals bulk_size
				if len(bulk) == bulk_size:
					if index is None:
						index = prepare_indices(es_conn, session)
					bulk, bulk_start_index, bulk_end_index = bulk_data(es_conn, bulk, bulk_size, bulk_start_index, bulk_end_index, index, session)

				# parse line to a json object
				jsonObject = json.loads(line)

				if newSessionName:
					jsonObject["session_name"] = session
				elif session is None:
					session = jsonObject["session_name"]
					logger.info("Session name: {}".format(session))

				if ("call_timestamp" in jsonObject) and (time["min_t"] == -1 or jsonObject["call_timestamp"] < time["min_t"]):
					time["min_t"] = jsonObject["call_timestamp"]
					min_time_str = jsonObject["time_called"]

				if ("return_timestamp" in jsonObject) and (jsonObject["return_timestamp"] > time["max_t"]):
					time["max_t"] = jsonObject["return_timestamp"]
					max_time_str = jsonObject["time_called"]

				# add json object to list of records
				bulk.append(jsonObject)
				bulk_end_index += 1

			# verify if there is any record to flush
			if len(bulk) > 0:
				if index is None:
					index = prepare_indices(es_conn, session)
				bulk, bulk_start_index, bulk_end_index = bulk_data(es_conn, bulk, bulk_size, bulk_start_index, bulk_end_index, index, session)

		logger.info("Sent %d records to ES" % bulk_end_index)

		# Creating doc of duration
		time["duration"] = time["max_t"] - time["min_t"]
		time["session_name"] = session
		time["min_t"] = min_time_str
		time["max_t"] = max_time_str
		es_conn.docIndex(index, time, "{}_{}".format(session, bulk_end_index+1))
		logger.info("Sent duration doc to ES: {}".format(time))

	except IOError:
		logger.error("could not load the provided file")


def _start():
	logger.info("DoParser Started!")

def _finish():
	logger.info("DoParser Finished!")
	exit(0)


def main():

	parser = argparse.ArgumentParser(description='Parses DIO trace file and export to ElasticSearch')
	parser.add_argument('-u', '--url',  default="http://cloud124:31111", type=str, help='elasticSearch URL')
	parser.add_argument('--session', help='session name', default=None, nargs='?')
	parser.add_argument('--size', metavar='size', default=1000, type=int, help='bulk size')
	parser.add_argument('-d', '--debug', action='store_true', default=False, help='Debug mode')
	parser.add_argument('file', help='DIO trace file', default=None, nargs='?')

	args = parser.parse_args()

	_start

	try:

		if args.debug:
			logger.setLevel("debug")

		es_conn = ESConnection(args.url)

		if args.file == None:
			logger.error("A valid file must be provided.")
			_finish()

		parse_tracer(es_conn, args.session, args.file, args.size)

	except Exception as e:

		logger.error("Got an unexpected error: %s" % e)
		traceback.print_exception(type(e), e, e.__traceback__)

	_finish()


if __name__ == '__main__':
	main()