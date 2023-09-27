import argparse
import traceback
from itertools import groupby
from utils.logging import Logger
from utils.queries import ESConnection
from multiprocessing import Process, current_process
from sys import exit

logger = Logger("FSysSeq")

def cfsysseq(args, file_paths, start_index, tid):
    try:
        if args.debug:
            global logger
            logger.setLevel("debug")
        es_conn = ESConnection(args.url)

        current_process_name = current_process().name

        sys_seqs = dict()
        bulk = []
        bulk_start_index = start_index
        bulk_end_index = start_index
        syscall_sequences_index = "criba_syscall_sequences"
        index = "criba_trace_%s" % args.session

        id = 0
        tenth = len(file_paths) // 10
        for path in file_paths:
            id += 1
            path_events = []

            path_events = es_conn.getSyscallsByPath(index, path, tid, args.size, args.maxQueries)

            sseq, count = get_hash(path_events)

            if sseq not in sys_seqs:
                sys_seqs[sseq] = 1
            else:
                sys_seqs[sseq] += 1

            sseq_compress = None
            if len(sseq) > 100:
                sseq_compress = sseq[:100] + "..."

            if not args.local:
                if len(bulk) >= 1000:
                    es_conn.bulkIndex(bulk, bulk_start_index, syscall_sequences_index, args.session+"_"+str(tid))
                    logger.info("[{}] bulking {} records ({} to {})...".format(current_process_name, len(bulk), bulk_start_index, bulk_end_index))
                    bulk_start_index = bulk_end_index
                    bulk = []

                doc = {
                    "session_name": args.session,
                    "tid": tid,
                    "file": path,
                    "repetitions": count,
                    "events": len(path_events)
                }
                if sseq_compress is not None:
                    doc["syscall_sequence_raw"] = sseq
                    doc["syscall_sequence"] =  sseq_compress
                else:
                    doc["syscall_sequence"] =  sseq

                bulk.append(doc)
                bulk_end_index += 1


            if tenth > 0 and id % tenth == 0:
                logger.debug("\t[%s] %3d sequences (%.2f%% files processed)" % (current_process_name, len(sys_seqs), float(id/len(file_paths)*100)))

        if tenth > 0:
            logger.debug("\t[%s] %3d sequences (%.2f%% files processed)" % (current_process_name, len(sys_seqs), float(id/len(file_paths)*100)))
        if len(bulk) > 0:
            es_conn.bulkIndex(bulk, bulk_start_index, syscall_sequences_index, args.session+"_"+str(tid))
            logger.info("[{}] bulking {} records ({} to {})...".format(current_process_name, len(bulk), bulk_start_index, bulk_end_index))
            bulk_start_index = bulk_end_index
            bulk = []

    except Exception as e:
        logger.error("Got an unexpected error: %s" % e)
        traceback.print_exc()
        return None

def find_duplicated_sequence(seq):
    max_w = (len(seq) // 2) + 1
    count = 0
    for w in range(2, max_w):
        count = 0
        subseq = seq[0:w]
        i = w
        while i <= (len(seq) - w):
            cursubseq = seq[i:i+w]
            if subseq == cursubseq:
                i += w
                count += 1
            else:
                break
        if count > 0 and i >= (len(seq) - w):
            return subseq, count
    return seq, 0

def get_hash(seq):
    prev_tag = None
    reduced_seq = []
    for i in range(len(seq)):
        sys = seq[i]
        if sys == "accept" or sys == "accept4":
            tag = "AC"
        elif sys == "bind":
            tag = "BD"
        elif sys == "close":
            tag = "CL"
        elif sys == "connect":
            tag = "CN"
        elif sys == "creat":
            tag = "CR"
        elif sys == "fsync" or sys == "fdatasync":
            tag = "FS"
        elif sys == "getsockopt":
            tag = "GS"
        elif sys == "getxattr" or sys == "fgetxattr" or sys == "lgetxattr":
            tag = "GX"
        elif sys == "lseek":
            tag = "LS"
        elif sys == "listen":
            tag = "LT"
        elif sys == "listxattr" or sys == "flistxattr" or sys == "llistxattr":
            tag = "LX"
        elif sys == "mknod" or sys == "mknodat":
            tag = "MK"
        elif sys == "open" or sys == "openat":
            tag = "OP"
        elif sys == "recvfrom" or sys == "recvmsg":
            tag = "RC"
        elif sys == "read" or sys == "pread64" or sys == "readv":
            tag = "RD"
        elif sys == "readahead":
            tag = "RH"
        elif sys == "readlink" or sys == "readlinkat":
            tag = "RL"
        elif sys == "rename" or sys == "renameat" or sys == "renameat2":
            tag = "RN"
        elif sys == "removexattr" or sys == "fremovexattr" or sys == "lremovexattr":
            tag = "RX"
        elif sys == "sendto" or sys == "sendmsg":
            tag = "SD"
        elif sys == "socket" or sys == "socketpair":
            tag = "SK"
        elif sys == "setsockopt":
            tag = "SS"
        elif sys == "stat" or sys == "lstat" or sys == "fstat" or sys == "fstatfs" or sys == "fstatat":
            tag = "ST"
        elif sys == "setxattr" or sys == "fsetxattr" or sys == "lsetxattr":
            tag = "SX"
        elif sys == "truncate" or sys == "ftruncate":
            tag = "TR"
        elif sys == "unlink" or sys == "unlinkat":
            tag = "UN"
        elif sys == "write" or sys == "pwrite64" or sys == "writev":
            tag = "WR"

        if prev_tag is None or prev_tag != tag:
            reduced_seq.append(tag)
        prev_tag = tag

    newseq, count = find_duplicated_sequence(reduced_seq)
    funiqseq_str = "->".join(newseq)

    return funiqseq_str, count

def compute_file_syscall_sequences(args, file_paths, tid):
    logger.info("Computing files syscall sequences...")

    jobs = []

    rel_len = len(file_paths)
    half = rel_len // args.nProc

    pos = 0
    for i in range(args.nProc):
        len_rel = len(file_paths)
        if len_rel < half:
            half = len_rel
        elif i == args.nProc - 1:
            half = len_rel
        paths_handling = file_paths[:half]
        file_paths = file_paths[half:]

        logger.info("Creating Process-%d to handle paths from %8d to %8d (%8d)" % (i+1, pos, pos+len(paths_handling), len(paths_handling)))
        process = Process(
            target=cfsysseq,
            args=(args, paths_handling, pos, tid)
        )
        jobs.append(process)
        pos = pos + len(paths_handling)

    for j in jobs:
        logger.info("Process %s started!" % j.name)
        j.start()

    for j in jobs:
        j.join()
        logger.info("Process %s finished!" % j.name)

def _print_args(args):
    logger.debug("Arguments:")
    for arg in vars(args):
        logger.debug("\t%s: %s" % (arg, getattr(args, arg)))

def _start():
    logger.info("FSysSeq Started!")

def _finish():
    logger.info("FSysSeq Finished!")
    exit(0)

def main():

    parser = argparse.ArgumentParser(description='Search for file syscalls sequences in ElasticSearch.')
    parser.add_argument('-u', '--url',  default="http://cloud124:31111", type=str, help='elasticSearch URL')
    parser.add_argument('-p', '--nProc', default=1, type=int,)
    parser.add_argument('-sz', '--size', default=10000, type=int, help='size of elasticsearch query')
    parser.add_argument('-mq', '--maxQueries', default=1000, type=int, help='max queries to ES')
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
            _print_args(args)

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

        logger.info("Getting TIDs from index %s..." % index)
        tids, exec_time = es_conn.getTIDsInIndex(index)
        logger.info("Got %s TIDs in %d ms." % (len(tids), exec_time))

        for tid in tids:
            logger.info("Getting file paths from index %s and TID %s..." % (index, tid))
            query = {
                "bool": {
                    "must": [
                        { "exists": { "field": "fdata.file_path" }},
                        { "match": { "tid": tid } },
                    ]
                }
            }
            filePaths, exec_time = es_conn.getFilePaths(index, args.size, query=query)
            logger.info("Got %s paths in %d ms." % (len(filePaths), exec_time))

            compute_file_syscall_sequences(args, filePaths, tid)

    except Exception as e:

        logger.error("Got an unexpected error: %s" % e)
        traceback.print_exception(type(e), e, e.__traceback__)

    _finish()

if __name__ == "__main__":
    main()