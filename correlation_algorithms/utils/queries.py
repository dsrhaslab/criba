from elasticsearch import Elasticsearch
from elasticsearch.client import IngestClient

class ESConnection:
    def __init__(self, url):
        # Initialize Elasticsearch connection
        self.es_conn = Elasticsearch([url], basic_auth=('dio', 'diopw'))

    def createNewDIOTracerIndex(self, index):
        # Create index if it doesn't exist
        mappings={
			"properties": {
				"time_called": { "type": "date_nanos" },
				"time_returned": { "type": "date_nanos" }
			}
		}
        self.es_conn.indices.create(index=index, mappings=mappings, ignore=400)

    def createNewIndex(self, index):
        # Create index if it doesn't exist
        print("Creating index: " + index)
        res = self.es_conn.indices.create(index=index, ignore=400)

    def createDIOIngestPipeleine(self):
        p = IngestClient(self.es_conn)
        pipeline = {
            "description": "Split system calls into different indexes",
            "processors": [
                {
                    "set": {
                        "field": "_index",
                        "value": "{{{ _index }}}-paths",
                        "if": "if ( ctx.doc_type == \"EventPath\") { return true; }",
                        "ignore_failure": True
                    }
                }
            ]
        }
        p.put_pipeline(id='split-events-pipeline', body=pipeline)

    def docIndex(self, index, event, id):
        # Index a document
        self.es_conn.index(index=index, document=event, id=id, request_timeout=300)

    def bulkIndex(self, records, begin_idx, index, session=None, pipeline=None):
        # Index a bulk of documents
        bulkArr = []
        for json in records:
            if session is not None:
                id = "{}_{}".format(session, begin_idx)
            else:
                id = begin_idx
            bulkArr.append({'index': {'_id': "{}".format(id)}})
            bulkArr.append(json)
            begin_idx = begin_idx + 1
        res = self.es_conn.bulk(index = index, body=bulkArr, pipeline=pipeline)
        # print(res)
        errors = {}
        for val in res["items"]:
            if "error" in val["index"]:
                if val["index"]["error"]["reason"] not in errors:
                    errors[val["index"]["error"]["reason"]] = 1
                else:
                    errors[val["index"]["error"]["reason"]] += 1
        return errors, res["took"]

    def updateByQuery(self, index, query, request_timeout=60):
        # Update documents by query
        return self.es_conn.update_by_query(index=index, body=query, request_timeout=request_timeout)

    def getSessions(self, size=1000):
        # Get all session_name values
        index = "criba_trace*"
        aggs_body = {
            "sessions": {
                "terms": {
                    "field": "session_name.keyword",
                    "size": size
                }
            }
        }
        res = self.es_conn.search(index=index, aggs=aggs_body, size=0)
        took = res['took']
        sessions_buckets = res['aggregations']['sessions']['buckets']
        sessions = dict()
        for bucket in sessions_buckets:
            sessions[bucket["key"]] = bucket["doc_count"]
        return sessions, took

    def getRelativePathsPartial(self, index, size, after_key=None, timeout=60):
        # Get relative_file_path values after a given key
        query = {
            "bool": {
                "must_not": [
                    {"exists": {"field": "file_tag"}},
                    {"exists": {"field": "fdata.relative_file_path"}}
                ],
                "must": [ { "exists": { "field": "fdata.file_path" }} ]
            }
        }
        if after_key:
            aggs_body = {
                "my_buckets": {
                    "composite": {
                        "size": size,
                        "sources": [ { "paths": { "terms": { "field": "fdata.file_path.keyword" } } } ],
                        "after": {"paths": after_key}
                    }
                }
            }
        else:
            aggs_body = {
                "my_buckets": {
                    "composite": {
                        "size": size,
                        "sources": [ { "paths": { "terms": { "field": "fdata.file_path.keyword" } } } ]
                    }
                }
            }

        res =  self.es_conn.search(index=index, query=query, aggs=aggs_body, size=0, track_total_hits=False, request_timeout=timeout)
        fpath_buckets = [bucket["key"]["paths"] for bucket in res["aggregations"]["my_buckets"]["buckets"]]
        took = res['took']
        if len(fpath_buckets) == 0:
            after_key = None
        else:
            after_key = res["aggregations"]["my_buckets"]["after_key"]["paths"]
        return fpath_buckets, took, after_key

    def getRelativeFilePaths(self, index, size, timeout=60):
        # Get all relative_file_path values
        fpaths = []
        after_key = None
        exec_time = 0
        while True:
            fpath_buckets, took, after_key = self.getRelativePathsPartial(index, size, after_key=after_key, timeout=timeout)
            exec_time += took
            if len(fpath_buckets) == 0:
                break
            fpaths.extend(fpath_buckets)
        return fpaths, exec_time

    def updateRelativePaths(self, index, rel_path, absolute_path, timeout=60):
        # Update relative_file_path values
        es_query_body = {
            "query": {
                "bool": {
                    "must_not": [
                        {"exists": {"field": "file_tag"}},
                        {"exists": {"field": "fdata.relative_file_path"}}
                    ],
                    "must": [ {"match": { "fdata.file_path.keyword": rel_path }} ]
                }
            },
            "script": {
                "lang": "painless",
                "source": """
                    ctx._source.fdata.relative_file_path = ctx._source.fdata.file_path;
                    ctx._source.fdata.file_path = params.fpath;
                """,
                "params": {
                    "fpath": absolute_path
                }
            }
        }
        self.updateByQuery(index, es_query_body, timeout)

    def countUniquePaths(self, index, size, timeout=60):
        # Get unique file extensions
        es_query_body = {
            "aggs": {
                "Distinct_Count": {
                "scripted_metric": {
                    "params": {
                    "fieldName": "file_path"
                    },
                    "init_script": "state.list = []",
                    "map_script": """
                    if(params._source.fdata != null && params._source.fdata[params.fieldName] != null)
                        state.list.add(params._source.fdata[params.fieldName]);
                    """,
                    "combine_script": "return state.list;",
                    "reduce_script": """
                    Map uniqueValueMap = new HashMap();
                    int count = 0;
                    for(shardList in states) {
                    if(shardList != null) {
                        for(key in shardList) {
                        if(!uniqueValueMap.containsKey(key)) {
                            count +=1;
                            uniqueValueMap.put(key, key);
                        }
                        }
                    }
                    }
                    return count;
                    """
                }
                }
            }
        }
        res = self.es_conn.search(index=index, body=es_query_body, size=0, request_timeout=timeout)
        count = res["aggregations"]["Distinct_Count"]["value"]
        return count, res['took']

    def countUniqueFileNames(self, index, size, timeout=60):
        # Get unique file extensions
        es_query_body = {
            "aggs": {
                "Distinct_Count": {
                "scripted_metric": {
                    "params": {
                    "fieldName": "file_name"
                    },
                    "init_script": "state.list = []",
                    "map_script": """
                    if(params._source.fdata != null && params._source.fdata[params.fieldName] != null)
                        state.list.add(params._source.fdata[params.fieldName]);
                    """,
                    "combine_script": "return state.list;",
                    "reduce_script": """
                    Map uniqueValueMap = new HashMap();
                    int count = 0;
                    for(shardList in states) {
                    if(shardList != null) {
                        for(key in shardList) {
                        if(!uniqueValueMap.containsKey(key)) {
                            count +=1;
                            uniqueValueMap.put(key, key);
                        }
                        }
                    }
                    }
                    return count;
                    """
                }
                }
            }
        }
        res = self.es_conn.search(index=index, body=es_query_body, size=0, request_timeout=timeout)
        count = res["aggregations"]["Distinct_Count"]["value"]
        return count, res['took']

    def countUniqueExtensions(self, index, size, timeout=60):
        # Get unique file extensions
        es_query_body = {
            "aggs": {
                "Distinct_Count": {
                "scripted_metric": {
                    "params": {
                    "fieldName": "file_extension"
                    },
                    "init_script": "state.list = []",
                    "map_script": """
                    if(params._source.fdata != null && params._source.fdata[params.fieldName] != null)
                    for(val in params._source.fdata[params.fieldName]) {
                        state.list.add(val);
                    }
                    """,
                    "combine_script": "return state.list;",
                    "reduce_script": """
                    Map uniqueValueMap = new HashMap();
                    int count = 0;
                    for(shardList in states) {
                    if(shardList != null) {
                        for(key in shardList) {
                        if(!uniqueValueMap.containsKey(key)) {
                            count +=1;
                            uniqueValueMap.put(key, key);
                        }
                        }
                    }
                    }
                    return count;
                    """
                }
                }
            }
        }
        res = self.es_conn.search(index=index, body=es_query_body, size=0, request_timeout=timeout)
        count = res["aggregations"]["Distinct_Count"]["value"]
        return count, res['took']

    # def countWrittenBytesPerFile(self, index, size, timeout=60):
    #     # Get unique file extensions
    #     es_query_body = {
    #         "aggs": {
    #             "Distinct_Count": {
    #             "scripted_metric": {
    #                 "params": {
    #                     "fieldName": "file_path"
    #                 },
    #                 "init_script": "uniqueValueMap = new HashMap()",
    #                 "map_script": """
    #                     if(params._source.fdata != null && params._source.fdata[params.fieldName] != null) {
    #                         if (!uniqueValueMap.containsKey(params._source.fdata[params.fieldName])) {
    #                             uniqueValueMap.put(params._source.fdata[params.fieldName], params._source.return_value);
    #                         } else {
    #                             uniqueValueMap.put(params._source.fdata[params.fieldName], uniqueValueMap.get(params._source.fdata[params.fieldName]) + params._source.return_value);
    #                         }
    #                     }
    #                 """,
    #                 "combine_script": "return state.list;",
    #                 "reduce_script": """
    #                 Map uniqueValueMap = new HashMap();
    #                 int count = 0;
    #                 for(shardList in states) {
    #                     if(shardList != null) {
    #                         for(key in shardList) {
    #                             if(!uniqueValueMap.containsKey(key)) {
    #                                 count +=1;
    #                                 uniqueValueMap.put(key, key);
    #                             }
    #                         }
    #                     }
    #                 }
    #                 return count;
    #                 """
    #             }
    #             }
    #         }
    #     }
    #     res = self.es_conn.search(index=index, body=es_query_body, size=0, request_timeout=timeout)
    #     count = res["aggregations"]["Distinct_Count"]["value"]
    #     return count, res['took']

    def fixFilesPaths(self, index, old_sufix, new_sufix, timeout=60):
        # Update relative_file_path values
        es_query_body_main = {
            "query": {
                "bool": {
                    "must": [
                        {"exists": {"field": "fdata.file_path"}}
                    ]
                }
            },
            "script": {
                "lang": "painless",
                "source": """
                    if (ctx._source.fdata.file_path.indexOf(params.old_sufix) == 0) {
                        ctx._source.fdata.file_path = ctx._source.fdata.file_path.replace(params.old_sufix,params.new_sufix);
                    }
                """,
                "params": {
                    "old_sufix": old_sufix,
                    "new_sufix": new_sufix
                }
            }
        }
        es_query_body_paths = {
            "query": {
                "bool": {
                    "must": [
                        {"exists": {"field": "file_path"}}
                    ]
                }
            },
            "script": {
                "lang": "painless",
                "source": """
                    if (ctx._source.file_path.indexOf(params.old_sufix) == 0) {
                        ctx._source.file_path = ctx._source.file_path.replace(params.old_sufix,params.new_sufix);
                    }
                """,
                "params": {
                    "old_sufix": old_sufix,
                    "new_sufix": new_sufix
                }
            }
        }
        self.updateByQuery(index, es_query_body_main, timeout)
        self.updateByQuery(index+"-paths", es_query_body_paths, timeout)

    def getFilePathsPartial(self, index, size, query=None, after_key=None, timeout=60):
        # Get file_path values after a given key
        aggs_body = {
            "my_buckets": {
                "composite": {
                    "size": size,
                    "sources": [ { "paths": { "terms": { "field": "fdata.file_path.keyword" } } } ]
                }
            }
        }

        if after_key:
            aggs_body["my_buckets"]["composite"]["after"] = {"paths": after_key}

        res =  self.es_conn.search(index=index, aggs=aggs_body, size=0, query=query, track_total_hits=False, request_timeout=timeout)
        fpath_buckets = [bucket["key"]["paths"] for bucket in res["aggregations"]["my_buckets"]["buckets"]]
        if len(fpath_buckets) == 0:
            after_key = None
        else:
            after_key = res["aggregations"]["my_buckets"]["after_key"]["paths"]
        took = res['took']
        return fpath_buckets, took, after_key

    def getFilePaths(self, index, size, query=None, timeout=60):
        # Get all file_path values
        fpaths = []
        after_key = None
        exec_time = 0
        i=0
        while True:
            i+=1
            fpath_buckets, took, after_key = self.getFilePathsPartial(index, size, query=query, after_key=after_key, timeout=timeout)
            exec_time += took
            if len(fpath_buckets) == 0:
                break
            fpaths.extend(fpath_buckets)
        return fpaths, exec_time

    def getEventsByPathAfterTimestamp(self, index, path, size, min_timestamp, tid=None):
        # Get events for a given path after a given timestamp
        es_query_body = {
            "bool": {
                "must": [
                    { "match": { "fdata.file_path.keyword": path } },
                    { "range": { "return_timestamp": { "gt": min_timestamp } } }
                ]
            }
        }
        if tid:
            es_query_body["bool"]["must"].append({ "match": { "tid": tid } })
        return self.es_conn.search(index=index, query=es_query_body, sort='return_timestamp:asc', size=size)

    def getSyscallsByPath(self, index, path, tid, size=1000, max_queries=100):
        # Get all syscalls for a given path
        syscalls =  []
        n_queries = 0
        min_timestamp = 0
        stop_while = False
        while(not stop_while and n_queries < max_queries):
            resp = self.getEventsByPathAfterTimestamp(index, path, size, min_timestamp, tid=tid)
            if len(resp['hits']['hits']) == 0:
                stop_while = True
            for hit in resp['hits']['hits']:
                syscall = "%(system_call_name)s" % hit["_source"]
                timestamp = hit["_source"]["return_timestamp"]
                if timestamp > min_timestamp:
                    min_timestamp = timestamp
                syscalls.append(syscall)
            n_queries += 1

        return syscalls

    def getTermEventsAfterTimestamp(self, index, term, size, min_timestamp, tid=None):
        # Get events with a given term after a given timestamp
        es_query_body = {
            "bool": {
                "must": [
                    { "exists": { "field": term } },
                    { "range": { "return_timestamp": { "gt": min_timestamp } } }
                ]
            }
        }
        if tid:
            es_query_body["bool"]["must"].append({ "match": { "tid": tid } })
        return self.es_conn.search(index=index, query=es_query_body, sort='return_timestamp:asc', size=size)

    def getSyscallsEvents(self, index, size=1000, max_queries=100):
        # Get all syscalls events
        syscalls =  []
        n_queries = 0
        min_timestamp = 0
        stop_while = False
        exec_time = 0
        while(not stop_while and n_queries < max_queries):
            resp = self.getTermEventsAfterTimestamp(index, "system_call_name", size, min_timestamp)
            took = resp['took']
            exec_time += took
            if len(resp['hits']['hits']) == 0:
                stop_while = True
            for hit in resp['hits']['hits']:
                syscall = "%(system_call_name)s" % hit["_source"]
                timestamp = hit["_source"]["return_timestamp"]
                if timestamp > min_timestamp:
                    min_timestamp = timestamp
                syscalls.append(syscall)
            n_queries += 1

        return syscalls, exec_time

    def getSyscallsAndPathsEvents(self, index, size=1000, max_queries=100, tid=None):
        # Get all syscalls events
        res =  []
        n_queries = 0
        min_timestamp = 0
        stop_while = False
        exec_time = 0
        while(not stop_while and n_queries < max_queries):
            resp = self.getTermEventsAfterTimestamp(index, "system_call_name", size, min_timestamp, tid=tid)
            took = resp['took']
            exec_time += took
            if len(resp['hits']['hits']) == 0:
                stop_while = True
            for hit in resp['hits']['hits']:
                syscall = "%(system_call_name)s" % hit["_source"]
                if "fdata" not in hit["_source"]:
                    continue
                timestamp = hit["_source"]["return_timestamp"]
                if timestamp > min_timestamp:
                    min_timestamp = timestamp
                res.append((syscall, hit["_source"]["fdata"]["file_path"]))
            n_queries += 1

        return res, exec_time

    def getFNamesAndExtensionsEvents(self, index, size=1000, max_queries=100):
        # Get all file name and extensions events
        fextensions =  []
        fnames = []
        n_queries = 0
        min_timestamp = 0
        stop_while = False
        while(not stop_while and n_queries < max_queries):
            resp = self.getTermEventsAfterTimestamp(index, "fdata.file_name", size, min_timestamp)
            if len(resp['hits']['hits']) == 0:
                stop_while = True
            for hit in resp['hits']['hits']:
                file_extension = hit["_source"]["fdata"]["file_extension"]
                file_name = hit["_source"]["fdata"]["file_name"]
                timestamp = hit["_source"]["return_timestamp"]
                if timestamp > min_timestamp:
                    min_timestamp = timestamp
                fextensions.append(file_extension)
                fnames.append(file_name)
            n_queries += 1

        return fextensions, fnames

    def getCommandsInIndex(self, index, size=1000, timeout=60):
        # Get all commands in an index
        aggs_body = {
            "commands": {
                "terms": {
                    "field": "comm.keyword",
                    "size": size
                }
            }
        }
        res = self.es_conn.search(index=index, aggs=aggs_body, size=0, request_timeout=timeout)
        took = res['took']
        commands_buckets = res['aggregations']['commands']['buckets']
        commands = dict()
        for bucket in commands_buckets:
            commands[bucket["key"]] = bucket["doc_count"]
        return commands, took

    def getFileExtensionsInIndex(self, index, size, timeout=60):
        # Get all file extensions in an index
        aggs_body = {
            "fextensions": {
                "terms": {
                    "field": "fdata.file_extension.keyword",
                    "size": size
                }
            }
        }
        res = self.es_conn.search(index=index, aggs=aggs_body, size=0, request_timeout=timeout)
        took = res['took']
        fextensions = res['aggregations']['fextensions']['buckets']
        return fextensions, took

    def getFileTagsInIndex(self, index, size, timeout=60):
        aggs_body = {
            "ftags": {
                "terms": {
                    "field": "file_tag.keyword",
                    "size": size
                }
            }
        }
        res = self.es_conn.search(index=index, aggs=aggs_body, size=0, request_timeout=timeout)
        took = res['took']
        ftags = res['aggregations']['ftags']['buckets']
        return ftags, took


    def getSyscallsTypesInPath(self, index, size, path, timeout=60):
        query_body = {
            "bool": {
                "must": [
                    { "exists": { "field": "fdata.file_path.keyword" } },
                    { "match": { "fdata.file_path.keyword": path } }
                ]
            }
        }
        aggs_body = {
            "syscalls": {
                "terms": {
                    "field": "system_call_name.keyword",
                    "size": size
                }
            }
        }
        res = self.es_conn.search(index=index, aggs=aggs_body, query=query_body, size=size, request_timeout=timeout)
        took = res['took']
        syscalls = []
        ftags = res['aggregations']['syscalls']['buckets']
        for syscall in ftags:
            syscalls.append(syscall["key"])
        return syscalls, took

    def getAbsolutePathsPartial(self, index, size, after_key=None):
        # Get absolute paths after a given key
        if after_key:
            aggs_body = {
                "my_buckets": {
                    "composite": {
                        "size": size,
                        "sources": [ { "paths": { "terms": { "field": "file_path.keyword" } } } ],
                        "after": {"paths": after_key}
                    }
                }
            }
        else:
            aggs_body = {
                "my_buckets": {
                    "composite": {
                        "size": size,
                        "sources": [ { "paths": { "terms": { "field": "file_path.keyword" } } } ]
                    }
                }
            }
        res = self.es_conn.search(index=index, aggs=aggs_body, size=0, track_total_hits=False)
        fpath_buckets = [bucket["key"]["paths"] for bucket in res["aggregations"]["my_buckets"]["buckets"]]
        took = res['took']
        if len(fpath_buckets) == 0:
            after_key = None
        else:
            after_key = res["aggregations"]["my_buckets"]["after_key"]["paths"]
        return fpath_buckets, took, after_key

    def getAbsoluteFilePaths(self, index, size):
        # Get all absolute file paths
        fpaths = []
        after_key = None
        exec_time = 0
        while True:
            fpath_buckets, took, after_key = self.getAbsolutePathsPartial(index+"-paths", size, after_key=after_key)
            exec_time += took
            if len(fpath_buckets) == 0:
                break
            fpaths.extend(fpath_buckets)
        return fpaths, exec_time

    def updateFileNamesAndExtensions(self, index, timeout=60):
        # Update file names and extensions
        es_query_body = {
            "query": {
                "bool": {
                    "must_not": [
                        {"exists": {"field": "fdata.file_extension"}},
                        {"exists": {"field": "fdata.file_name"}}
                    ],
                    "must": [ {"exists": { "field": "fdata.file_path.keyword" }} ]
                }
            },
            "script": {
                "lang": "painless",
                "source": """
                    def extensions = new ArrayList();
                    if (ctx._source.fdata != null) {
                        def temp=ctx._source.fdata.file_path;
                        def items= temp.splitOnToken('/');
                        def count = items.length;

                        if (count > 1) {
                            ctx._source.fdata.file_name = items[count-1];
                            def last = items[count-1];
                            def exts = last.splitOnToken('.');
                            def count2 = exts.length;
                            if (count2 > 1) {
                                def ext = "";
                                for (def i = 1; i < count2; i++) {
                                    ext = ext + "." + exts[i];
                                }
                                ext = "." + exts[count2-1];
                                extensions.add(ext);
                            } else {
                                extensions.add("");
                            }
                        } else {
                            extensions.add("");
                        }
                        ctx._source.fdata.file_extension = extensions;
                    }

                    if (ctx._source.args != null && ctx._source.args.newname != null) {
                        def temp =ctx._source.args.newname;
                        def items = temp.splitOnToken('/');
                        def count = items.length;

                        if (count > 1) {
                            ctx._source.args.new_file_name = items[count-1];
                            def last = items[count-1];
                            def exts = last.splitOnToken('.');
                            def count2 = exts.length;
                            if (count2 > 1) {
                                def ext = "";
                                for (def i = 1; i < count2; i++) {
                                    ext = ext + "." + exts[i];
                                }
                                ext = "." + exts[count2-1];
                                extensions.add(ext);
                            } else {
                                extensions.add("");
                            }
                        } else {
                            extensions.add("");
                        }
                        ctx._source.fdata.file_extension = extensions;
                    }
                """
            }
        }
        return self.updateByQuery(index, es_query_body, timeout)

    def getTIDsInIndex(self, index, size=100, timeout=60):
        # Get all TIDs in an index
        aggs_body = {
            "tids": {
                "terms": {
                    "field": "tid",
                    "size": size
                }
            }
        }
        res = self.es_conn.search(index=index, aggs=aggs_body, size=0, request_timeout=timeout)
        took = res['took']
        tids = {}
        for val in res['aggregations']['tids']['buckets']:
            tids[val['key']] = val['doc_count']

        return tids, took

    def getPIDsInIndex(self, index, size=100, timeout=60):
        # Get all TIDs in an index
        aggs_body = {
            "pids": {
                "terms": {
                    "field": "pid",
                    "size": size
                }
            }
        }
        res = self.es_conn.search(index=index, aggs=aggs_body, size=0, request_timeout=timeout)
        took = res['took']
        tids = {}
        for val in res['aggregations']['pids']['buckets']:
            tids[val['key']] = val['doc_count']

        return tids, took

    def getFileTypesInIndex(self, index, size=100, timeout=60):
        # Get all TIDs in an index
        aggs_body = {
            "ftypes": {
                "terms": {
                    "field": "fdata.file_type.keyword",
                    "size": size
                }
            }
        }
        res = self.es_conn.search(index=index, aggs=aggs_body, size=0, request_timeout=timeout)
        took = res['took']
        ftypes = {}
        for val in res['aggregations']['ftypes']['buckets']:
            ftypes[val['key']] = val['doc_count']

        return ftypes, took

    def getOpenPathsAfterTimestamp(self, index, tid=None, size=1000, min_timestamp=0):
        # Get all open paths after a timestamp
        if tid == None:
            es_query_body = {
                "bool": {
                    "must": [
                        { "match": { "system_call_name.keyword": "openat" } },
                        { "exists": { "field": "fdata.file_path" }},
                        { "range": { "return_timestamp": { "gt": min_timestamp } } }
                    ]
                }
            }
        else:
            es_query_body = {
                "bool": {
                    "must": [
                        { "match": { "tid": tid } },
                        { "match": { "system_call_name.keyword": "openat" } },
                        { "exists": { "field": "fdata.file_path" }},
                        { "range": { "return_timestamp": { "gt": min_timestamp } } }
                    ]
                }
            }
        return self.es_conn.search(index=index, query=es_query_body, source=["fdata.file_path","system_call_name"], sort='return_timestamp:asc', size=size)

    def getOpenPaths(self, index, tid, size, max_queries):
        # Get all open paths
        paths =  []
        n_queries = 0
        min_timestamp = 0
        stop_while = False
        while(not stop_while and n_queries < max_queries):
            resp = self.getOpenPathsAfterTimestamp(index, tid, size, min_timestamp)
            if len(resp['hits']['hits']) == 0:
                stop_while = True
            for hit in resp['hits']['hits']:
                path = hit["_source"]["fdata"]["file_path"]
                timestamp = hit["sort"][0]
                if timestamp > min_timestamp:
                    min_timestamp = timestamp
                paths.append(path)
            n_queries += 1
        return paths

    def getFilesEventsAfterTimestamp(self, index, size=1000, min_timestamp=0):
        # Get all open paths after a timestamp
        es_query_body = {
            "bool": {
                "must": [
                    { "exists": { "field": "fdata.file_path" }},
                    { "range": { "return_timestamp": { "gt": min_timestamp } } }
                ]
            }
        }
        return self.es_conn.search(index=index, query=es_query_body, source=["fdata.file_path"], sort='return_timestamp:asc', size=size)

    def getFilesEventsSorted(self, index, size, max_queries):
        # Get all open paths
        paths =  []
        n_queries = 0
        min_timestamp = 0
        stop_while = False
        while(not stop_while and n_queries < max_queries):
            resp = self.getFilesEventsAfterTimestamp(index, size, min_timestamp)
            if len(resp['hits']['hits']) == 0:
                stop_while = True
            for hit in resp['hits']['hits']:
                path = hit["_source"]["fdata"]["file_path"]
                timestamp = hit["sort"][0]
                if timestamp > min_timestamp:
                    min_timestamp = timestamp
                paths.append(path)
            n_queries += 1

        return paths

    def getTIDFilesEventsAfterTimestamp(self, index, tid, size=1000, min_timestamp=0):
        # Get all open paths after a timestamp
        es_query_body = {
            "bool": {
                "must": [
                    # { "exists": { "field": "fdata.file_name" }},
                    { "exists": { "field": "fdata.file_path" }},
                    { "match": { "tid": tid } },
                    { "range": { "return_timestamp": { "gt": min_timestamp } } }
                ]
            }
        }
        # return self.es_conn.search(index=index, query=es_query_body, source=["fdata.file_name"], sort='return_timestamp:asc', size=size)
        return self.es_conn.search(index=index, query=es_query_body, source=["fdata.file_path"], sort='return_timestamp:asc', size=size)

    def getTIDFilesEventsSorted(self, index, tid, size, max_queries):
        # Get all open paths
        paths =  []
        n_queries = 0
        min_timestamp = 0
        stop_while = False
        while(not stop_while and n_queries < max_queries):
            resp = self.getTIDFilesEventsAfterTimestamp(index, tid, size, min_timestamp)
            if len(resp['hits']['hits']) == 0:
                stop_while = True
            for hit in resp['hits']['hits']:
                # path = hit["_source"]["fdata"]["file_name"]
                path = hit["_source"]["fdata"]["file_path"]
                timestamp = hit["sort"][0]
                if timestamp > min_timestamp:
                    min_timestamp = timestamp
                paths.append(path)
            n_queries += 1

        return paths


    def getNoUniquePaths(self, index, field):
        aggs_body = {
            "unique_paths": {
                "cardinality": {
                    "field": field
                }
            }
        }
        res = self.es_conn.search(index=index, query=None, aggs=aggs_body, size=0)
        return res["aggregations"]["unique_paths"]["value"], res["took"]