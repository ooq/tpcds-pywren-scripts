import pywren
import boto3
import md5
import numpy as np
import random
import os

if __name__ == "__main__":
    import logging
    import subprocess
    import gc
    import time
    def run_command(key):
        pywren.wrenlogging.default_config()
        logger = logging.getLogger(__name__)

        m = md5.new()
        genpart = str(random.choice(range(50)))
        m.update(genpart)
        genfile = 'tools/' + m.hexdigest()[:8] + "-dsdgen-" + genpart
        idxfile = 'tools/' + m.hexdigest()[:8] + "-idx-" + genpart
        client = boto3.client('s3', 'us-west-2')
        client.download_file('qifan-public', genfile, '/tmp/condaruntime/dsdgen')
        client.download_file('qifan-public', idxfile, '/tmp/condaruntime/tpcds.idx')
        res = subprocess.check_output(["chmod",
                                        "a+x",
                                        "/tmp/condaruntime/dsdgen"])

        for i in range(0,5):
            table = key['table']
            start_index = key['start_index']
            total = key['total']
            scale = key['scale']
            index = start_index + i
            if index  > total:
                return "good" 
            
            if total == 1:
                data = subprocess.check_output(["/tmp/condaruntime/dsdgen",
                                            "-table", table,
                                            "-scale", str(scale),
                                            "-force",
                                            "-suffix", ".csv",
                                            "-distribution", "/tmp/condaruntime/tpcds.idx",
                                            "-dir", "/tmp/condaruntime/"])
            else:         
                command =  ["/tmp/condaruntime/dsdgen",
                                            "-table", table,
                                            "-scale", str(scale),
                                            "-force",
                                            "-suffix", ".csv",
                                            "-parallel", str(total),
                                            "-child", str(index),
                                            "-distribution", "/tmp/condaruntime/tpcds.idx",
                                            "-dir", "/tmp/condaruntime/"]
                print(" ".join(command))
                data = subprocess.check_output(command)
            filename = table + "_" + str(index) + "_" + str(total) + ".csv"
            fullpathfile = "/tmp/condaruntime/" + filename
            if total == 1:
                srcfullpath = "/tmp/condaruntime/" + table + ".csv" 
                res = subprocess.check_output(["mv", srcfullpath, fullpathfile])
            if not os.path.isfile(fullpathfile):
                return "bad" 
            keyname = "scale" + str(scale) + "/" + table + "/" + filename
            put_start = time.time() 
            client.upload_file(fullpathfile, "qifan-tpcds-data", keyname)
            put_end = time.time()
            res = subprocess.check_output(["rm", fullpathfile])
            if "sales" in table:
                return_table = table.split("_")[0] + "_returns"
                return_filename = return_table + "_" + str(index) + "_" + str(total) + ".csv"
                return_fullpathfile = "/tmp/condaruntime/" + return_filename
                if total == 1:
                    src_return_fullpath = "/tmp/condaruntime/" + return_table + ".csv"
                    res = subprocess.check_output(["mv", src_return_fullpath, return_fullpathfile])
                return_keyname = "scale" + str(scale) + "/" + return_table + "/" + return_filename
                client.upload_file(return_fullpathfile, "qifan-tpcds-data", return_keyname)
                res = subprocess.check_output(["rm", return_fullpathfile])
            logger.info(str(index) + " th object uploaded using " + str(put_end-put_start) + " seconds.")
        return "good"
    wrenexec = pywren.default_executor(shard_runtime=True)
    #fp = open("../finished_calls.txt","r")
    #finished_calls = []
    #data = fp.readline()
    #while data:
    #    finished_calls.append(int(data))
    #    data = fp.readline()
    #print(str(len(finished_calls)))
    #tasks = range(20,1000000,5)
    #unfinished_calls = list(set(range(len(tasks))).difference(set(finished_calls)))
    #unfinished_tasks = list(np.array(tasks)[unfinished_calls])
    #print(str(len(unfinished_tasks)))
    #passed_tasks = []
    #for iii in unfinished_tasks:
    #    passed_tasks.append(int(iii))
    #passed_tasks = range(0,1000000,5)
    #passed_tasks = range(0,10000,5)
    #passed_tasks = range(0,1000000,5)
    #passed_tasks = range(1)
    tables_1000 = [("call_center",1),
    ("catalog_page",1),
    ("catalog_sales",3614),
    ("customer",18),
    ("customer_address",8),
    ("customer_demographics",1),
    ("date_dim",1),
    ("household_demographics",1),
    ("income_band",1),
    ("inventory",140),
    ("item",1),
    ("promotion",1),
    ("reason",1),
    ("ship_mode",1),
    ("store",1),
    ("store_sales",5248),
    ("time_dim",1),
    ("warehouse",1),
    ("web_page",1),
    ("web_sales",1808),
    ("web_site",1)]
    tables_100 = [("call_center",1),
    ("catalog_page",1),
    ("catalog_sales",322),
    ("customer",3),
    ("customer_address",1),
    ("customer_demographics",1),
    ("date_dim",1),
    ("household_demographics",1),
    ("income_band",1),
    ("inventory",90),
    ("item",1),
    ("promotion",1),
    ("reason",1),
    ("ship_mode",1),
    ("store",1),
    ("store_sales",433),
    ("time_dim",1),
    ("warehouse",1),
    ("web_page",1),
    ("web_sales",166),
    ("web_site",1)]
    tables_10 = [("call_center",1),
    ("catalog_page",1),
    ("catalog_sales",33),
    ("customer",1),
    ("customer_address",1),
    ("customer_demographics",1),
    ("date_dim",1),
    ("household_demographics",1),
    ("income_band",1),
    ("inventory",27),
    ("item",1),
    ("promotion",1),
    ("reason",1),
    ("ship_mode",1),
    ("store",1),
    ("store_sales",44),
    ("time_dim",1),
    ("warehouse",1),
    ("web_page",1),
    ("web_sales",109),
    ("web_site",1)]
    all_tables = {10:tables_10, 100: tables_100, 1000: tables_1000}

    scale = 100
    passed_tasks = [] 
    for (table, total) in all_tables[scale]:
        if table is not "web_sales":
            continue
        print table + " " + str(total)
        #if table is not "web_sales":
        #    continue
        for i in range(1, total+1, 5):
            key = {} 
            key['total'] = total
            key['scale'] = scale
            key['table'] = table
            key['start_index'] = i
            passed_tasks.append(key)
        #for key in passed_tasks:
        #    print(run_command(key))
        #continueZ
    run_command(passed_tasks[0])
    exit(0)
    for task in passed_tasks:
        print(run_command(task))
    exit(0)
    fut = wrenexec.map_sync_with_rate_and_retries(run_command, passed_tasks, rate=300)

    pywren.wait(fut)
    res = [f.result() for f in fut]
    print "good:" + str(res.count("good")) + " bad:" + str(res.count("bad")) + " total:" + str(len(res))
    #exit(0)
