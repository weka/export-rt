#
# collector module - implement prometheus_client collectors
#

# author: Vince Fleming, vince@weka.io


import time
import traceback
from logging import getLogger
from threading import Lock

from prometheus_client.core import GaugeMetricFamily
# local imports
from wekalib.sthreads import simul_threads

# initialize logger - configured in main routine
log = getLogger(__name__)


# makes life easier
def parse_sizes_values_post38(values):  # returns list of tuples of [(iosize,value),(iosize,value),...], and their sum
    # example input: [{'value': 2474458, 'start_range': 4096, 'end_range': 8192}]  - a list, NOT a string
    # log.debug(f"value_string={values}, type={type(values)}")
    gsum = 0
    stat_list = []

    for value in values:
        gsum += float(value['value']) / 60  # bug - have to divide by 60 secs to calc average per sec
        # stat_list.append( ( str(value['end_range']), float(value['value'])/60 ) )
        stat_list.append((str(value['end_range']), gsum))  # each bucket must be sum of all previous

    return stat_list, gsum


# makes life easier
def parse_sizes_values_pre38(
        value_string):  # returns list of tuples of [(iosize,value),(iosize,value),...], and their sum
    # example input: "[32768..65536] 19486, [65536..131072] 1.57837e+06"
    # log.debug(f"value_string={value_string}, type={type(value_string)}")
    gsum = 0
    stat_list = []
    values_list = value_string.split(", ")  # should be "[32768..65536] 19486","[65536..131072] 1.57837e+06"
    for values_str in values_list:  # value_list should be "[32768..65536] 19486" the first time through
        tmp = values_str.split("..")  # should be "[32768", "65536] 19486"
        tmp2 = tmp[1].split("] ")  # should be "65536","19486"
        gsum += float(tmp2[1])
        # stat_list.append( ( str(int(tmp2[0])-1), float(tmp2[1]) ) )
        stat_list.append((str(int(tmp2[0]) - 1), gsum))

    return stat_list, gsum


# our prometheus collector
class wekaCollector(object):
    WEKAINFO = {
        "hostList": dict(method="hosts_list", parms={}),
        "clusterinfo": dict(method="status", parms={}),
        "nodeList": dict(method="nodes_list", parms={}),
    }

    def __init__(self, configfile, cluster_obj):  # wekaCollector

        # dynamic module globals
        # this comes from a yaml file
        self._access_lock = Lock()
        self.gather_timestamp = None
        self.collect_time = None
        self.clusterdata = {}
        self.threaderror = False
        self.api_stats = {}

        self.wekaCollector_objlist = {str(cluster_obj): cluster_obj}

        log.debug("wekaCollector created")

    def add_cluster(self, cluster_obj):
        self.wekaCollector_objlist[str(cluster_obj)] = cluster_obj

    # module global metrics allows for getting data from multiple clusters in multiple threads - DO THIS WITH A LOCK
    def _reset_metrics(self):
        # create all the metrics objects that we'll fill in elsewhere
        global metric_objs
        metric_objs = {
            'cmd_gather': GaugeMetricFamily('weka_rt_gather_seconds', 'Time spent gathering statistics',
                labels=["cluster"]),
            'weka_rtstats_gauge': GaugeMetricFamily('weka_rtstats', 'WekaFS RT statistics.',
                labels=['cluster', 'host_name', 'host_role', 'node_id', 'node_role', 'stat'])
        }

    def collect(self):
        with self._access_lock:  # be thread-safe - if we get called from simultaneous scrapes... could be ugly
            self.api_stats['num_calls'] = 0
            start_time = time.time()

            log.info("gathering")
            self.gather_timestamp = start_time
            self._reset_metrics()
            thread_runner = simul_threads(len(self.wekaCollector_objlist))  # one thread per cluster
            for clustername, cluster in self.wekaCollector_objlist.items():
                thread_runner.new(self.gather, cluster)
                # thread_runner.new( cluster.gather )
            thread_runner.run()
            del thread_runner

            # ok, the prometheus_client module calls this method TWICE every time it scrapes...  ugh
            last_collect = self.collect_time

            # yield for each metric 
            for metric in metric_objs.values():
                yield metric

            # report time if we gathered, otherwise, it's meaningless
            elapsed = time.time() - start_time
            self.last_elapsed = elapsed

            yield GaugeMetricFamily('weka_rt_collect_seconds', 'Total Time spent in Prometheus collect', value=elapsed)
            yield GaugeMetricFamily('weka_rt_collect_apicalls', 'Total number of api calls',
                                    value=self.api_stats['num_calls'])

            log.info(
                f"status returned. total time = {round(elapsed, 2)}s {self.api_stats['num_calls']} api calls made. {time.asctime()}")

    # runs in a thread, so args comes in as a dict
    def call_api(self, cluster, metric, category, args):
        self.api_stats['num_calls'] += 1
        method = args['method']
        parms = args['parms']
        log.debug(f"method={method}, parms={parms}")
        try:
            if category is None:
                self.clusterdata[str(cluster)][metric] = cluster.call_api(method=method, parms=parms)
            else:
                if category not in self.clusterdata[str(cluster)]:
                    self.clusterdata[str(cluster)][category] = {}
                api_return = cluster.call_api(method=method, parms=parms)
                if metric not in self.clusterdata[str(cluster)][category]:
                    log.debug(f"first call for {category}/{metric}")
                    self.clusterdata[str(cluster)][category][metric] = api_return
                else:
                    log.debug(f"follow-on call for {category}/{metric}")
                    self.clusterdata[str(cluster)][category][metric] += api_return
                    # then we already have data for this category - must be a lot of nodes (>100)
        except Exception as exc:
            # just log it, as we're probably in a thread
            log.critical(f"Exception caught: {exc}")
            log.debug(traceback.format_exc())

    # start here
    #
    # gather() gets fresh stats from the cluster as they update
    #       populates all datastructures with fresh data
    #
    # gather() is PER CLUSTER
    #
    # @gather_gauge.time()        # doesn't make a whole lot of sense since we may have more than one cluster
    def gather(self, cluster):
        start_time = time.time()
        log.info("gathering weka data from cluster {}".format(str(cluster)))

        # re-initialize wekadata so changes in the cluster don't leave behind strange things (hosts/nodes that no longer exist, etc)
        wekadata = {}
        self.clusterdata[str(cluster)] = wekadata  # clear out old data

        # reset the cluster config to be sure we can talk to all the hosts
        cluster.refresh_config()

        # to do on-demand gathers instead of every minute;
        #   only gather if we haven't gathered in this minute (since 0 secs has passed)

        thread_runner = simul_threads(cluster.sizeof())  # 1 per host, please

        # get info from weka cluster
        for stat, command in self.WEKAINFO.items():
            try:
                thread_runner.new(self.call_api, cluster, stat, None, command)
            except:
                log.error("error scheduling wekainfo threads for cluster {}".format(cluster.name))
                return  # bail out if we can't talk to the cluster with this first command

        thread_runner.run()  # kick off threads; wait for them to complete

        if self.threaderror or cluster.sizeof() == 0:
            log.critical(f"api unable to contact cluster {cluster}; aborting gather")
            return

        del thread_runner

        # build maps - need this for decoding data, not collecting it.
        #    do in a try/except block because it can fail if the cluster changes while we're collecting data

        # clear old maps, if any - if nodes come/go this can get funky with old data, so re-create it every time
        weka_maps = {"node-host": {}, "node-role": {}, "host-role": {}}  # initial state of maps

        # populate maps
        try:
            for node in wekadata["nodeList"]:
                weka_maps["node-host"][node["node_id"]] = node["hostname"]
                weka_maps["node-role"][node["node_id"]] = node["roles"]  # note - this is a list
            for host in wekadata["hostList"]:
                if host["mode"] == "backend":
                    weka_maps["host-role"][host["hostname"]] = "server"
                else:
                    weka_maps["host-role"][host["hostname"]] = "client"
        except Exception as exc:
            print(f"EXCEPTION {exc}")
            track = traceback.format_exc()
            print(track)
            log.error("error building maps. Aborting data gather from cluster {}".format(str(cluster)))
            return

        log.info(f"Cluster {cluster} Using {cluster.sizeof()} hosts")
        thread_runner = simul_threads(
            cluster.sizeof() * 4)  # up the server count - so 1 thread per server in the cluster

        # be simplistic at first... let's just gather on a subset of nodes each query
        # all_nodes = backend_nodes + client_nodes    # concat both lists

        node_maps = {"FRONTEND": [], "COMPUTE": [], "DRIVES": [], "MANAGEMENT": []}  # initial state of maps

        # log.debug(f'{weka_maps["node-role"]}')

        for node in weka_maps["node-role"]:  # node == "NodeId<xx>"
            for role in weka_maps['node-role'][node]:
                nid = int(node.split('<')[1].split('>')[0])  # make nodeid numeric
                node_maps[role].append(nid)

        # log.debug(f"{cluster.name} {node_maps}")

        # find a better place to define this... for now here is good (vince)
        category_nodetypes = {
            'cpu': ['FRONTEND', 'COMPUTE', 'DRIVES'],
            'ops': ['FRONTEND'],
            'ops_driver': ['FRONTEND'],
            'ops_nfs': ['COMPUTE'],  # not sure about this one
            'ssd': ['DRIVES']
        }

        try:
            thread_runner.new(self.call_api, cluster, "realtime", None, {"method": "stats_get_realtime", "parms": []})
        except:
            log.error("gather(): error scheduling thread wekastat for cluster {}".format(str(cluster)))

        thread_runner.run()  # schedule the rest of the threads, wait for them
        del thread_runner
        elapsed = time.time() - start_time
        log.debug(f"gather for cluster {cluster} complete.  Elapsed time {elapsed}")
        metric_objs['cmd_gather'].add_metric([str(cluster)], value=elapsed)

        # if the cluster changed during a gather, this may puke, so just go to the next sample.
        #   One or two missing samples won't hurt

        #  Start filling in the data
        log.info("populating datastructures for cluster {}".format(str(cluster)))

        for nodeid, node_stats in wekadata["realtime"].items():
            for stat, value in node_stats.items():
                hostname = weka_maps["node-host"][nodeid]
                role_list = weka_maps["node-role"][nodeid]
                for role in role_list:
                    labelvalues = [
                        str(cluster),
                        hostname,
                        weka_maps["host-role"][hostname],
                        nodeid,
                        role,
                        stat]

                    try:
                        metric_objs['weka_rtstats_gauge'].add_metric(labelvalues, value)
                    except:
                        print(f"{traceback.format_exc()}")
                        # print(track)
                        log.error("error processing io stats for cluster {}".format(str(cluster)))

        log.debug(f"Complete cluster={cluster.name}")

    # ------------- end of gather() -------------

    @staticmethod
    def _trim_time(time_string):
        tmp = time_string.split('.')
        return tmp[0]
