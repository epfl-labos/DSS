class YarnSchedulerStats(object):
    def __init__(self):
        super(YarnSchedulerStats, self).__init__()
        self.total_decisions = 0
        self.accept_decisions = 0
        self.reject_decisions = 0
        self.reserve_decisions = 0
        self.job_total_decisions = {}
        self.job_accept_decisions = {}
        self.job_reject_decisions = {}
        self.job_reserve_decisions = {}
        self.reserved_memory_time = 0
        self.reserved_usable_memory_time = 0
        self.job_min_start_time_ms = -1
        self.job_max_end_time_ms = -1
        self.total_mem_utilization_ms = 0
        self.cluster_reservations = dict()

    def stats_job_start_update(self, job_start_ms):
        if job_start_ms < self.job_min_start_time_ms or self.job_min_start_time_ms == -1:
            self.job_min_start_time_ms = job_start_ms

    def stats_job_end_update(self, job_end_ms):
        if job_end_ms > self.job_max_end_time_ms or self.job_max_end_time_ms == -1:
            self.job_max_end_time_ms = job_end_ms

    def stats_container_end_update(self, container):
        self.total_mem_utilization_ms += container.duration_ms * container.resource.memory_mb

    def get_makespan_ms(self):
        return self.job_max_end_time_ms - self.job_min_start_time_ms

    def stats_decisions_inc(self, job_id):
        self.total_decisions += 1
        if job_id not in self.job_total_decisions:
            self.job_total_decisions[job_id] = 0
        self.job_total_decisions[job_id] += 1

    def stats_accept_decisions_inc(self, job_id):
        self.accept_decisions += 1
        if job_id not in self.job_accept_decisions:
            self.job_accept_decisions[job_id] = 0
        self.job_accept_decisions[job_id] += 1

    def stats_reject_decisions_inc(self, job_id):
        self.reject_decisions += 1
        if job_id not in self.job_reject_decisions:
            self.job_reject_decisions[job_id] = 0
        self.job_reject_decisions[job_id] += 1

    def stats_reserve_decisions_inc(self, job_id):
        self.reserve_decisions += 1
        if job_id not in self.job_reserve_decisions:
            self.job_reserve_decisions[job_id] = 0
        self.job_reserve_decisions[job_id] += 1

    def stats_reserve_node(self, job_id, node_id, clock_millis):
        while True:
            try:
                self.cluster_reservations[node_id].append((job_id, clock_millis, None))
                return
            except KeyError:
                self.cluster_reservations[node_id] = []

    def stats_release_node(self, job_id, node_id, clock_millis):
        try:
            last_reservation = self.cluster_reservations[node_id][-1]
            assert last_reservation[0] == job_id
            self.cluster_reservations[node_id][-1] = (last_reservation[0], last_reservation[1], clock_millis)
        except KeyError:
            self.cluster_reservations[node_id] = []

    @staticmethod
    def __get_stats_str(total_decisions, accept_decisions, reserve_decisions, reject_decisions):
        return "TOTAL: {}, ACCEPT: {}({:.2f}%), REJECT: {}({:.2f}%), RESERVE: {}({:.2f}%)".format(
            total_decisions, accept_decisions, accept_decisions * 100.0 / total_decisions,
            reject_decisions, reject_decisions * 100.0 / total_decisions,
            reserve_decisions, reserve_decisions * 100.0 / total_decisions
        )

    def get_job_stats_str(self, job_id):
        total_decisions = self.job_total_decisions[job_id]
        reject_decisions = self.job_reject_decisions[job_id] if job_id in self.job_reject_decisions else 0
        reserve_decisions = self.job_reserve_decisions[job_id] if job_id in self.job_reserve_decisions else 0
        accept_decisions = self.job_accept_decisions[job_id] if job_id in self.job_accept_decisions else 0
        return self.__get_stats_str(total_decisions, accept_decisions, reserve_decisions, reject_decisions)

    def get_total_stats_str(self):
        return self.__get_stats_str(self.total_decisions, self.accept_decisions,
                                    self.reserve_decisions, self.reject_decisions)


class YarnFairSchedulerElasticStats(YarnSchedulerStats):

    def __init__(self):
        super(YarnFairSchedulerElasticStats, self).__init__()
        self.elastic_decisions = 0
        self.job_elastic_decisions = {}

    def stats_elastic_decisions_inc(self, job_id):
        self.elastic_decisions += 1
        if job_id not in self.job_elastic_decisions:
            self.job_elastic_decisions[job_id] = 0
        self.job_elastic_decisions[job_id] += 1

    @staticmethod
    def __get_elastic_stats_str(accept_decisions, elastic_decisions):
        return "REGULAR: {}({:.2f}%), ELASTIC: {}({:.2f}%)".format(
            accept_decisions - elastic_decisions, (accept_decisions - elastic_decisions) * 100.0 / accept_decisions,
            elastic_decisions, elastic_decisions * 100.0 / accept_decisions
        )

    def get_job_stats_str(self, job_id):
        fair_scheduler_stats_str = super(YarnFairSchedulerElasticStats, self).get_job_stats_str(job_id)
        accept_decisions = self.job_accept_decisions[job_id] if job_id in self.job_accept_decisions else 0
        elastic_decisions = self.job_elastic_decisions[job_id] if job_id in self.job_elastic_decisions else 0
        elastic_stats_str = self.__get_elastic_stats_str(accept_decisions, elastic_decisions)
        return ', '.join([fair_scheduler_stats_str, elastic_stats_str])

    def get_total_stats_str(self):
        fair_scheduler_stats_str = super(YarnFairSchedulerElasticStats, self).get_total_stats_str()
        elastic_stats_str = self.__get_elastic_stats_str(self.accept_decisions, self.elastic_decisions)
        return ', '.join([fair_scheduler_stats_str, elastic_stats_str])
