import copy
import sys
from abc import ABCMeta
from operator import attrgetter

from more_itertools import peekable

from models.yarn.objects import YarnResource
from schedulers.elastic import YarnSmartgScheduler
from schedulers.yarn import YarnSchedulerType, YarnRegularScheduler, LOG
from utils import PEnum

PushbackStrategy = PEnum("PushbackStrategy", "TIMELINE_CHECK_CURRENT_JOB PEEK_EACH_ELASTIC")


class YarnOracleScheduler(YarnSmartgScheduler):
    __metaclass__ = ABCMeta

    def __init__(self, state):
        super(YarnOracleScheduler, self).__init__(state, 0, None)
        self.next_simulation_event = None

    def __deepcopy__(self, memo):
        new_scheduler = super(YarnOracleScheduler, self).__deepcopy__(memo)
        new_scheduler.next_simulation_event = None
        return new_scheduler


class YarnGlobalDecisionScheduler(YarnOracleScheduler):
    __metaclass__ = ABCMeta

    def __init__(self, state):
        super(YarnGlobalDecisionScheduler, self).__init__(state)
        self.behavior = YarnSchedulerType.REGULAR

    def __deepcopy__(self, memo):
        new_scheduler = super(YarnGlobalDecisionScheduler, self).__deepcopy__(memo)
        new_scheduler.behavior = copy.copy(self.behavior)
        return new_scheduler

    def allocate_on_node(self, node, task):

        # First try being regular
        regular = YarnRegularScheduler.allocate_on_node(self, node, task)
        if regular is not None:
            return regular

        if self.behavior is YarnSchedulerType.SMARTG:
            return YarnSmartgScheduler.allocate_on_node(self, node, task)

        return None


class YarnJobDecisionScheduler(YarnOracleScheduler):
    __metaclass__ = ABCMeta

    def __init__(self, state):
        super(YarnJobDecisionScheduler, self).__init__(state)
        self.job_behaviors = {}
        for job in state.jobs:
            self.job_behaviors[job.job_id] = YarnSchedulerType.REGULAR

    def __deepcopy__(self, memo):
        new_scheduler = super(YarnJobDecisionScheduler, self).__deepcopy__(memo)
        new_scheduler.job_behaviors = copy.copy(self.job_behaviors)
        return new_scheduler

    def set_behavior(self, job, behavior):
        self.job_behaviors[job.job_id] = behavior

    def allocate_on_node(self, node, task):

        # First try being regular
        regular = YarnRegularScheduler.allocate_on_node(self, node, task)
        if regular is not None:
            return regular

        if self.job_behaviors[task.job.job_id] is YarnSchedulerType.SMARTG:
            return YarnSmartgScheduler.allocate_on_node(self, node, task)

        return None


class YarnNodeGDecisionScheduler(YarnOracleScheduler):

    def __init__(self, state):
        super(YarnNodeGDecisionScheduler, self).__init__(state)
        self.node_behaviors = {}
        for node in state.nodes:
            self.node_behaviors[node] = YarnSchedulerType.REGULAR

    def __deepcopy__(self, memo):
        new_scheduler = super(YarnNodeGDecisionScheduler, self).__deepcopy__(memo)
        new_scheduler.node_behaviors = copy.copy(self.node_behaviors)
        return new_scheduler

    def set_behavior(self, node, behavior):
        self.node_behaviors[node.name] = behavior

    def allocate_on_node(self, node, task):

        # First try being regular
        regular = YarnRegularScheduler.allocate_on_node(self, node, task)
        if regular is not None:
            return regular

        if self.node_behaviors[node.name] is YarnSchedulerType.SMARTG:
            return YarnSmartgScheduler.allocate_on_node(self, node, task)

        return None


class YarnPeekScheduler(YarnJobDecisionScheduler):
    def __init__(self, state):
        super(YarnPeekScheduler, self).__init__(state)
        self.elastic_time_limits = {}
        self.job_finish_scheduling_times = {}
        self.job_finish_times = {}
        self.most_recent_peek = None

    def __deepcopy__(self, memo):
        new_scheduler = super(YarnPeekScheduler, self).__deepcopy__(memo)
        new_scheduler.elastic_time_limits = {}
        new_scheduler.job_finish_scheduling_times = {}
        new_scheduler.job_finish_times = {}
        return new_scheduler

    def handle_job_arrived(self, job):
        super(YarnPeekScheduler, self).handle_job_arrived(job)
        self.elastic_time_limits[job.job_id] = sys.maxint

    def set_elastic_time_limit(self, job, time_limit):
        self.elastic_time_limits[job.job_id] = time_limit

    def set_job_finish_scheduling_time(self, job, finish_time):
        self.job_finish_scheduling_times[job.job_id] = finish_time

    def set_job_finish_time(self, job, finish_time):
        self.job_finish_times[job.job_id] = finish_time

    def get_job_finish_scheduling_time(self, job):
        if job.job_id not in self.job_finish_scheduling_times:
            return None

        return self.job_finish_scheduling_times[job.job_id]

    def get_job_finish_time(self, job):
        if job.job_id not in self.job_finish_times:
            return None

        return self.job_finish_times[job.job_id]

    def reset_times(self):
        self.elastic_time_limits = {job_id: sys.maxint for job_id in self.elastic_time_limits}
        self.job_finish_times.clear()
        self.job_finish_scheduling_times.clear()

    def check_pushback_timeline_current_job(self, job_id, allocated, node, elastic_finish_time):
        if self.most_recent_peek is not None and self.most_recent_peek.jobs:
            # Get the required resources of regular containers that will execute on the current node
            # for the duration of this elastic container.
            peeked_job = next(job for job in self.most_recent_peek.jobs if job.job_id == job_id)
            if not peeked_job.finished:
                return False
            regular_containers = sorted(
                (container for container in peeked_job.finished_tasks if
                 not container.is_elastic and container.node.node_id == node.node_id and
                 self.state.simulator.clock_millis <= container.scheduled_time_millis < elastic_finish_time),
                key=attrgetter('scheduled_time_millis')
            )

            regular_containers_by_end = sorted(list(regular_containers), key=attrgetter('finished_time_millis'))

            max_resource = YarnResource(0, 0)
            iter_resource = YarnResource(0, 0)
            end_iter = peekable(iter(regular_containers_by_end))
            for container in regular_containers:
                iter_resource += container.resource
                while end_iter:
                    ending_container = end_iter.peek()

                    if ending_container.finished_time_millis < container.scheduled_time_millis:
                        iter_resource -= ending_container.resource
                        end_iter.next()
                    else:
                        break

                max_resource = max(max_resource, iter_resource)

            # Check that scheduling this ELASTIC container will not hinder regular containers of the same job.
            return allocated[0] + max_resource > node.capacity

        else:
            return False

    def check_pushback_each_elastic(self, job_id, allocated, node):
        # Run PEEK again, but mark this container as launched
        simulator = self.state.simulator
        from models.oracle.peek import PeekOracle
        cutoff_time = PeekOracle.compute_cutoff_time(simulator.clock_millis,
                                                     self.state.user_config.node_hb_ms,
                                                     self.running_jobs)
        simulator_state = PeekOracle.generate_simulation(self.state, LOG, simulator.clock_millis, cutoff_time)
        # Simulate container launching
        peek_scheduler = simulator_state.scheduler
        peek_job = next(job for job in simulator_state.jobs if job.job_id == job_id)
        peek_queue_idx = peek_scheduler.job_queue.index(peek_job)
        peek_task = next(task for task in peek_job.pending_tasks)
        peek_node = simulator_state.nodes[node.name]
        peek_scheduler.disable_elastic = False
        peek_scheduler.adjust_for_allocation(peek_node,
                                             peek_task,
                                             peek_queue_idx,
                                             allocated)
        peek_scheduler.disable_elastic = True

        if peek_node.next_heartbeat is not None and peek_node.next_heartbeat.handled:
            peek_node.next_heartbeat.generate_next_heartbeat()

        # Run the simulation
        PeekOracle.run_simulation(self.state, simulator_state)

        # Get the new job finish time
        if not peek_job.finished:
            LOG.info("PEEK: Job {} would not finish by {} instead of {} if container is scheduled ELASTIC.".format(
                job_id, cutoff_time, self.elastic_time_limits[job_id]
            ))
            return True
        elif max(map(lambda x: x.finished_time_millis, peek_job.finished_tasks)) > self.elastic_time_limits[job_id]:
            LOG.info("PEEK: Job {} would finish at {} instead of {} if container is scheduled ELASTIC.".format(
                job_id, peek_job.end_ms, self.elastic_time_limits[job_id]
            ))
            return True
        else:
            return False

    def elastic_will_pushback(self, job_id, allocated, node, elastic_finish_time):
        if self.elastic_time_limits[job_id] >= elastic_finish_time:
            if not self.state.user_config.peek_pushback_strategy:
                return False
            elif self.state.user_config.peek_pushback_strategy is PushbackStrategy.TIMELINE_CHECK_CURRENT_JOB:
                return self.check_pushback_timeline_current_job(job_id, allocated, node, elastic_finish_time)
            elif self.state.user_config.peek_pushback_strategy is PushbackStrategy.PEEK_EACH_ELASTIC:
                return self.check_pushback_each_elastic(job_id, allocated, node)
        return True

    def allocate_on_node(self, node, task):

        # First try being regular
        allocated = YarnRegularScheduler.allocate_on_node(self, node, task)
        if allocated is not None:
            return allocated

        job_id = task.job.job_id
        if self.job_behaviors[job_id] is YarnSchedulerType.SMARTG:
            allocated = YarnSmartgScheduler.allocate_on_node(self, node, task)
            if allocated is None or self.elastic_time_limits[job_id] is sys.maxint:
                return allocated
            # Check if there is enough time remaining to run this container elasticly.
            elastic_finish_time = allocated[1] + self.state.simulator.clock_millis
            LOG.info("PEEK: Job {} finish time: {}, task finish time: {}".format(
              job_id, self.elastic_time_limits[job_id], elastic_finish_time
            ))
            if not self.elastic_will_pushback(job_id, allocated, node, elastic_finish_time):
                return allocated
            else:
                LOG.info("PEEK: Job " + str(job_id) + ", ELASTIC not possible.")

        return None
