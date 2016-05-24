import logging
from abc import ABCMeta

from models.yarn.objects import YarnResource, YarnRunningContainer, YarnContainerType
from schedulers.yarn import YarnRegularScheduler, YarnFairScheduler, LOG, MEM_INCREMENT, YARN_MIN_ALLOCATION_MB
from stats.decisions import YarnFairSchedulerElasticStats
from utils import round_up

ELASTIC_MEM_MIN_RATIO = 10


class YarnElasticScheduler(YarnRegularScheduler, YarnFairSchedulerElasticStats):
    __metaclass__ = ABCMeta

    def __init__(self, state, node_count, elastic_pool):
        super(YarnElasticScheduler, self).__init__(state)
        self.node_count = node_count
        self.elastic_pool = elastic_pool
        self.disable_elastic = False
        # Per-job error factors for IB
        self.ib_errors = {}

    def __deepcopy__(self, memo):
        new_scheduler = YarnFairScheduler.__deepcopy__(self, memo)
        return new_scheduler

    def handle_job_arrived(self, job):
        super(YarnElasticScheduler, self).handle_job_arrived(job)
        if self.state.is_inside_oracle_simulation:
            return
        if self.state.user_config.ib_error is not None:
            self.ib_errors[job.job_id] = self.compute_ib_error_adjustment(
                next(task for task in job.tasks if task.type != YarnContainerType.MRAM)
            )

    def compute_ib_error_adjustment(self, task):
        if not hasattr(task.penalty, "initial_bump"):
            return 0
        ib_adjustment = self.compute_error_adjustment(task.penalty.initial_bump,
                                                      self.state.user_config.ib_error,
                                                      self.state.user_config.ib_error_mode,
                                                      self.state.user_config.ib_error_type)

        if task.penalty.initial_bump + ib_adjustment < 1:
            ib_adjustment = 1 - task.penalty.initial_bump

        return ib_adjustment

    def create_container_from_task(self, node, allocated, job, task):
        # XXX: This code duplicates the code from YarnScheduler. Needs refactoring.
        err_adjusted_duration = duration = task.duration
        if task.penalty is not None:
            mem_error_adjustment = self.mem_errors[job.job_id] if job.job_id in self.mem_errors else 0
            ib_error_adjustment = self.ib_errors[job.job_id] if job.job_id in self.ib_errors else 0
            if LOG.isEnabledFor(logging.DEBUG):
                LOG.debug("YARN_ELASTIC_SCHEDULER: MEM_ERR_ADJUST: {}, IB_ERR_ADJUST: {}".format(mem_error_adjustment,
                                                                                                 ib_error_adjustment))
            resource = task.ideal_resource
            if mem_error_adjustment != 0:
                resource = YarnResource(task.ideal_resource.memory_mb, task.ideal_resource.vcores)
                resource.memory_mb += mem_error_adjustment
                resource.memory_mb = round_up(resource.memory_mb, MEM_INCREMENT)

            if allocated < resource:
                err_adjusted_duration = task.penalty.get_penalized_runtime(resource, allocated, task.duration,
                                                                           error_offset=ib_error_adjustment)

                duration = task.penalty.get_penalized_runtime(task.ideal_resource, allocated, task.duration)

        # Create YarnRunningContainer from YarnPrototypeContainer
        yarn_container = YarnRunningContainer(
            container_id=job.get_new_container_id(),
            duration=err_adjusted_duration,
            resource=allocated,
            priority=task.priority,
            container_type=task.type,
            job=task.job,
            node=node,
            task=task)

        yarn_container.duration_error = err_adjusted_duration - duration

        if yarn_container.is_elastic:
            # Update decision count
            self.stats_elastic_decisions_inc(task.job.job_id)

            if LOG.isEnabledFor(logging.DEBUG):
                LOG.debug("YARN_ELASTIC_SCHEDULER: Allocated {} elasticly for {}:{} on node {} "
                          "with penalized runtime of {} (err adjusted: {}) for a request of {} "
                          "and a duration of {}".format(
                            allocated.memory_mb, job.get_name(), job.next_container_id, node, duration,
                            err_adjusted_duration, task.resource.memory_mb, task.duration
                          ))
        else:
            if LOG.isEnabledFor(logging.DEBUG):
                LOG.debug("YARN_ELASTIC_SCHEDULER: Allocated {} regularly for {}:{} on node {} "
                          "with a duration of: {} (err adjusted: {})".format(
                            allocated.memory_mb, job.get_name(), job.next_container_id, node, task.duration,
                            err_adjusted_duration
                          ))

        return yarn_container

    @staticmethod
    def get_penalized_runtime(penalty, overestimate_assume, requested_resource, given_resource, optimal_duration):
        # If the resource's memory is within the overestimation assumption, don't penalize
        if given_resource.memory_mb > requested_resource.memory_mb / overestimate_assume:
            return optimal_duration

        return penalty.get_penalized_runtime(requested_resource, given_resource, optimal_duration)

    def elastic_possible(self, node, task):
        # Check that the task is not an MRAM container:
        if task.type is YarnContainerType.MRAM:
            return False

        # Check if the task has an associated penalty function.
        if task.penalty is None:
            LOG.error("Task " + str(task) + " has no associated penalty function. ELASTIC disabled.")
            return False

        # Check if elastic behavior has been disabled as part of symbex or PEEK.
        if self.disable_elastic:
            return False

        # Check that if node pools are set, and the current node is part
        # of a non-elastic pool, we don't attempt to be elastic.
        if self.elastic_pool is not None and node.node_id > (self.node_count * 1.0 * self.elastic_pool / 100):
            return False

        # Check that sufficient cores exist on this node.
        if node.available.vcores < task.resource.vcores:
            return False

        # Next check that the minimum memory required exists
        if node.available.memory_mb < round_up(task.resource.memory_mb * ELASTIC_MEM_MIN_RATIO / 100, MEM_INCREMENT):
            return False

        return True


class YarnGreedyScheduler(YarnElasticScheduler):

    def __init__(self, state, node_count, elastic_pool):
        super(YarnGreedyScheduler, self).__init__(state, node_count, elastic_pool)

    def allocate_on_node(self, node, task):
        # First try being regular
        regular = YarnRegularScheduler.allocate_on_node(self, node, task)
        if regular is not None:
            return regular

        # Check if elasticity is possible
        if not self.elastic_possible(node, task):
            return None

        elastic_resource = YarnResource(node.available.memory_mb, task.resource.vcores)
        elastic_duration = YarnElasticScheduler.get_penalized_runtime(task.penalty,
                                                                      self.state.user_config.mem_overestimate_assume,
                                                                      task.resource, elastic_resource, task.duration)

        return elastic_resource, elastic_duration


class YarnSmartgScheduler(YarnElasticScheduler):

    def __init__(self, state, node_count, elastic_pool):
        super(YarnSmartgScheduler, self).__init__(state, node_count, elastic_pool)

    def allocate_on_node(self, node, task):
        # First try being regular
        regular = YarnRegularScheduler.allocate_on_node(self, node, task)
        if regular is not None:
            return regular

        # Check if elasticity is possible
        if not self.elastic_possible(node, task):
            return None

        # Check what is the best running time in these resources
        min_mem_allocatable = round_up(task.resource.memory_mb * ELASTIC_MEM_MIN_RATIO / 100, MEM_INCREMENT)
        if min_mem_allocatable < YARN_MIN_ALLOCATION_MB:
            min_mem_allocatable = YARN_MIN_ALLOCATION_MB
        min_mem_for_runtime = node.available.memory_mb
        tmp_resource = YarnResource(min_mem_for_runtime, task.resource.vcores)
        min_runtime = YarnElasticScheduler.get_penalized_runtime(task.penalty,
                                                                 self.state.user_config.mem_overestimate_assume,
                                                                 task.resource, tmp_resource, task.duration)
        for mem in xrange(min_mem_allocatable, node.available.memory_mb, MEM_INCREMENT):
            tmp_resource.memory_mb = mem
            penalized_runtime = YarnElasticScheduler.get_penalized_runtime(
                task.penalty, self.state.user_config.mem_overestimate_assume, task.resource, tmp_resource, task.duration
            )
            if penalized_runtime < min_runtime:
                min_runtime = penalized_runtime
                min_mem_for_runtime = mem
            elif penalized_runtime == min_runtime and mem < min_mem_for_runtime:
                min_mem_for_runtime = mem

        if LOG.isEnabledFor(logging.DEBUG):
            LOG.debug("YARN_SMARTG_SCHEDULER: SMARTG possible with " +
                      str(min_mem_for_runtime) + " for " + task.job.get_name() +
                      ":" + str(task.job.next_container_id) + " on node " +
                      str(node) + " with penalized runtime of " +
                      str(min_runtime) + " for a request of " +
                      str(task.resource.memory_mb) + " and a duration of: " +
                      str(task.duration))
        return YarnResource(min_mem_for_runtime, task.resource.vcores), min_runtime

