import logging
from abc import ABCMeta

from models.yarn.objects import YarnErrorMode, YarnErrorType, YarnResource, YarnRunningContainer, YarnContainerType
from schedulers.yarn import YarnRegularScheduler, YarnFairScheduler, LOG
from stats.decisions import YarnFairSchedulerElasticStats
from utils import round_up

ELASTIC_MEM_INCREMENT = 100
ELASTIC_MEM_MIN_RATIO = 10
YARN_MIN_ALLOCATION_MB = 100


class YarnElasticScheduler(YarnRegularScheduler, YarnFairSchedulerElasticStats):
    __metaclass__ = ABCMeta

    def __init__(self, state, node_count, elastic_pool):
        super(YarnElasticScheduler, self).__init__(state)
        self.node_count = node_count
        self.elastic_pool = elastic_pool
        self.disable_elastic = False

    def __deepcopy__(self, memo):
        new_scheduler = YarnFairScheduler.__deepcopy__(self, memo)
        return new_scheduler

    def get_memory_error_adjustment(self, task):
        # Check if we need to inject a random error in the ideal memory.
        if self.state.user_config.mem_error is None:
            return 0

        memory_error_perc = self.state.user_config.mem_error
        memory_error = task.ideal_resource.memory_mb * memory_error_perc / 100
        if self.state.user_config.mem_error_mode is YarnErrorMode.CONSTANT:
            if self.state.user_config.mem_error_type is YarnErrorType.POSITIVE:
                memory_adjustment = memory_error
            else:
                memory_adjustment = -memory_error
        else:
            lower_limit = -memory_error
            upper_limit = memory_error
            if self.state.user_config.mem_error_type is YarnErrorType.POSITIVE:
                lower_limit = 0
            elif self.state.user_config.mem_error_type is YarnErrorType.NEGATIVE:
                upper_limit = 0
            memory_adjustment = task.job.random.randint(lower_limit, upper_limit)

        if task.ideal_resource.memory_mb + memory_adjustment < YARN_MIN_ALLOCATION_MB:
            memory_adjustment = YARN_MIN_ALLOCATION_MB - task.ideal_resource.memory_mb

        if task.ideal_resource.memory_mb + memory_adjustment > self.state.user_config.node_mem_mb:
            memory_adjustment = self.state.user_config.node_mem_mb - task.ideal_resource.memory_mb

        return memory_adjustment

    def create_container_from_task(self, node, allocated, job, task):
        duration = task.duration
        duration_error = 0
        if allocated.memory_mb < task.ideal_resource.memory_mb:
            duration = task.penalty.get_penalized_runtime(task.ideal_resource, allocated, task.duration)
            error_adjustment = self.get_memory_error_adjustment(task)
            if error_adjustment != 0:
                resource = YarnResource(task.ideal_resource.memory_mb, task.ideal_resource.vcores)
                resource.memory_mb += self.get_memory_error_adjustment(task)
                resource.memory_mb = round_up(resource.memory_mb * ELASTIC_MEM_MIN_RATIO / 100, ELASTIC_MEM_INCREMENT)
                if allocated.memory_mb >= resource.memory_mb:
                    erred_duration = task.duration
                else:
                    erred_duration = task.penalty.get_penalized_runtime(resource, allocated, task.duration)
                duration_error = erred_duration - duration
            # Update decision count
            self.stats_elastic_decisions_inc(task.job.job_id)

            if LOG.isEnabledFor(logging.DEBUG):
                LOG.debug("YARN_ELASTIC_SCHEDULER: Allocated " +
                          str(allocated.memory_mb) + " elasticly for " +
                          job.get_name() + ":" + str(job.next_container_id) +
                          " on node " + str(node) + " with penalized runtime of " +
                          str(duration) + " for a request of " +
                          str(task.resource.memory_mb) + " and a duration of: " +
                          str(task.duration))
        else:
            if LOG.isEnabledFor(logging.DEBUG):
                LOG.debug("YARN_SMARTG_SCHEDULER: Allocated " +
                          str(allocated.memory_mb) + " regularly for " +
                          job.get_name() + ":" + str(job.next_container_id) +
                          " on node " + str(node) + " with a duration of: " +
                          str(task.duration))

        # Create YarnRunningContainer from YarnPrototypeContainer
        yarn_container = YarnRunningContainer(
            duration=duration,
            resource=allocated,
            priority=task.priority,
            container_type=task.type,
            job=task.job,
            node=node,
            task=task)

        yarn_container.duration_error = duration_error

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
        if node.available.memory_mb < round_up(task.resource.memory_mb * ELASTIC_MEM_MIN_RATIO / 100,
                                               ELASTIC_MEM_INCREMENT):
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
        min_mem_allocatable = round_up(task.resource.memory_mb * ELASTIC_MEM_MIN_RATIO / 100, ELASTIC_MEM_INCREMENT)
        if min_mem_allocatable < YARN_MIN_ALLOCATION_MB:
            min_mem_allocatable = YARN_MIN_ALLOCATION_MB
        min_mem_for_runtime = node.available.memory_mb
        tmp_resource = YarnResource(min_mem_for_runtime, task.resource.vcores)
        min_runtime = YarnElasticScheduler.get_penalized_runtime(task.penalty,
                                                                 self.state.user_config.mem_overestimate_assume,
                                                                 task.resource, tmp_resource, task.duration)
        for mem in xrange(min_mem_allocatable, node.available.memory_mb, ELASTIC_MEM_INCREMENT):
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

