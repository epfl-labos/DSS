#!/usr/bin/env python
import copy
import logging
import sys
from abc import ABCMeta, abstractmethod

from events.event import EventResult
from models.yarn.objects import YarnRunningContainer, YarnContainerType, YarnResource
from stats.decisions import YarnSchedulerStats
from utils import PEnum

YarnSchedulerType = PEnum("YarnSchedulerType", "REGULAR GREEDY SMARTG SYMBEX RACE_LOCKSTEP RACE_CONTINUOUS " +
                          "RACE_JOB RACE_NODEG SRTF PEEK")

LOG = logging.getLogger("yarn_schedulers")


class YarnScheduler(YarnSchedulerStats):
    __metaclass__ = ABCMeta

    def __init__(self, state):
        super(YarnScheduler, self).__init__()
        self.state = state
        self.running_jobs = set()
        self.completed_jobs = set()
        self.allocated_containers = {}
        self.next_job_id = 1
        self.allow_scheduling = True

    # noinspection PyArgumentList
    def __deepcopy__(self, memo):
        new_scheduler = copy.copy(self)
        memo[id(self)] = new_scheduler
        new_scheduler.state = copy.deepcopy(self.state, memo)
        new_scheduler.running_jobs = copy.deepcopy(self.running_jobs, memo)
        new_scheduler.completed_jobs = self.completed_jobs.copy()
        new_scheduler.allocated_containers = copy.deepcopy(self.allocated_containers, memo)

        return new_scheduler

    def all_jobs_are_done(self):
        return all(job.finished for job in self.state.jobs)

    # NOTE: This method should return a tuple:
    # (AllocationSuccessful, EventResultTuple)
    # where AllocationResult is a bool marking if any
    # containers were allocated on this node,
    # and EventResultTuple is of the form:
    # (EventResult, [ newly_forked_symbex_states ]
    # A non-symbex call to schedule() will always return
    # (EventResult.CONTINUE,) as the EventResultTuple
    @abstractmethod
    def schedule(self, node):
        raise NotImplementedError()

    def handle_job_arrived(self, job):
        # Set a new application id for the job
        job.yarn_id = self.next_job_id
        self.next_job_id += 1

        # Check that job isn't already running
        if job in self.running_jobs:
            LOG.error("Attempt to launch existing job: " + str(job))
            raise Exception("Attempt to launch existing job")

        # Adjust job start time
        job.start_ms = self.state.simulator.clock_millis
        self.stats_job_start_update(job.start_ms)

        # Add job to set of running jobs
        self.running_jobs.add(job)

    def handle_job_completed(self, job):
        # Update finishing time
        job.end_ms = self.state.simulator.clock_millis
        self.stats_job_end_update(job.end_ms)
        # Move job to completed list.
        if job not in self.running_jobs:
            LOG.error("Completed job " + str(job) + " not found in running list: " + str(self.running_jobs))
            raise Exception("Completed job not in running list.")
        self.running_jobs.remove(job)
        self.completed_jobs.add(job)

    @abstractmethod
    def has_pending_jobs(self):
        raise NotImplementedError()

    def create_container_from_task(self, node, allocated, job, task):
        # Create YarnRunningContainer from YarnPrototypeContainer
        yarn_container = YarnRunningContainer(
            duration=task.duration,
            resource=task.resource,
            priority=task.priority,
            container_type=task.type,
            job=task.job,
            node=node,
            task=task)

        return yarn_container

    def handle_container_allocation(self, node, allocated, job, task, time_millis):
        # Create a container from the task
        yarn_container = self.create_container_from_task(node, allocated, job, task)

        # Adjust job to reflect launched container
        task.num_containers -= 1
        if task.num_containers == 0:
            # All of this job's type of tasks were processed: remove it from the task list
            job.pending_tasks.remove(task)
        job.running_tasks.add(yarn_container)
        job.consumption += yarn_container.resource
        # Mark container as scheduled
        yarn_container.schedule_container(time_millis)
        # Adjust node to reflect scheduled container
        node.book_container(yarn_container)
        # Add container to list of containers to launch
        try:
            self.allocated_containers[job.job_id].add(yarn_container)
        except KeyError:
            self.allocated_containers[job.job_id] = {yarn_container}

        # Generate next AM heartbeat, if needed.
        if job.am_next_heartbeat.handled:
            job.am_next_heartbeat.generate_next_heartbeat()

    def handle_container_finished(self, node, finished_container):
        job = finished_container.job
        job.consumption -= finished_container.resource
        self.stats_container_end_update(finished_container)


class YarnFairScheduler(YarnScheduler):
    __metaclass__ = ABCMeta

    def __init__(self, state):
        super(YarnFairScheduler, self).__init__(state)
        self.job_queue = []

    # noinspection PyArgumentList
    def __deepcopy__(self, memo):
        new_scheduler = YarnScheduler.__deepcopy__(self, memo)
        memo[id(self)] = new_scheduler
        new_scheduler.job_queue = copy.deepcopy(self.job_queue, memo)

        return new_scheduler

    def handle_job_arrived(self, job):
        YarnScheduler.handle_job_arrived(self, job)
        self.job_queue.insert(0, job)
        # Sort the job queue
        self.queue_down_job(0)

    def handle_container_finished(self, node, finished_container):
        YarnScheduler.handle_container_finished(self, node, finished_container)
        # Sort the job queue
        if finished_container.job in self.job_queue:
            self.queue_up_job(self.job_queue.index(finished_container.job))

    def has_pending_jobs(self):
        return len(self.job_queue) != 0

    @abstractmethod
    def allocate_on_node(self, node, task):
        # Returns a tuple (memory, duration)
        raise NotImplementedError()

    def queue_down_job(self, job_idx):
        queue_len = len(self.job_queue)
        while job_idx < queue_len - 1:
            job = self.job_queue[job_idx]
            next_job = self.job_queue[job_idx + 1]
            if [job.consumption.memory_mb, job.start_ms, job.job_id] > [next_job.consumption.memory_mb,
                                                                        next_job.start_ms, next_job.job_id]:
                self.job_queue[job_idx], self.job_queue[job_idx + 1] = \
                    self.job_queue[job_idx + 1], self.job_queue[job_idx]
                job_idx += 1
            else:
                return

    def queue_up_job(self, job_idx):
        while job_idx > 0:
            job = self.job_queue[job_idx]
            next_job = self.job_queue[job_idx - 1]
            if [job.consumption.memory_mb, job.start_ms, job.job_id] < [next_job.consumption.memory_mb,
                                                                        next_job.start_ms, next_job.job_id]:
                self.job_queue[job_idx], self.job_queue[job_idx - 1] = \
                    self.job_queue[job_idx - 1], self.job_queue[job_idx]
                job_idx -= 1
            else:
                return

    # noinspection PyUnusedLocal
    def adjust_for_allocation(self, node, task, queue_idx, alloc_result):
        allocated_resource = alloc_result[0]
        allocated_duration = alloc_result[1]
        job = self.job_queue[queue_idx]
        if LOG.isEnabledFor(logging.DEBUG):
            LOG.debug("CLUSTER: Capacity: " + str(reduce(lambda x, y: x + y,
                                                         map(lambda x: x.available,
                                                             self.state.nodes.values())).memory_mb)
                      + "MB Maximum: " + str(max(self.state.nodes.values(), key=lambda x:
                                                 x.available.memory_mb).available.memory_mb) + "MB")
            LOG.debug("QUEUE: " + ", ".join(map(lambda x: "<" + x.get_name() + " " + str(x.am_launched) + " " +
                      str(x.consumption) + ">", self.job_queue)))
            if not all(self.job_queue[i].consumption.memory_mb <= self.job_queue[i + 1].consumption.memory_mb
                       for i in xrange(len(self.job_queue) - 1)):
                LOG.error("QUEUE SORTING ERROR")

        # Adjust task, job and node properties to reflect allocation
        self.handle_container_allocation(node, allocated_resource, job, task,
                                         self.state.simulator.clock_millis)
        if not job.pending_tasks:
            # All of the job's containers were processed: remove it from the queue
            self.job_queue.remove(job)
        else:
            # Sort the queue again since this job's consumption has changed
            self.queue_down_job(queue_idx)

    def get_job_finish_scheduling_time(self, job):
        # Returns the expected time when all containers of this job are scheduled.
        return sys.maxint

    def get_job_finish_time(self, job):
        # Returns the expected time when the job finishes.
        return sys.maxint

    def get_container_finish_time(self, container, memo=None):
        # memo is an optional dictionary used to cache durations (e.g., for AM containers)
        if memo is not None and container in memo:
            return memo[container]

        if container.type is not YarnContainerType.MRAM:
            # NOTE: This is an approximation, since the container launch time is
            # given by the AM heartbeat that triggers it.
            container_finish_time = container.launched_time_millis if container.launched_time_millis != -1 else \
                                    (container.scheduled_time_millis + container.node.hb_interval_ms)
            container_finish_time += container.duration
        else:
            # AM containers can only be determined if the job duration is somehow known.
            container_finish_time = self.get_job_finish_time(container.job)

        if memo is not None:
            memo[container] = container_finish_time
        return container_finish_time

    def schedule(self, node, queue_start=0):
        # Check if scheduling is postponed
        if not self.allow_scheduling:
            # Return True so that node heartbeats keep getting created.
            return True, (EventResult.CONTINUE,)

        gap_allocation_mode = False
        release_time = -1

        # Check first if this node is reserved.
        if node.reserved_application is not None:
            # Get next task needed for the job
            required_task = None
            if node.reserved_application.pending_tasks:
                required_task = node.reserved_application.pending_tasks[0]
            # Check if the reserved application still needs this reservation.
            if node.reserved_application not in self.job_queue or \
                    required_task is None or \
                    node.reserved_task_type != required_task.type:
                if LOG.isEnabledFor(logging.DEBUG):
                    LOG.debug("Released node " + str(node) + " from app " + str(node.reserved_application))
                node.reserved_application = None
                node.reserved_task_type = None
                reserved_duration = self.state.simulator.clock_millis - node.reserved_start_ms
                reserved_memory_mb = node.capacity.memory_mb - node.available.memory_mb
                self.reserved_memory_time += reserved_duration * reserved_memory_mb
                if node.available.vcores > 0:
                    self.reserved_usable_memory_time += reserved_duration * reserved_memory_mb
                node.reserved_start_ms = 0
            else:
                self.stats_decisions_inc(node.reserved_application.job_id)
                # Try to allocate the reserved application.
                alloc_result = self.allocate_on_node(node, required_task)
                if alloc_result is not None:
                    reserved_duration = self.state.simulator.clock_millis - node.reserved_start_ms
                    reserved_memory_mb = node.capacity.memory_mb - node.available.memory_mb
                    self.reserved_memory_time += reserved_duration * reserved_memory_mb
                    if node.available.vcores > 0:
                        self.reserved_usable_memory_time += reserved_duration * reserved_memory_mb
                    self.stats_accept_decisions_inc(node.reserved_application.job_id)
                    self.adjust_for_allocation(node, required_task, self.job_queue.index(node.reserved_application),
                                               alloc_result)
                    node.reserved_application = None
                    node.reserved_task_type = None
                    node.reserved_start_ms = 0
                    if not self.state.user_config.assign_multiple:
                        return True, (EventResult.CONTINUE,)
                else:
                    if self.state.use_gaps:
                        # Try to see if another app in the queue can make use of the unused slot.
                        # Compute time required for the reserved application to be scheduled.
                        required_resource = required_task.resource
                        released_resource = YarnResource(node.available.memory_mb, node.available.vcores)
                        available_resource = node.available
                        node.available = released_resource
                        release_time = -1

                        # Look at all the scheduled and running containers on this node.
                        duration_memo = {}
                        for container in sorted(node.running_containers | node.allocated_containers,
                                                key=lambda x: self.get_container_finish_time(x, duration_memo)):

                            container_finish_time = self.get_container_finish_time(container, duration_memo)
                            released_resource += container.resource
                            if container_finish_time > release_time:
                                release_time = container_finish_time

                            if release_time == sys.maxint:
                                break

                            # Check if the reserved application can now make use of this.
                            if self.allocate_on_node(node, required_task) is not None or \
                                    released_resource >= required_resource:
                                break

                        node.available = available_resource

                        # Check if the job that has the reservation is due to finish scheduling soon.
                        job_release_time = self.get_job_finish_scheduling_time(node.reserved_application)
                        if job_release_time is not None and \
                                job_release_time < release_time:
                            release_time = job_release_time

                        if release_time != -1:
                            if LOG.isEnabledFor(logging.DEBUG):
                                LOG.debug("Gap for node " + node.name + " reserved by " +
                                          node.reserved_application.get_name()
                                          + " of " + str(required_resource) + " available at " + str(release_time))
                            gap_allocation_mode = True

                    if not gap_allocation_mode:
                        self.stats_reject_decisions_inc(node.reserved_application.job_id)
                        return False, (EventResult.CONTINUE,)

        queue_idx = queue_start
        while queue_idx < len(self.job_queue):
            # Iterate over jobs in order of consumption
            job = self.job_queue[queue_idx]
            self.stats_decisions_inc(job.job_id)
            # Get next task needed for the job
            task = job.pending_tasks[0]
            # Check if the AM container was launched. In YARN this can't happen, since
            # the AM would not send ResourceRequests for any other containers before
            # the AM itself wouldn't be up and running.
            # In gap-allocation mode, check that AMs can be allocated in gaps.
            if not job.am_launched and \
                    (task.type is not YarnContainerType.MRAM or
                        (gap_allocation_mode and not self.state.user_config.gaps_allow_ams)):
                self.stats_reject_decisions_inc(job.job_id)
                queue_idx += 1
                continue
            # Check if the task fits on the current node
            # In gap-allocation mode, also check that the task would finish in time.
            alloc_result = self.allocate_on_node(node, task)
            if alloc_result is not None and \
                    (gap_allocation_mode is False or
                     self.state.simulator.clock_millis +
                     alloc_result[1] + node.hb_interval_ms <= release_time):
                self.stats_accept_decisions_inc(job.job_id)
                self.adjust_for_allocation(node, task, queue_idx, alloc_result)
                if not self.state.user_config.assign_multiple:
                    return True, (EventResult.CONTINUE,)
                else:
                    # Reset the queue_idx to take first job in order of consumption
                    queue_idx = 0
            elif self.state.user_config.use_reservations and not gap_allocation_mode:
                # Reserve this node and finish.
                self.stats_reserve_decisions_inc(job.job_id)
                if LOG.isEnabledFor(logging.DEBUG):
                    LOG.debug("Reserving node " + str(node) + " by app " + str(job))
                node.reserved_application = job
                node.reserved_task_type = task.type
                node.reserved_start_ms = self.state.simulator.clock_millis
                break
            else:
                self.stats_reject_decisions_inc(job.job_id)
                queue_idx += 1

        return bool(node.allocated_containers), (EventResult.CONTINUE,)


class YarnRegularScheduler(YarnFairScheduler):

    def __init__(self, state):
        super(YarnRegularScheduler, self).__init__(state)

    def create_container_from_task(self, node, allocated, job, task):
        if LOG.isEnabledFor(logging.DEBUG):
            LOG.debug("YARN_REGULAR_SCHEDULER: " + str(self.state.simulator.clock_millis) + " Allocated " +
                      str(task.resource.memory_mb) + " regularly for " +
                      task.job.get_name() + ":" +
                      str(task.job.next_container_id) + " with a duration of: " +
                      str(task.duration))

        return YarnScheduler.create_container_from_task(self, node, allocated, job, task)

    def allocate_on_node(self, node, task):
        if task.resource <= node.available:
            if LOG.isEnabledFor(logging.DEBUG):
                LOG.debug("YARN_REGULAR_SCHEDULER: REGULAR possible with " +
                          str(task.resource.memory_mb) + " for " +
                          task.job.get_name() + ":" +
                          str(task.job.next_container_id) + " with a duration of: " +
                          str(task.duration))
            return task.resource, task.duration

        return None

