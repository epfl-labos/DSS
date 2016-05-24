#!/usr/bin/env python

import copy
import logging
from functools import total_ordering
from random import Random

from utils import PEnum

LOG = logging.getLogger('yarn_objects')

YarnAMType = PEnum("YarnAMType", "MAPREDUCE GENERIC")
YarnContainerType = PEnum("YarnContainerType", "MRAM MAP REDUCE")
YarnErrorType = PEnum("YarnErrorType", "MIXED POSITIVE NEGATIVE")
YarnErrorMode = PEnum("YarnErrorMode", "CONSTANT RANDOM")
YarnSimulation = PEnum("YarnSimulation", "STANDALONE SYMBEX RACE PEEK")

JOB_RANDOM_SEED = 129831982379


@total_ordering
class YarnResource(object):
    def __init__(self, memory_mb, vcores):
        self.memory_mb = memory_mb
        self.vcores = vcores

    def __copy__(self):
        return YarnResource(self.memory_mb, self.vcores)

    def __deepcopy__(self, memo):
        return self.__copy__()

    def __add__(self, other):
        return YarnResource(self.memory_mb + other.memory_mb, self.vcores + other.vcores)

    def __sub__(self, other):
        return YarnResource(self.memory_mb - other.memory_mb, self.vcores - other.vcores)

    def __iadd__(self, other):
        self.memory_mb += other.memory_mb
        self.vcores += other.vcores
        return self

    def __isub__(self, other):
        self.memory_mb -= other.memory_mb
        self.vcores -= other.vcores
        return self

    def __ne__(self, other):
        return not self.__eq__(other)

    def __eq__(self, other):
        return self.memory_mb == other.memory_mb and self.vcores == other.vcores

    def __le__(self, other):
        return self.memory_mb <= other.memory_mb and self.vcores <= other.vcores

    def __lt__(self, other):
        return self.memory_mb < other.memory_mb or \
               (self.memory_mb == other.memory_mb and self.vcores < other.vcores)

    def __str__(self):
        return "<{}MB, {} cores>".format(self.memory_mb, self.vcores)

    def __repr__(self):
        return self.__str__()


class YarnRack(object):
    def __init__(self, name):
        self.name = name
        self.nodes = set()
        self.capacity = YarnResource(0, 0)
        self.available = YarnResource(0, 0)

    def __deepcopy__(self, memo):
        return self

    def add_node(self, node):
        self.nodes.add(node)
        self.capacity += node.capacity
        self.available += node.capacity


class YarnNode(object):
    def __init__(self, name, resource, rack, hb_interval_ms, node_id):
        # Parameters from SLS
        self.capacity = YarnResource(resource.memory_mb, resource.vcores)
        self.rack = rack
        self.name = name
        self.hb_interval_ms = hb_interval_ms
        # Parameters for YARN execution
        self.node_id = node_id
        self.available = copy.copy(resource)
        self.allocated_containers = set()
        self.running_containers = set()
        self.next_heartbeat = None
        self.reserved_application = None
        self.reserved_task_type = None
        self.reserved_start_ms = 0

    # noinspection PyArgumentList
    def __deepcopy__(self, memo):
        new_node = YarnNode(self.name, self.capacity, self.rack, self.hb_interval_ms, self.node_id)
        memo[id(self)] = new_node
        new_node.available = YarnResource(self.available.memory_mb, self.available.vcores)
        new_node.allocated_containers = set(copy.deepcopy(container, memo) for container in self.allocated_containers)
        new_node.running_containers = set(copy.deepcopy(container, memo) for container in self.running_containers)
        new_node.next_heartbeat = copy.deepcopy(self.next_heartbeat, memo)
        new_node.reserved_application = copy.deepcopy(self.reserved_application, memo)
        new_node.reserved_task_type = self.reserved_task_type
        return new_node

    def __str__(self):
        return "<{}/{}, {} app reserved: {}>".format(self.rack.name, self.name, self.available,
                                                     self.reserved_application)

    def __repr__(self):
        return self.__str__()

    def book_container(self, container):
        # Adjust available resource
        self.available -= container.resource
        # Adjust rack available resource
        self.rack.available -= container.resource
        # Add container to the list of allocated containers
        self.allocated_containers.add(container)

    def launch_container(self, container):
        if container not in self.allocated_containers:
            LOG.error("Tried to launch unallocated container " + str(container))
            raise Exception("Tried to launch unallocated container")

        # Remove container from allocated list
        self.allocated_containers.remove(container)
        # Add container to running list
        self.running_containers.add(container)

    def remove_container(self, container):
        # Adjust available resource
        self.available += container.resource
        # Adjust rack available resource
        self.rack.available += container.resource
        # Remove container from running list
        self.running_containers.remove(container)


class YarnContainer(object):
    def __init__(self, resource, priority, container_type, job, duration=0, penalty=None):
        self.duration = duration
        self.resource = resource
        self.priority = priority
        self.type = container_type
        self.job = job
        self.penalty = penalty

    # noinspection PyArgumentList
    def __deepcopy__(self, memo):
        new_container = self.__class__(resource=self.resource, priority=self.priority, container_type=self.type,
                                       job=self.job, duration=self.duration)
        new_container.penalty = self.penalty
        memo[id(self)] = new_container
        new_container.resource = copy.deepcopy(self.resource, memo)
        new_container.priority = copy.deepcopy(self.priority, memo)
        new_container.job = copy.deepcopy(self.job, memo)
        return new_container

    def __str__(self):
        return "<{}({}): {}ms, {}, penalty: {}>".format(self.type.name, self.priority,
                                                        self.duration, self.resource, self.penalty)

    def __repr__(self):
        return self.__str__()


class YarnRunningContainer(YarnContainer):
    def __init__(self, resource, priority, container_type, job, node=None, task=None, duration=0):
        super(YarnRunningContainer, self).__init__(resource, priority, container_type, job, duration,
                                                   task.penalty if task is not None else None)
        self.id = job.get_new_container_id()
        self.node = node
        self.task = task
        self.scheduled_time_millis = -1
        self.launched_time_millis = -1
        self.finished_time_millis = -1
        self.duration_error = 0

    # noinspection PyArgumentList
    def __deepcopy__(self, memo):
        new_container = super(YarnRunningContainer, self).__deepcopy__(memo)
        new_container.id = self.id
        new_container.scheduled_time_millis = self.scheduled_time_millis
        new_container.launched_time_millis = self.launched_time_millis
        new_container.finished_time_millis = self.finished_time_millis
        new_container.node = copy.deepcopy(self.node, memo)
        new_container.task = copy.deepcopy(self.task, memo)

        return new_container

    def __str__(self):
        return "<{} {}>".format(self.name, YarnContainer.__str__(self)[1:-1])

    @property
    def name(self):
        return "{}:{}".format(self.job.name, self.id)

    def schedule_container(self, time_millis):
        self.scheduled_time_millis = time_millis

    def launch_container(self, time_millis):
        self.launched_time_millis = time_millis

    def finish_container(self, time_millis):
        self.finished_time_millis = time_millis

    @property
    def duration_ms(self):
        if self.launched_time_millis == -1:
            raise Exception("Container {}:{} not launched yet.".format(self.job.name, self.id))
        if self.finished_time_millis == -1:
            raise Exception("Container {}:{} not finished yet.".format(self.job.name, self.id))
        return self.finished_time_millis - self.launched_time_millis


class YarnPrototypeContainer(YarnContainer):
    def __init__(self, resource, priority, container_type, job, duration=0, num_containers=0, ideal_resource=None,
                 penalty=None):
        super(YarnPrototypeContainer, self).__init__(resource, priority, container_type, job, duration, penalty)
        self.num_containers = num_containers
        self.ideal_resource = ideal_resource if ideal_resource is not None else copy.copy(resource)

    # noinspection PyArgumentList
    def __deepcopy__(self, memo):
        new_container = super(YarnPrototypeContainer, self).__deepcopy__(memo)
        new_container.num_containers = self.num_containers
        new_container.ideal_resource = copy.deepcopy(self.ideal_resource, memo)
        return new_container

    def __str__(self):
        return "{}, {} tasks{}>".format(
            super(YarnPrototypeContainer, self).__str__()[:-1],
            self.num_containers,
            "" if self.resource == self.ideal_resource else ", ideal: {}".format(self.ideal_resource)
        )


class YarnJob(object):
    def __init__(self, am_type, name, job_id, start_ms, end_ms, am_hb_ms, tasks, after_job=None):
        # Parameters from SLS trace
        self.am_type = am_type
        self.name = name
        self.job_id = job_id
        self.trace_start_ms = start_ms
        self.trace_end_ms = end_ms
        self.tasks = tasks
        self.am_hb_ms = am_hb_ms
        self.after_job = after_job

        # Parameters for YARN execution
        self.consumption = YarnResource(0, 0)
        self.pending_tasks = None  # Gets set in the YarnGenerator.
        self.running_tasks = set()
        self.finished_tasks = set()
        self.start_ms = 0
        self.end_ms = 0
        self.yarn_id = 0
        self.am_launched = False
        self.am_next_heartbeat = None
        self.next_container_id = 1
        self.finished = False
        self.random = Random(JOB_RANDOM_SEED)

    # noinspection PyArgumentList
    def __deepcopy__(self, memo):
        new_job = YarnJob(self.am_type, self.name, self.job_id, self.trace_start_ms, self.trace_end_ms, self.am_hb_ms,
                          self.tasks, self.after_job)
        new_job.start_ms = self.start_ms
        new_job.end_ms = self.end_ms
        new_job.yarn_id = self.yarn_id
        new_job.am_launched = self.am_launched
        new_job.next_container_id = self.next_container_id
        new_job.finished = self.finished

        memo[id(self)] = new_job
        new_job.pending_tasks = list(copy.deepcopy(task, memo) for task in self.pending_tasks)
        new_job.consumption = YarnResource(self.consumption.memory_mb, self.consumption.vcores)
        new_job.running_tasks = set(copy.deepcopy(task, memo) for task in self.running_tasks)
        new_job.finished_tasks = self.finished_tasks.copy()
        new_job.am_next_heartbeat = copy.deepcopy(self.am_next_heartbeat, memo)
        new_job.random = Random(JOB_RANDOM_SEED)

        return new_job

    def get_name(self):
        return "{} app_{:04d}".format(self.name, self.yarn_id)

    def get_new_container_id(self):
        result = self.next_container_id
        self.next_container_id += 1
        return result

    @property
    def duration_ms(self):
        if not self.finished:
            raise Exception(' '.join(["Job", self.name, "not finished yet."]))
        return self.end_ms - self.start_ms

    def __str__(self):
        return "<{}, {}, [{}]>".format(self.get_name(), self.am_type.name,
                                       ", ".join(str(task) for task in self.tasks))

    def __repr__(self):
        return self.__str__()
