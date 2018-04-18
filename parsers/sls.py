#!/usr/bin/env python

import re
from json import JSONDecoder
from operator import attrgetter

from models.yarn.penalties import get_penalty, YarnPenalty
from models.yarn.objects import *
from utils import lines_per_n

LINES_TO_READ = 10

TASK_NR_KEY = "c.nr"
TASK_DURATION_KEY = "c.dur"
TASK_MEMORY_KEY = "c.mem"
TASK_CORES_KEY = "c.cores"
TASK_PRIORITY_KEY = "c.prio"
TASK_TYPE_KEY = "c.type"
TASK_PENALTY_KEY = "c.penalty"
TASK_IB_KEY = "c.ib"

JOB_AM_TYPE_KEY = "am.type"
JOB_ID_KEY = "job.id"
JOB_TASKS_KEY = "job.tasks"
JOB_START_MS_KEY = "job.start.ms"
JOB_END_MS_KEY = "job.end.ms"

RACK_NAME_KEY = "rack"
RACK_NODES_KEY = "nodes"

NODE_KEY = "node"

LOG = logging.getLogger('sls_parser')


class SLSParser(object):
    def __init__(self, sls_file, topo_file, node_mem_capacity, node_core_capacity, node_hb_ms, am_hb_ms,
                 am_container_mb, am_container_cores, use_meganode, default_task_penalty):
        self.sls_file = sls_file
        self.topo_file = topo_file
        self.am_container_resource = YarnResource(am_container_mb, am_container_cores)
        self.node_resource = YarnResource(memory_mb=node_mem_capacity, vcores=node_core_capacity)
        self.am_hb_ms = am_hb_ms
        self.node_hb_ms = node_hb_ms
        self.use_meganode = use_meganode
        self.default_task_penalty = default_task_penalty

    @staticmethod
    def _print_chunk(chunk):
        for j, line in enumerate(chunk.splitlines()):
            print '{0:<5}{1}'.format(j+1, line)

    def parse_topo(self):
        """ Parse a YARN SLS topology file. This is a JSON file containing multiple rack configurations. """
        json_decoder = JSONDecoder()
        rack_objects = []
        with open(self.topo_file) as topo_file:
            lines = "".join(topo_file.readlines()).strip()
            done_parsing_file = False
            while not done_parsing_file:
                try:
                    rack_object, object_end = json_decoder.raw_decode(lines)
                except ValueError as e:
                    LOG.exception("Unable to parse topology file", exc_info=e)
                    break
                rack_objects.append(rack_object)
                if object_end != len(lines):
                    lines = lines[object_end + 1:]
                else:
                    done_parsing_file = True

        return rack_objects

    def get_yarn_topo(self):
        """ Parse a YARN SLS topology file and return a touple (racks, nodes).
        'racks' are a dict of YarnRack objects, 'nodes' are a dict of YarnNode objects. """
        racks = {}
        nodes = {}
        rack_objects = self.parse_topo()

        node_id_counter = 1

        for rack_object in rack_objects:
            rack = YarnRack(rack_object[RACK_NAME_KEY])
            if self.use_meganode:
                # Generate racks with 1 node that has all the
                # resources pooled
                resource = YarnResource(self.node_resource.memory_mb * len(rack_object[RACK_NODES_KEY]),
                                        self.node_resource.vcores * len(rack_object[RACK_NODES_KEY]))
                name = "meganode1"

                node = YarnNode(name, resource, rack, self.node_hb_ms, node_id=node_id_counter)
                node_id_counter += 1
                rack.add_node(node)
                nodes[name] = node

            else:
                for node_object in rack_object[RACK_NODES_KEY]:
                    node = YarnNode(
                            name=node_object[NODE_KEY],
                            resource=copy.copy(self.node_resource),
                            rack=rack,
                            hb_interval_ms=self.node_hb_ms,
                            node_id=node_id_counter)
                    node_id_counter += 1
                    rack.add_node(node)
                    nodes[node.name] = node

            racks[rack.name] = rack

        return racks, nodes

    def parse_sls(self):
        """ Parse a YARN SLS trace file. This is a JSON file containing multiple job objects. """
        json_decoder = JSONDecoder()
        job_objects = []
        value_error_pattern = re.compile('Expecting .+ \(char (\d+)\)$')
        with open(self.sls_file) as sls_file:
            object_chunk = ''
            last_error_idx = -1
            # Read file in chunks of lines.
            for chunk in lines_per_n(sls_file, LINES_TO_READ):
                # Remove all whitespace
                chunk = chunk.replace(" ", "")
                chunk = chunk.replace("\n", "")
                # Add (hopefully good) whitespace
                chunk = re.sub(r"{", r'{\n', chunk)
                chunk = re.sub(r"}", r'}\n', chunk)
                chunk = re.sub(r"\[", r'[\n', chunk)
                chunk = re.sub(r"\]", r']\n', chunk)

                # Further sanitize some JSON stuff
                chunk = re.sub(r"{\s*'?(\w)", r'{"\1', chunk)
                chunk = re.sub(r",\s*'?(\w)", r',"\1', chunk)
                chunk = re.sub(r"(\w)'?\s*:", r'\1":', chunk)
                chunk = re.sub(r":\s*'(\w+)'\s*([,}])", r':"\1"\2', chunk)

                object_chunk += chunk
                # Try to parse chunk read so far.
                chunk_parsing_done = False
                # Chunk may contain more than one object.
                while not chunk_parsing_done:
                    try:
                        parse_result = json_decoder.raw_decode(object_chunk)
                        last_error_idx = -1
                    except ValueError as e:
                        m = value_error_pattern.match(e.message)
                        if m:
                            # Get the index that the parsing error occurred on.
                            idx = int(m.group(1))

                            if last_error_idx == -1 or last_error_idx != idx:
                                # Chunk is not yet complete, keep reading.
                                last_error_idx = idx
                                break

                        # The error at the current index was not due to an incomplete chunk.
                        SLSParser._print_chunk(object_chunk)
                        raise e
                    # Add decoded job object to array
                    job_objects.append(parse_result[0])
                    # Check if there's trailing data from another object
                    object_end = parse_result[1]
                    if object_end != len(object_chunk):
                        # Trim chunk for the next object
                        object_chunk = object_chunk[object_end + 1:]
                    if not object_chunk.isspace():
                        chunk_parsing_done = True

        return job_objects

    def get_yarn_jobs(self):
        """ Parse the SLS trace file and return a list of YarnJob objects. """
        jobs = self.parse_sls()
        yarn_jobs = []

        for job in jobs:
            yarn_tasks = []
            # Generate the job object
            job_name = job[JOB_ID_KEY]
            # Translate "job_XXX" to an int
            job_id = int(job_name[4:])
            start_ms = -1
            start_after_job = None
            if type(job[JOB_START_MS_KEY]) is int:
                start_ms = job[JOB_START_MS_KEY]
            else:
                start_after_job = int(job[JOB_START_MS_KEY][4:])

            yarn_j = YarnJob(
                    am_type=YarnAMType.__members__.get(job[JOB_AM_TYPE_KEY].upper()),
                    name=job_name,
                    job_id=job_id,
                    start_ms=start_ms,
                    end_ms=job[JOB_END_MS_KEY],
                    am_hb_ms=self.am_hb_ms,
                    tasks=yarn_tasks,
                    after_job=start_after_job)

            # Generate an AM container
            am_container = YarnPrototypeContainer(
                    num_containers=1,
                    resource=copy.copy(self.am_container_resource),
                    priority=0,
                    container_type=YarnContainerType.MRAM,
                    job=yarn_j)
            yarn_tasks.append(am_container)

            for task in job[JOB_TASKS_KEY]:
                # Generate all the other containers
                task_penalty = self.default_task_penalty
                if TASK_PENALTY_KEY in task:
                    if TASK_IB_KEY not in task:
                        LOG.warning("Task " + str(task) + " has a penalty model defined, but no IB set. Ignoring.")
                    else:
                        task_penalty = get_penalty(YarnPenalty.__members__.get(task[TASK_PENALTY_KEY].upper()),
                                                   initial_bump=float(task[TASK_IB_KEY]))
                yarn_task = YarnPrototypeContainer(
                        num_containers=int(task[TASK_NR_KEY]),
                        duration=int(task[TASK_DURATION_KEY]),
                        resource=YarnResource(memory_mb=int(task[TASK_MEMORY_KEY]),
                                              vcores=int(task.get(TASK_CORES_KEY, '1'))),
                        priority=int(task[TASK_PRIORITY_KEY]),
                        container_type=YarnContainerType.__members__.get(task[TASK_TYPE_KEY].upper()),
                        job=yarn_j,
                        penalty=task_penalty)
                yarn_tasks.append(yarn_task)

            # Sort containers by priority
            yarn_tasks.sort(key=attrgetter('priority'))

            yarn_jobs.append(yarn_j)

        return yarn_jobs
