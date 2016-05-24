#!/usr/bin/env python
import logging
from random import Random

from events.yarn.elastic.peek import YarnPeekJobArriveEvent, YarnPeekSimulationFinishEvent,\
    YarnPeekSimulationStartEvent, YarnPeekStandaloneSimulationFinishEvent
from events.yarn.elastic.race import YarnRaceSimulationFinishEvent, YarnRaceContinuousStandaloneSimulationFinishEvent, \
    YarnRaceSynchronousJobArriveEvent, YarnRaceJobJobArriveEvent, YarnRaceNodeGJobArriveEvent
from events.yarn.symbex import YarnSymbexSimulationFinishEvent, YarnSymbexSimulationStartEvent, SymbexMode
from events.yarn.yarn import YarnStandaloneSimulationFinishEvent, YarnJobArriveEvent, YarnSimulationStartEvent
from models.state import SymbexState, YarnState
from models.yarn.objects import YarnContainerType, YarnSimulation, YarnPrototypeContainer, YarnResource
from models.yarn.penalties import get_penalty
from parsers.sls import SLSParser
from schedulers.oracle import YarnGlobalDecisionScheduler, YarnJobDecisionScheduler, YarnNodeGDecisionScheduler, \
    YarnPeekScheduler
from schedulers.srtf import YarnSRTFScheduler
from schedulers.symbex import YarnSymbexScheduler
from schedulers.elastic import YARN_MIN_ALLOCATION_MB, YarnGreedyScheduler, YarnSmartgScheduler
from schedulers.yarn import YarnSchedulerType, YarnRegularScheduler

LOG = logging.getLogger("yarn_generator")

GENERATOR_RANDOM_SEED = 23482374928374


class YarnGenerator(object):
    """ The YarnGenerator will create a list of YarnJob objects
    from the output of the SLSParser.
    Items added to the state:
    - the node configuration
    - the rack configuration
    - the scheduler used
    """

    def __init__(self, state=None):
        self.state = state
        self.random = Random(GENERATOR_RANDOM_SEED)

    def generate_state(self, simulator, args):

        user_config = args

        # Check if this is a SYMBEX simulation.
        if user_config.symbex:
            self.state = SymbexState(user_config=user_config)
            # Override scheduler type
            if user_config.symbex_mode is SymbexMode.DECISION:
                self.state.scheduler_type = YarnSchedulerType.SYMBEX
            elif user_config.symbex_mode is SymbexMode.RACE:
                self.state.scheduler_type = YarnSchedulerType.SMARTG

        else:
            self.state = YarnState(user_config=user_config)
            self.state.scheduler_type = user_config.scheduler_type

        self.state.user_config = user_config
        self.state.simulator = simulator

        # Get the default penalty model, if set
        default_task_penalty = None
        if user_config.penalty is not None and user_config.ib is not None:
            default_task_penalty = get_penalty(user_config.penalty, user_config.ib)

        # Create a new SLSParser to read the simulation data
        sls_parser = SLSParser(
            sls_file=user_config.sls_trace_file,
            topo_file=user_config.network_topology_file,
            node_mem_capacity=user_config.node_mem_mb,
            node_core_capacity=user_config.node_cores,
            node_hb_ms=user_config.node_hb_ms,
            am_hb_ms=user_config.am_hb_ms,
            am_container_mb=user_config.am_container_mb,
            am_container_cores=user_config.am_container_cores,
            use_meganode=user_config.meganode,
            default_task_penalty=default_task_penalty)

        # Call the SLSParser to read the config files
        yarn_jobs = sls_parser.get_yarn_jobs()
        yarn_topo = sls_parser.get_yarn_topo()

        # Parse jobs and add random user overestimation of memory
        overestimation_range = user_config.mem_overestimate_range
        for job in yarn_jobs:
            overestimation_factor = self.random.uniform(overestimation_range[0], overestimation_range[1])
            for task in job.tasks:
                if task.type is YarnContainerType.MRAM:
                    continue
                if task.resource.memory_mb < YARN_MIN_ALLOCATION_MB:
                    raise Exception("Job " + str(job) + " has tasks with memory" +
                                    " less than the minimum allocatable amount (" + str(YARN_MIN_ALLOCATION_MB) +
                                    ") : " + str(task.resource.memory_mb))
                task.ideal_resource.memory_mb = \
                    max(int(task.resource.memory_mb / overestimation_factor),
                        YARN_MIN_ALLOCATION_MB)
            job.pending_tasks = list(
                YarnPrototypeContainer(resource=YarnResource(container.resource.memory_mb, container.resource.vcores),
                                       priority=container.priority, container_type=container.type, job=job,
                                       duration=container.duration, num_containers=container.num_containers,
                                       ideal_resource=container.ideal_resource, penalty=container.penalty)
                for container in job.tasks)

        # Add node config
        self.state.jobs = yarn_jobs
        self.state.racks = yarn_topo[0]
        self.state.nodes = yarn_topo[1]

        # Create and add the scheduler
        self.state.scheduler = self._get_scheduler(user_config, yarn_topo)

        # Add the generator itself
        self.state.generator = self

        return self.state

    def _get_scheduler(self, user_config, yarn_topo):
        elastic_pool = None
        if user_config.ep is not None:
            elastic_pool = user_config.ep
        scheduler_type = self.state.scheduler_type
        if scheduler_type is YarnSchedulerType.REGULAR:
            return YarnRegularScheduler(state=self.state)
        elif scheduler_type is YarnSchedulerType.SRTF:
            return YarnSRTFScheduler(state=self.state)
        elif scheduler_type is YarnSchedulerType.GREEDY:
            return YarnGreedyScheduler(state=self.state, node_count=len(yarn_topo[1]), elastic_pool=elastic_pool)
        elif scheduler_type is YarnSchedulerType.SMARTG:
            return YarnSmartgScheduler(state=self.state, node_count=len(yarn_topo[1]), elastic_pool=elastic_pool)
        elif scheduler_type is YarnSchedulerType.SYMBEX:
            return YarnSymbexScheduler(state=self.state)
        elif scheduler_type is YarnSchedulerType.RACE_LOCKSTEP or \
                scheduler_type is YarnSchedulerType.RACE_CONTINUOUS:
            self.state.oracle_type = scheduler_type
            return YarnGlobalDecisionScheduler(state=self.state)
        elif scheduler_type is YarnSchedulerType.RACE_JOB:
            self.state.oracle_type = scheduler_type
            return YarnJobDecisionScheduler(state=self.state)
        elif scheduler_type is YarnSchedulerType.RACE_NODEG:
            self.state.oracle_type = scheduler_type
            return YarnNodeGDecisionScheduler(state=self.state)
        elif scheduler_type is YarnSchedulerType.PEEK:
            self.state.oracle_type = scheduler_type
            return YarnPeekScheduler(state=self.state)
        else:
            raise Exception("Invalid scheduler model specified: " + scheduler_type)

    def get_simulation_start_event(self):
        if self.state.user_config.symbex:
            return YarnSymbexSimulationStartEvent(self.state)

        if self.state.is_oracle_simulation:
            if self.state.oracle_type is YarnSchedulerType.RACE_CONTINUOUS:
                return YarnRaceContinuousStandaloneSimulationFinishEvent(self.state)
            elif self.state.oracle_type is YarnSchedulerType.PEEK:
                return YarnPeekSimulationStartEvent(self.state)

        return YarnSimulationStartEvent(self.state)

    def get_simulation_finish_event(self):
        if self.state.simulation_type is YarnSimulation.STANDALONE:
            if self.state.oracle_type is YarnSchedulerType.RACE_CONTINUOUS:
                return YarnRaceContinuousStandaloneSimulationFinishEvent(self.state)
            elif self.state.oracle_type is YarnSchedulerType.PEEK:
                return YarnPeekStandaloneSimulationFinishEvent(self.state)
            else:
                return YarnStandaloneSimulationFinishEvent(self.state)
        elif self.state.simulation_type is YarnSimulation.SYMBEX:
            return YarnSymbexSimulationFinishEvent(self.state)
        elif self.state.simulation_type is YarnSimulation.RACE:
            return YarnRaceSimulationFinishEvent(self.state)
        elif self.state.simulation_type is YarnSimulation.PEEK:
            return YarnPeekSimulationFinishEvent(self.state)

    def get_job_arrive_event(self, job):
        if self.state.is_oracle_simulation and self.state.simulation_type is YarnSimulation.STANDALONE:
            if self.state.oracle_type is YarnSchedulerType.RACE_LOCKSTEP:
                return YarnRaceSynchronousJobArriveEvent(self.state, job)
            elif self.state.oracle_type is YarnSchedulerType.RACE_JOB:
                return YarnRaceJobJobArriveEvent(self.state, job)
            elif self.state.oracle_type is YarnSchedulerType.RACE_NODEG:
                return YarnRaceNodeGJobArriveEvent(self.state, job)
            elif self.state.oracle_type is YarnSchedulerType.PEEK:
                return YarnPeekJobArriveEvent(self.state, job)
            else:
                raise Exception("Invalid RACE simulation.")

        else:
            return YarnJobArriveEvent(self.state, job)
