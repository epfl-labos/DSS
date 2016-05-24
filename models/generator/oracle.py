import copy
import heapq

import sys
from itertools import count

from more_itertools import peekable

from models.yarn.objects import YarnSimulation
from events.yarn.elastic.elastic import YarnOracleSimulationEvent, YarnResumeSchedulingEvent
from events.yarn.occupancy_stats import YarnOccupancyStatsEvent
from events.yarn.yarn import YarnJobArriveEvent, YarnNodeContainerFinishEvent
from models.state import YarnRaceState, YarnState
from schedulers.yarn import YarnSchedulerType, YarnRegularScheduler
from schedulers.elastic import YarnSmartgScheduler
from simulator import Simulator


class OracleGenerator(object):
    @staticmethod
    def generate_oracle_runner(state, log, simulator_clock, scheduler_type, simulation_type):
        generate_node_heartbeats = False
        # Duplicate all of the current state
        while True:
            try:
                deepcopy_memo = {}
                if simulation_type is YarnSimulation.RACE:
                    new_state = YarnRaceState(user_config=state.user_config)
                else:
                    new_state = YarnState(user_config=state.user_config)

                new_state.simulation_type = simulation_type

                deepcopy_memo[id(state)] = new_state
                # Add the simulator & scheduler references (many other objects reference it).
                # SIMULATOR (initial)
                new_state.simulator = Simulator()
                new_state.simulator.clock_millis = simulator_clock
                deepcopy_memo[id(state.simulator)] = new_state.simulator

                # JOBS
                # noinspection PyArgumentList
                new_state.jobs = list(copy.deepcopy(job, deepcopy_memo)
                                      for job in state.jobs if job.trace_start_ms < simulator_clock)

                # NODES
                # noinspection PyArgumentList
                new_state.nodes = dict((node.name, copy.deepcopy(node, deepcopy_memo)) for node in state.nodes.values())

                # SCHEDULER
                old_scheduler = state.scheduler
                if scheduler_type is YarnSchedulerType.REGULAR or \
                        scheduler_type is YarnSchedulerType.SMARTG:

                    # Generate the correct schedulers for each state.
                    if scheduler_type is YarnSchedulerType.REGULAR:
                        new_state.scheduler = YarnRegularScheduler(new_state)
                    elif scheduler_type is YarnSchedulerType.SMARTG:
                        new_state.scheduler = YarnSmartgScheduler(new_state, old_scheduler.node_count,
                                                                  old_scheduler.elastic_pool)
                    else:
                        log.error("Invalid scheduler type: " + str(scheduler_type))
                        return None

                    # Copy the scheduler state to the new scheduler
                    new_scheduler = new_state.scheduler
                    new_scheduler.next_job_id = old_scheduler.next_job_id
                    # noinspection PyArgumentList
                    new_scheduler.running_jobs = set(copy.deepcopy(job, deepcopy_memo)
                                                     for job in old_scheduler.running_jobs)
                    new_scheduler.completed_jobs = old_scheduler.completed_jobs.copy()
                    # noinspection PyArgumentList
                    new_scheduler.allocated_containers = \
                        dict((job_id, set(copy.deepcopy(container, deepcopy_memo)
                                          for container in old_scheduler.allocated_containers[job_id]))
                             for job_id in old_scheduler.allocated_containers)
                    # noinspection PyArgumentList
                    new_scheduler.job_queue = list(copy.deepcopy(job, deepcopy_memo) for job in old_scheduler.job_queue)
                else:
                    # noinspection PyArgumentList
                    new_state.scheduler = copy.deepcopy(state.scheduler, deepcopy_memo)

                deepcopy_memo[id(old_scheduler)] = new_state.scheduler

                # GENERATOR
                old_generator = state.generator
                new_generator = old_generator.__class__(state)
                new_generator.state = new_state
                deepcopy_memo[id(old_generator)] = new_generator
                new_state.generator = new_generator

                # SIMULATOR (copy state)
                new_state.simulator.queue.counter = peekable(count(state.simulator.queue.counter.peek()))
                new_state.simulator.queue.pq = []
                # Also eliminate all the task duration errors injected
                # (the simulation needs to be unaware of them)
                queue_needs_resorting = False
                for queue_el in state.simulator.queue.pq:
                    # queue_el = [ time_millis, tie_breaker_count, event ]
                    event_time = queue_el[0]
                    event_count = queue_el[1]
                    event = queue_el[2]
                    new_event = None
                    if (issubclass(type(event), YarnJobArriveEvent)) or \
                            issubclass(type(event), YarnOracleSimulationEvent) or \
                            type(event) is YarnOccupancyStatsEvent or \
                            type(event) is YarnResumeSchedulingEvent:

                        if type(event) is YarnResumeSchedulingEvent:
                            generate_node_heartbeats = True
                    else:
                        # noinspection PyArgumentList
                        new_event = copy.deepcopy(event, deepcopy_memo)
                        if type(event) is YarnNodeContainerFinishEvent and event.duration_error != 0:
                            event_time -= event.duration_error
                            new_event.time_millis -= event.duration_error
                            queue_needs_resorting = True

                    new_state.simulator.queue.pq.append([event_time, event_count, new_event])

                if queue_needs_resorting:
                    heapq.heapify(new_state.simulator.queue.pq)

                # Copy elements which do not require duplication
                new_state.user_config = state.user_config
                break
            except RuntimeError as e:
                if "recursion depth" in str(e):
                    old_limit = sys.getrecursionlimit()
                    log.warning("Increasing recursion limit from " + str(old_limit) + " to " + str(old_limit * 2))
                    sys.setrecursionlimit(old_limit * 2)
                else:
                    raise e

        if generate_node_heartbeats:
            # Generate NodeHeartbeat events for all the nodes.
            map(lambda x: x.next_heartbeat.generate_next_heartbeat(),
                (node for node in new_state.nodes.values() if node.next_heartbeat.handled))

        # Disable the use of gaps.
        new_state.use_gaps = False

        log.info("ORACLE_STATE_DUPLICATION_DONE")

        return new_state
