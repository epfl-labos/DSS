import copy
import logging
import math
from abc import ABCMeta, abstractmethod
from random import Random

from models.yarn.objects import YarnSimulation
from events.event import EventResult, Event
from events.yarn.elastic.elastic import YarnOracleSimulationEvent, YarnResumeSchedulingEvent
from events.yarn.yarn import YarnSimulationFinishEvent, YarnSimulationStartEvent, YarnStandaloneSimulationFinishEvent, \
    YarnJobArriveEvent
from models.generator.oracle import OracleGenerator
from schedulers.yarn import YarnSchedulerType
from utils import PEnum

YarnRaceMetric = PEnum("YarnRaceMetric", "TOTAL_JRT MAKESPAN CDF_50 CDF_75 CDF_90 CDF_99")


class YarnRaceJobArriveEvent(YarnJobArriveEvent):
    __metaclass__ = ABCMeta

    def __init__(self, state, job):
        super(YarnRaceJobArriveEvent, self).__init__(state, job)

    @abstractmethod
    def get_next_simulation_event(self):
        raise NotImplementedError()

    def handle(self):
        result = super(YarnRaceJobArriveEvent, self).handle()

        # Check if there is already a race simulation event in the queue
        if not hasattr(self.state.scheduler, "next_simulation_event"):
            logging.error("Invalid scheduler used for a RACE strategy.")
            return result

        if self.state.scheduler.next_simulation_event is None:
            self.state.scheduler.next_simulation_event = self.get_next_simulation_event()
            self.state.simulator.add_event(self.state.scheduler.next_simulation_event)

        return result


class YarnRaceSynchronousJobArriveEvent(YarnRaceJobArriveEvent):

    def __init__(self, state, job):
        super(YarnRaceSynchronousJobArriveEvent, self).__init__(state, job)

    def get_next_simulation_event(self):
        return YarnRaceSynchronousSimulationEvent(self.state)


class YarnRaceJobJobArriveEvent(YarnRaceJobArriveEvent):

    def __init__(self, state, job):
        super(YarnRaceJobJobArriveEvent, self).__init__(state, job)

    def get_next_simulation_event(self):
        return YarnRaceJobSimulationEvent(self.state, self.job)


class YarnRaceNodeGJobArriveEvent(YarnRaceJobArriveEvent):

    def __init__(self, state, job):
        super(YarnRaceNodeGJobArriveEvent, self).__init__(state, job)

    def get_next_simulation_event(self):
        return YarnRaceNodeGSimulationEvent(self.state)


class YarnRaceContinuousSimulationStartEvent(YarnSimulationStartEvent):
    def __init__(self, state):
        super(YarnRaceContinuousSimulationStartEvent, self).__init__(state)

    def handle(self):
        result = super(YarnRaceContinuousSimulationStartEvent, self).handle()
        # Check if RACE simulations are supposed to have a duration
        race_duration_range = self.state.user_config.race_duration_range
        race_duration = YarnRaceSimulationEvent.get_race_duration(race_duration_range, 1)
        # Generate the next simulation point.
        next_race_simulation = YarnRaceContinuousSimulationEvent(self.state)
        next_race_simulation.time_millis = self.state.simulator.clock_millis + race_duration
        self.state.simulator.add_event(next_race_simulation)

        return result


class YarnRaceContinuousStandaloneSimulationFinishEvent(YarnStandaloneSimulationFinishEvent):
    def __init__(self, state):
        super(YarnRaceContinuousStandaloneSimulationFinishEvent, self).__init__(state)

    def cancel_recurring_events(self):
        super(YarnRaceContinuousStandaloneSimulationFinishEvent, self).cancel_recurring_events()

        if not hasattr(self.state.scheduler, "next_simulation_event"):
            logging.error("Invalid scheduler used for RACE_CONTINUOUS.")
            return

        next_simulation_event = self.state.scheduler.next_simulation_event
        if next_simulation_event is not None:
            self.state.simulator.queue.unsafe_remove(next_simulation_event)


class YarnRaceSimulationFinishEvent(YarnSimulationFinishEvent):
    def __init__(self, state):
        super(YarnRaceSimulationFinishEvent, self).__init__(state)

    def handle(self):
        super(YarnRaceSimulationFinishEvent, self).handle()
        # Compute and write out metric value (total job running time)
        race_result = 0

        if self.state.user_config.race_metric is YarnRaceMetric.TOTAL_JRT:
            total_running_time = 0
            for job in sorted(self.state.scheduler.completed_jobs, key=lambda x: x.job_id):
                total_running_time += job.end_ms - job.start_ms

            race_result = total_running_time
        elif self.state.user_config.race_metric is YarnRaceMetric.MAKESPAN:
            race_result = max(self.state.scheduler.completed_jobs, key=lambda x: x.end_ms).end_ms - \
                          min(self.state.scheduler.completed_jobs, key=lambda x: x.start_ms).start_ms
        elif self.state.user_config.race_metric is YarnRaceMetric.CDF_50 or \
            self.state.user_config.race_metric is YarnRaceMetric.CDF_75 or \
            self.state.user_config.race_metric is YarnRaceMetric.CDF_90 or \
                self.state.user_config.race_metric is YarnRaceMetric.CDF_99:

            # Sort jobs by completion time
            sorted_jobs = sorted(self.state.scheduler.completed_jobs, key=lambda x: x.end_ms - x.start_ms)

            cdf_perc = 0
            if self.state.user_config.race_metric is YarnRaceMetric.CDF_50:
                cdf_perc = 50
            elif self.state.user_config.race_metric is YarnRaceMetric.CDF_75:
                cdf_perc = 75
            elif self.state.user_config.race_metric is YarnRaceMetric.CDF_90:
                cdf_perc = 90
            elif self.state.user_config.race_metric is YarnRaceMetric.CDF_99:
                cdf_perc = 99

            index = cdf_perc * len(sorted_jobs) / 100.0
            if not index.is_integer():
                index = int(math.ceil(index))
                race_result = sorted_jobs[index - 1].end_ms - sorted_jobs[index - 1].start_ms
            else:
                index = int(index)
                race_result = sorted_jobs[index - 1].end_ms - sorted_jobs[index - 1].start_ms
                if index < len(sorted_jobs) - 1:
                    race_result += sorted_jobs[index].end_ms - sorted_jobs[index].start_ms
                    race_result /= 2

        self.state.race_result = race_result

        return EventResult.FINISHED,


RACE_DURATION_RANDOM_SEED = 23948209348


class YarnRaceSimulationEvent(YarnOracleSimulationEvent):
    def __init__(self, state):
        Event.__init__(self, state)

    def __deepcopy__(self, memo):
        new_event = copy.copy(self)
        return new_event

    @staticmethod
    def generate_race_runner(state, log, simulator_clock, scheduler_type):
        new_state = OracleGenerator.generate_oracle_runner(state, log, simulator_clock, scheduler_type,
                                                           YarnSimulation.RACE)
        # Check if a cut-off limit has been set.
        if state.user_config.race_cutoff_perc != 0:
            total_containers_remaining = 0
            # Count how many containers are left to be scheduled.
            for job in state.scheduler.job_queue:
                for task in job.pending_tasks:
                    total_containers_remaining += task.num_containers

            containers_to_schedule = \
                total_containers_remaining * state.user_config.race_cutoff_perc / 100.0
            new_state.race_containers_to_simulate = containers_to_schedule

        # Make sure the RACE scheduler is enabled (ResumeSchedulingEvents are cancelled for simulations)
        new_state.scheduler.allow_scheduling = True

        return new_state

    random = Random(RACE_DURATION_RANDOM_SEED)

    @staticmethod
    def get_race_duration(race_duration_range, min_duration=0):
        if race_duration_range[0] == 0 and \
                        race_duration_range[1] == 0:
            race_duration = min_duration
        else:
            race_duration = YarnRaceSimulationEvent.random.randint(int(race_duration_range[0]),
                                                                   int(race_duration_range[1]))

        return race_duration

    @staticmethod
    def check_degradation(regular_state, elastic_state):
        for elastic_job in elastic_state.jobs:
            elastic_dur = elastic_job.end_ms - elastic_job.start_ms
            regular_job = filter(lambda x: x.job_id == elastic_job.job_id, regular_state.jobs)[0]
            regular_dur = regular_job.end_ms - regular_job.start_ms
            if elastic_dur > regular_dur:
                return True

        return False

    def run_binary_simulation(self):

        # Generate the new simulators.
        regular_state = YarnRaceSimulationEvent.generate_race_runner(self.state, self.log,
                                                                     self.state.simulator.clock_millis,
                                                                     YarnSchedulerType.REGULAR)
        elastic_state = YarnRaceSimulationEvent.generate_race_runner(self.state, self.log,
                                                                     self.state.simulator.clock_millis,
                                                                     YarnSchedulerType.SMARTG)

        # Run the simulations.
        debug_flag = self.state.user_config.oracle_debug
        with YarnRaceSimulationEvent.nested_logging("RACE REGULAR", debug_flag):
            regular_state.state.run()
        with YarnRaceSimulationEvent.nested_logging("RACE SMARTG", debug_flag):
            elastic_state.state.run()

        # Compare the results.
        if regular_state.race_result is None or elastic_state.race_result is None:
            # No jobs started yet. Ignore.
            return None

        regular_result = regular_state.race_result
        elastic_result = elastic_state.race_result

        if self.log.isEnabledFor(logging.INFO):
            self.log.info("YARN_RACE_SIMULATION_EVENT: REGULAR: " + str(regular_result) + ", SMARTG: "
                          + str(elastic_result) + ". Winner: "
                          + ("REGULAR" if regular_result <= elastic_result else "SMARTG"))

        # If the 'never degrade' option is set, check if any jobs take longer with SMARTG.
        # If that is the case, opt to be REGULAR instead.
        if self.state.user_config.race_never_degrade:
            degradation_exists = YarnRaceSimulationEvent.check_degradation(regular_state, elastic_state)

            if degradation_exists:
                self.log.info("YARN_RACE_SIMULATION_EVENT: SMARTG causes at least one"
                              + " job running time to degrade. REGULAR wins.")
                return YarnSchedulerType.REGULAR

        if regular_result != elastic_result:
            return YarnSchedulerType.REGULAR if regular_result < elastic_result else YarnSchedulerType.SMARTG
        else:
            return None

    def handle(self):
        raise NotImplementedError()


class YarnRaceContinuousSimulationEvent(YarnRaceSimulationEvent):
    def __init__(self, state, prev_winner=None):
        YarnRaceSimulationEvent.__init__(self, state)
        self.prev_winner = prev_winner

    def handle(self):

        # If the previous RACE needs to enact a change, do it here.
        if self.prev_winner is not None:
            self.log.info("YARN_RACE_CONTINUOUS_SIMULATION_EVENT: Changing scheduler behavior to " +
                          str(self.prev_winner))
            self.state.scheduler.behavior = self.prev_winner

        # Check if there are anymore tasks to schedule.
        winner = None
        if self.state.scheduler.job_queue:
            winner = self.run_binary_simulation()

        # Check if RACE simulations are supposed to have a duration
        race_duration_range = self.state.user_config.race_duration_range
        race_duration = YarnRaceSimulationEvent.get_race_duration(race_duration_range, 1)

        # Generate the next simulation point.
        next_race_simulation = YarnRaceContinuousSimulationEvent(self.state, winner)
        next_race_simulation.time_millis = self.state.simulator.clock_millis + race_duration
        self.state.scheduler.next_simulation_event = next_race_simulation
        self.state.simulator.add_event(next_race_simulation)

        return EventResult.CONTINUE,


class YarnRaceSynchronousSimulationEvent(YarnRaceSimulationEvent):
    def __init__(self, state):
        YarnRaceSimulationEvent.__init__(self, state)

    def postpone_scheduling(self):
        # Check if RACE simulations are supposed to have a duration
        race_duration_range = self.state.user_config.race_duration_range
        if race_duration_range[0] == 0 and race_duration_range[1] == 0:
            return False

        if self.state.user_config.race_lockstep_regular:
            # Switch flag back to REGULAR
            self.state.scheduler.behavior = YarnSchedulerType.REGULAR
        else:
            # Disable scheduling
            self.state.scheduler.allow_scheduling = False

        # Reenable scheduling at a point where, presumably, the RACE simulation has ended.
        race_duration = YarnRaceSimulationEvent.random.randint(int(race_duration_range[0]),
                                                               int(race_duration_range[1]))

        # Check for and disable any previous ResumeScheduling events that are due to occur
        # in the future.
        scheduling_resume_event = YarnResumeSchedulingEvent(self.state)
        scheduling_resume_event.time_millis = self.state.simulator.clock_millis + race_duration
        existing_resume_event = self.state.race_next_resume_event
        if existing_resume_event is None or \
                existing_resume_event.time_millis < scheduling_resume_event.time_millis:
            self.state.simulator.add_event(scheduling_resume_event)
            if existing_resume_event is not None:
                self.state.simulator.queue.safe_remove(existing_resume_event)
            self.state.race_next_resume_event = scheduling_resume_event

        return True

    def handle(self):
        # Synchronous event, postpone scheduling
        postponed = self.postpone_scheduling()

        # Run simulation.
        winner = self.run_binary_simulation()
        if winner is not None:
            if not self.state.user_config.race_lockstep_regular or not postponed:
                # Adjust scheduler to the new winning strategy.
                self.state.scheduler.behavior = winner
            else:
                self.state.race_next_resume_event.winner = winner

        return EventResult.CONTINUE,


class YarnRaceJobSimulationEvent(YarnRaceSynchronousSimulationEvent):
    def __init__(self, state, job):
        self.job = job
        YarnRaceSimulationEvent.__init__(self, state)

    # noinspection PyArgumentList
    def __deepcopy__(self, memo):
        new_event = copy.copy(self)
        memo[id(self)] = new_event
        new_event.job = copy.deepcopy(self.job, memo)
        return new_event

    def handle(self):
        # Synchronous event, postpone scheduling
        postponed = self.postpone_scheduling()

        # Generate the new simulators.
        regular_state = YarnRaceSimulationEvent.generate_race_runner(self.state, self.log,
                                                                     self.state.simulator.clock_millis,
                                                                     YarnSchedulerType.RACE_JOB)
        elastic_state = YarnRaceSimulationEvent.generate_race_runner(self.state, self.log,
                                                                     self.state.simulator.clock_millis,
                                                                     YarnSchedulerType.RACE_JOB)
        elastic_state.scheduler.set_behavior(self.job, YarnSchedulerType.SMARTG)

        # Run the simulations.
        debug_flag = self.state.user_config.oracle_debug
        with YarnRaceSimulationEvent.nested_logging("RACE JOB " + self.job.name + " REGULAR", debug_flag):
            regular_state.state.run()
        with YarnRaceSimulationEvent.nested_logging("RACE JOB " + self.job.name + " SMARTG", debug_flag):
            elastic_state.state.run()

        # Compare the results.
        regular_result = regular_state.race_result
        elastic_result = elastic_state.race_result

        if self.log.isEnabledFor(logging.INFO):
            self.log.info("YARN_RACE_SIMULATION_EVENT: " + self.job.name + " REGULAR: " + str(regular_result)
                          + ", SMARTG: " + str(elastic_result) + ". Winner: "
                          + ("REGULAR" if regular_result <= elastic_result else "SMARTG"))

        winner = None
        # If the 'never degrade' option is set, check if any jobs take longer with SMARTG.
        # If that is the case, opt to be REGULAR instead.
        if self.state.user_config.race_never_degrade:
            degradation_exists = YarnRaceSimulationEvent.check_degradation(regular_state, elastic_state)

            if degradation_exists:
                self.log.info("YARN_RACE_SIMULATION_EVENT: SMARTG causes at least one "
                              + "job running time to degrade. REGULAR wins.")
                winner = YarnSchedulerType.REGULAR

        if regular_result != elastic_result:
            winner = YarnSchedulerType.REGULAR if regular_result < elastic_result else YarnSchedulerType.SMARTG

        if winner is not None:
            if not self.state.user_config.race_lockstep_regular or not postponed:
                # Adjust scheduler to the new winning strategy.
                self.state.scheduler.set_behavior(self.job, winner)
            else:
                self.state.race_next_resume_event.winner = winner
                self.state.race_next_resume_event.job = self.job

        return EventResult.CONTINUE,


RACE_NODE_GROUP_GRANULARITY_PERC = 10


class YarnRaceNodeGSimulationEvent(YarnRaceSynchronousSimulationEvent):
    def __init__(self, state):
        YarnRaceSimulationEvent.__init__(self, state)

    def handle(self):
        # Synchronous event, postpone scheduling
        self.postpone_scheduling()

        # Get a list of the current nodes and their occupancy.
        node_list = self.state.nodes.values()
        node_count = len(node_list)
        node_list.sort(key=lambda item: [item.available.memory_mb, item.node_id])

        # Generate the new simulators.
        simulator_states = {}
        for perc in xrange(0, node_count + (RACE_NODE_GROUP_GRANULARITY_PERC * node_count / 100),
                           (RACE_NODE_GROUP_GRANULARITY_PERC * node_count / 100)):
            simulator_states[perc] = YarnRaceSimulationEvent.generate_race_runner(self.state, self.log,
                                                                                  self.state.simulator.clock_millis,
                                                                                  YarnSchedulerType.RACE_NODEG)
            for i in xrange(0, perc):
                simulator_states[perc].scheduler.set_behavior(node_list[i], YarnSchedulerType.SMARTG)

        # Run the simulations.
        debug_flag = self.state.user_config.oracle_debug
        for perc in xrange(0, node_count + RACE_NODE_GROUP_GRANULARITY_PERC, RACE_NODE_GROUP_GRANULARITY_PERC):
            with YarnRaceSimulationEvent.nested_logging("RACE NODEG PERC " + str(perc) + "%%", debug_flag):
                simulator_states[perc].state.run()

        # Compare the results.
        winner_perc = -1
        winner_total_time = -1
        for perc in xrange(0, node_count + RACE_NODE_GROUP_GRANULARITY_PERC, RACE_NODE_GROUP_GRANULARITY_PERC):
            if simulator_states[perc].race_result is None:
                continue
            # If the 'never degrade' option is set, check if any jobs take longer with SMARTG.
            # If that is the case, opt to be REGULAR instead.
            if self.state.user_config.race_never_degrade:
                degradation_exists = YarnRaceSimulationEvent.check_degradation(simulator_states[0],
                                                                               simulator_states[perc])

                if degradation_exists:
                    self.log.info("YARN_RACE_SIMULATION_EVENT: NOEDG PERC " + str(
                        perc) + "%% SMARTG causes at least one job running time to degrade. REGULAR wins.")
                    continue

            if winner_total_time == -1 or simulator_states[perc].race_result < winner_total_time:
                winner_perc = perc
                winner_total_time = simulator_states[perc].race_result

        if winner_perc == -1:
            # No jobs started yet. Ignore.
            return EventResult.CONTINUE,

        if self.log.isEnabledFor(logging.INFO):
            self.log.info("YARN_RACE_SIMULATION_EVENT: Winner: " + str(winner_perc) + "%" + ", RT: "
                          + str(winner_total_time) + ", REGULAR_RT: "
                          + str(simulator_states[0].race_result))

        for i in xrange(0, node_count):
            if i < node_count * winner_perc / 100:
                self.state.scheduler.set_behavior(node_list[i], YarnSchedulerType.SMARTG)
            else:
                self.state.scheduler.set_behavior(node_list[i], YarnSchedulerType.REGULAR)

        return EventResult.CONTINUE,

