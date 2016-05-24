import copy
import logging

from events.event import EventResult
from events.simulation import SimulationFinishEvent
from events.yarn.elastic.elastic import YarnOracleSimulationEvent
from events.yarn.yarn import YarnSimulationStartEvent, YarnJobArriveEvent, YarnStandaloneSimulationFinishEvent
from models.oracle.peek import PeekOracle


class YarnPeekSimulationStartEvent(YarnSimulationStartEvent):
    def __init__(self, state):
        super(YarnPeekSimulationStartEvent, self).__init__(state)

    def handle(self):
        result = super(YarnPeekSimulationStartEvent, self).handle()
        next_peek_simulation = YarnPeekSimulationEvent(self.state)
        next_peek_simulation.time_millis = self.state.simulator.clock_millis
        self.state.simulator.add_event(next_peek_simulation)
        self.state.cluster_changed = True

        return result


class YarnPeekJobArriveEvent(YarnJobArriveEvent):
    def __init__(self, state, job):
        super(YarnPeekJobArriveEvent, self).__init__(state, job)

    def handle(self):
        result = super(YarnPeekJobArriveEvent, self).handle()
        self.state.simulator.add_event(YarnPeekSimulationEvent(self.state, one_time=True))
        return result


class YarnPeekStandaloneSimulationFinishEvent(YarnStandaloneSimulationFinishEvent):
    def __init__(self, state):
        super(YarnPeekStandaloneSimulationFinishEvent, self).__init__(state)

    def cancel_recurring_events(self):
        super(YarnPeekStandaloneSimulationFinishEvent, self).cancel_recurring_events()

        if not hasattr(self.state.scheduler, "next_simulation_event"):
            logging.error("Invalid scheduler used for PEEK.")
            return

        next_simulation_event = self.state.scheduler.next_simulation_event
        if next_simulation_event is not None:
            self.state.simulator.queue.unsafe_remove(next_simulation_event)


class YarnPeekSimulationEvent(YarnOracleSimulationEvent):
    jobs_to_finish = set()

    def __init__(self, state, one_time=False):
        YarnOracleSimulationEvent.__init__(self, state)
        self.one_time = one_time

    def __deepcopy__(self, memo):
        new_event = copy.copy(self)
        memo[id(self)] = new_event
        return new_event

    def should_run_simulation(self, cutoff_time):
        # No need to run PEEK if there are no jobs in the queue.
        if not self.state.scheduler.job_queue:
            return False

        if self.state.cluster_changed:
            self.state.cluster_changed = False
            return True

        new_jobs_finishing = False
        # No containers launched or finished since last time, check that the job is not due to end soon.
        for job in self.state.jobs:
            # Skip jobs that have not begun yet, have already finished or still have containers to schedule.
            if job.trace_start_ms > self.state.simulator.clock_millis or job.finished or job.pending_tasks:
                continue

            job_finishes = True
            job_last_container_time = 0
            for container in job.running_tasks:
                if container.launched_time_millis != -1:
                    container_finish_time = container.launched_time_millis + container.duration
                else:
                    container_finish_time = container.scheduled_time_millis + container.duration \
                                            + container.node.hb_interval_ms

                if container_finish_time > job_last_container_time:
                    job_last_container_time = container_finish_time

                if container_finish_time > cutoff_time:
                    job_finishes = False
                    break

            if job_finishes and job.job_id not in YarnPeekSimulationEvent.jobs_to_finish:
                YarnPeekSimulationEvent.jobs_to_finish.add(job.job_id)
                self.log.info("YARN_PEEK_SIMULATION: " + job.name + " triggers PEEK with " +
                              "last finish time " + str(job_last_container_time))
                new_jobs_finishing = True

        return new_jobs_finishing

    def compute_cutoff_time(self):
        return PeekOracle.compute_cutoff_time(self.state.simulator.clock_millis,
                                              self.state.user_config.node_hb_ms,
                                              self.state.scheduler.running_jobs)

    def handle(self):
        # Generate the next simulation point.
        if self.one_time:
            # If we overlap with a perodic event push it back.
            next_simulation_event = self.state.scheduler.next_simulation_event
            if next_simulation_event is not None and \
                    next_simulation_event.time_millis == self.state.simulator.clock_millis:
                self.state.simulator.queue.safe_remove(next_simulation_event)
                next_simulation_event.time_millis += self.state.user_config.node_hb_ms
                self.state.simulator.add_event(next_simulation_event)

            # Clear all retained jobs to finish, since new jobs influence job completion times.
            YarnPeekSimulationEvent.jobs_to_finish.clear()
            self.state.cluster_changed = True
        elif not self.state.scheduler.all_jobs_are_done():
            next_simulation = YarnPeekSimulationEvent(self.state)
            next_simulation.time_millis = self.state.simulator.clock_millis + self.state.user_config.node_hb_ms
            self.state.scheduler.next_simulation_event = next_simulation
            self.state.simulator.add_event(next_simulation)

        # Check if the simulation should be run
        cutoff_time = self.compute_cutoff_time()

        if self.one_time or self.should_run_simulation(cutoff_time):
            # Run the PEEK simulation
            self.run_peek_simulation(cutoff_time)

        return EventResult.CONTINUE,

    def run_peek_simulation(self, cutoff_time):
        if self.log.isEnabledFor(logging.INFO):
            self.log.info("YARN_PEEK_SIMULATION_EVENT.")

        # Run the simulation
        simulator_state = PeekOracle.generate_simulation(self.state, self.log,
                                                         self.state.simulator.clock_millis, cutoff_time)
        PeekOracle.run_simulation(self.state, simulator_state)

        peek_scheduler = simulator_state.scheduler
        # Save the outcome.
        self.state.scheduler.most_recent_peek = simulator_state
        # Reset existing times.
        self.state.scheduler.reset_times()

        # Check which jobs have finished scheduling tasks.
        for job in (peek_scheduler.running_jobs | peek_scheduler.completed_jobs):
            if not job.pending_tasks:
                max_scheduling_time = \
                    max(task.scheduled_time_millis for task in (job.running_tasks | job.finished_tasks))
                self.state.scheduler.set_job_finish_scheduling_time(job, max_scheduling_time)

        # Check which jobs have completed.
        for job in peek_scheduler.completed_jobs:
            max_end_time = max(task.finished_time_millis for task in job.finished_tasks)
            self.state.scheduler.set_elastic_time_limit(job, max_end_time)
            self.state.scheduler.set_job_finish_time(job, job.end_ms)


class YarnPeekSimulationFinishEvent(SimulationFinishEvent):
    def __init__(self, state):
        SimulationFinishEvent.__init__(self, state)

    def handle(self):
        super(YarnPeekSimulationFinishEvent, self).handle()
        if self.log.isEnabledFor(logging.INFO):
            self.log.info("YARN_PEEK_SIMULATION_FINISHED")
        while not self.state.simulator.queue.empty():
            _ = self.state.simulator.queue.pop()

        return EventResult.FINISHED,
