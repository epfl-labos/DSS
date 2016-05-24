from events.yarn.elastic.elastic import YarnOracleSimulationEvent
from models.generator.oracle import OracleGenerator
from models.yarn.objects import YarnContainerType, YarnSimulation
from schedulers.yarn import YarnSchedulerType


class PeekOracle(object):

    @staticmethod
    def compute_cutoff_time(clock_millis, delta_millis, jobs):
        # Compute the maximum duration of the PEEK.
        cutoff_time = -1
        for job in jobs:
            # XXX: Make this work for jobs with more than 1 type of containers
            penalized_duration = 0
            for task in job.pending_tasks:
                if task.type is not YarnContainerType.MRAM:
                    penalized_duration = int(task.duration * (task.penalty.initial_bump + 1)) + delta_millis
                    break
            if clock_millis + penalized_duration > cutoff_time:
                cutoff_time = clock_millis + penalized_duration

        return cutoff_time

    @staticmethod
    def generate_simulation(state, log, clock_millis, cutoff_time):
        # Generate the new simulator.
        # NOTE: We use a SMARTG scheduler, but we disable elasticity. This is needed to allow injection of ELASTIC
        # containers when checking pushback in PEEK.
        oracle_state = OracleGenerator.generate_oracle_runner(state, log, clock_millis, YarnSchedulerType.SMARTG,
                                                              YarnSimulation.PEEK)
        oracle_scheduler = oracle_state.scheduler
        oracle_scheduler.disable_elastic = True

        map(lambda job: state.scheduler.set_behavior(job, YarnSchedulerType.SMARTG), state.scheduler.job_queue)

        # Cut-off the PEEK after the maximum duration has been reached.
        from events.yarn.elastic.peek import YarnPeekSimulationFinishEvent
        finish_event = YarnPeekSimulationFinishEvent(oracle_state)
        finish_event.time_millis = cutoff_time
        oracle_state.simulator.add_event(finish_event)

        return oracle_state

    @staticmethod
    def run_simulation(state, simulator_state):
        # Run the simulation.
        debug_flag = state.user_config.oracle_debug
        with YarnOracleSimulationEvent.nested_logging("PEEK", debug_flag):
            simulator_state.simulator.run()
