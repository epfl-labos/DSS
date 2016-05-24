import copy
import datetime
import os

from models.yarn.objects import YarnSimulation
from events.event import EventResult
from events.yarn.yarn import YarnSimulationStartEvent, YarnSimulationFinishEvent, YarnJobArriveEvent
from utils import PEnum

SymbexMode = PEnum("SymbexMode", "DECISION RACE")


class YarnSymbexJobArriveEvent(YarnJobArriveEvent):
    def __init__(self, state, yarn_job):
        YarnJobArriveEvent.__init__(self, state, yarn_job)

    def handle(self):
        YarnJobArriveEvent.handle(self)

        # Fork off a state that's always regular
        # Duplicate all internal state
        new_state = copy.deepcopy(self.state)
        new_state.scheduler.disable_greedy = True

        # Make this current state be enabled.
        self.state.scheduler.disable_greedy = False

        return EventResult.PAUSE, [new_state]


class YarnSymbexSimulationStartEvent(YarnSimulationStartEvent):
    def __init__(self, state):
        super(YarnSymbexSimulationStartEvent, self).__init__(state)

    def activate_jobs(self):
        if self.state.symbex_mode is SymbexMode.RACE:
            # Create and add all the YarnJobArrive events to the simulation queue
            for job in self.state.jobs:
                self.state.simulator.add_event(YarnSymbexJobArriveEvent(self.state, job))
        else:
            super(YarnSymbexSimulationStartEvent, self).activate_jobs()

    def handle(self):
        super(YarnSymbexSimulationStartEvent, self).handle()

        # Mark this as a SYMBEX simulation
        self.state.simulation_type = YarnSimulation.SYMBEX

        # Create output folder
        now = datetime.datetime.now()
        now_str = now.strftime("%Y-%m-%d_%H-%M-%S")
        ctr = 1
        while True:
            folder_str = now_str + "_" + str(self.state.symbex_mode) + "_" + (
                "DFS" if self.state.user_config.symbex_dfs else "BFS") + "_" + str(ctr).zfill(4)
            if not os.path.exists(folder_str):
                os.mkdir(folder_str)
                break

            ctr += 1

        self.state.symbex_out_folder = folder_str

        return EventResult.CONTINUE,


class YarnSymbexSimulationFinishEvent(YarnSimulationFinishEvent):
    def __init__(self, state):
        super(YarnSymbexSimulationFinishEvent, self).__init__(state)

    def handle(self):
        super(YarnSymbexSimulationFinishEvent, self).handle()
        # Write out all statistics of this state
        out_folder_str = self.state.symbex_out_folder
        out_file_str = "state_" + str(self.state.state_id).zfill(8)
        with open(os.path.join(out_folder_str, out_file_str), "w") as out:
            # Write basic symbex state info
            out.write("State id: " + str(self.state.state_id)
                      + ". Parent state id: " + str(self.state.parent_id) + "\n")
            # Write job and task info
            total_job_running_time = 0
            total_job_num = 0
            for job in sorted(self.state.scheduler.completed_jobs, key=lambda x: x.job_id):
                total_job_running_time += job.end_ms - job.start_ms
                total_job_num += 1
                out.write(job.name + ", start=" + str(job.start_ms) + ", end=" + str(job.end_ms) + ", duration=" + str(
                    (job.end_ms - job.start_ms) / 1000.0) + "\n")
                for container in sorted(job.finished_tasks, key=lambda x: x.id):
                    out.write("\tcontainer_" + str(container.id) + ", optimal RT = " + str(
                        container.task.duration) + ", actual RT = " + str(
                        container.duration) + ", optimal MEM = " + str(
                        container.task.resource.memory_mb) + " MB, actual MEM = " + str(
                        container.resource.memory_mb) + " MB, node = " + container.node.name + ", SCH ms = " + str(
                        container.scheduled_time_millis) + ", LAU ms = " + str(
                        container.launched_time_millis) + ", FIN ms = " + str(container.finished_time_millis) + "\n")

            # Write total job running time
            out.write("Total cumulative job running time: " + str(total_job_running_time) + " (avg: " + str(
                total_job_running_time * 1.0 / total_job_num) + ")" + "\n")

        return EventResult.FINISHED,
