from operator import attrgetter

import numpy

from models.yarn.objects import YarnRunningContainer, YarnContainerType, YarnJob


class StatsGenerator(object):
    def __init__(self, state):
        self.state = state
        self.sorted_jobs = sorted(self.state.scheduler.completed_jobs, key=lambda x: x.job_id)

    def get_job_stats_str(self):
        result = []
        for job in self.sorted_jobs:
            result.append("{}, start_ms={}, end_ms={}, duration_s={:.2f}, tasks = ({})".format(
                job.name, job.start_ms, job.end_ms, job.duration_ms / 1000.0,
                ", ".join(["{}MB x {}".format(task.resource.memory_mb, task.num_containers)
                           for task in job.tasks if task.type != YarnContainerType.MRAM])
            ))
        return "\n".join(result)

    def get_task_stats_str(self):
        result = []
        for job in self.sorted_jobs:
            for task in sorted(job.finished_tasks, key=lambda x: x.id):
                assert type(task) is YarnRunningContainer
                result.append(
                    "{}, type={}, start_ms={}, end_ms={}, duration_s={:.2f}, given_memory_mb={}, alloc_ratio={:.2f}, "
                    "node={}"
                    .format(
                        task.name,
                        task.type,
                        task.launched_time_millis,
                        task.finished_time_millis,
                        task.duration_ms / 1000.0,
                        task.resource.memory_mb,
                        1 if task.task.ideal_resource.memory_mb == 0 else
                        task.resource.memory_mb * 1.0 / task.task.ideal_resource.memory_mb,
                        task.node.name
                    ))

        return "\n".join(result)

    def get_misc_stats_str(self):
        result = [": ".join(["ALL JOBS", self.state.scheduler.get_total_stats_str()]),
                  "TOTAL RESERVED GB * s = {:.2f}".format(self.state.scheduler.reserved_memory_time / 1000000),
                  "TOTAL RESERVED USABLE GB * s = {}".format(
                      self.state.scheduler.reserved_usable_memory_time / 1000000
                  ),
                  "PERCENT RESERVED/(UTILIZED + RESERVED) GB * s = {:.2f}".format(
                      self.state.scheduler.reserved_memory_time * 100.0
                      / (self.state.scheduler.total_mem_utilization_ms + self.state.scheduler.reserved_memory_time)
                  ),
                  "PERCENT RESERVED USABLE/(UTILIZED + RESERVED USABLE) GB * s = {:.2f}".format(
                    self.state.scheduler.reserved_usable_memory_time * 100.0
                    / (self.state.scheduler.total_mem_utilization_ms + self.state.scheduler.reserved_usable_memory_time)
                  ),
                  "MAKESPAN (s) = {}".format(self.state.scheduler.get_makespan_ms() / 1000),
                  "TOTAL UTILIZED GB * s = {}".format(self.state.scheduler.total_mem_utilization_ms / 1000000),
                  "",
                  "# run_duration(s)\tavg_job_duration(s)\tfirst_job_arrival(ms) / makespan(ms)\taverage cluster "
                  "utilization(GB) ",
                  "{}\t{:.2f}\t{:.2f}\t{:.2f}".format(
                      self.state.simulator.duration_seconds,
                      numpy.mean(list(job.duration_ms for job in self.state.jobs)) / 1000,
                      next(
                          iter(sorted(self.state.jobs, key=attrgetter("trace_start_ms"), reverse=True) or []), YarnJob()
                      ).trace_start_ms * 1.0 / self.state.scheduler.get_makespan_ms(),
                      self.state.scheduler.reserved_memory_time * 1.0 / (
                          next(
                              iter(sorted(self.state.jobs, key=attrgetter("end_ms"), reverse=True) or []), YarnJob()
                          ).end_ms -
                          next(
                              iter(sorted(self.state.jobs, key=attrgetter("start_ms")) or []), YarnJob()
                          ).start_ms
                      )
                  )]

        return "\n".join(result)

    def get_job_decision_stats_str(self):
        result = []
        for job in self.sorted_jobs:
            result.append("{}: {}".format(job.name, self.state.scheduler.get_job_stats_str(job.job_id)))
        return "\n".join(result)

    def get_reservation_stats_str(self):
        result = []
        for node in self.state.nodes.values():
            result.append(
                "{}:\n{}".format(
                    node.name,
                    "\n".join(
                        list("job={}, start_ms={}, end_ms={}".format(
                            next(job.name for job in self.state.jobs if job.job_id == reservation[0]),
                            reservation[1], reservation[2]
                        ) for reservation in self.state.scheduler.cluster_reservations[node.node_id])
                        if node.node_id in self.state.scheduler.cluster_reservations else "")
                )
            )
        return "\n\n".join(result)


class StatsWriter(object):
    def __init__(self, state, stats_generator):
        self.stats_generator = stats_generator
        self.state = state

    def write_stats(self):
        with open("decision_stats.out", "w") as f:
            f.writelines([self.stats_generator.get_job_decision_stats_str(), "\n",
                          self.state.scheduler.get_total_stats_str(), "\n"])

        with open("job_stats.out", "w") as f:
            f.write(self.stats_generator.get_job_stats_str() + "\n")

        with open("task_stats.out", "w") as f:
            f.write(self.stats_generator.get_task_stats_str() + "\n")

        with open("misc_stats.out", "w") as f:
            f.write(self.stats_generator.get_misc_stats_str() + "\n")

        with open("reservation_stats.out", "w") as f:
            f.write(self.stats_generator.get_reservation_stats_str() + "\n")
