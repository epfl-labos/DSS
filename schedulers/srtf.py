import logging

from events.event import EventResult
from models.yarn.objects import YarnContainerType
from schedulers.yarn import YarnScheduler, LOG


class YarnSRTFScheduler(YarnScheduler):
    def __init__(self, state):
        YarnScheduler.__init__(self, state)
        self.job_queue = []

    def handle_job_arrived(self, job):
        YarnScheduler.handle_job_arrived(self, job)
        self.job_queue.append(job)

    @staticmethod
    def compute_job_score(job):
        score = 0
        for task in job.pending_tasks:
            if task.type is YarnContainerType.MRAM:
                continue
            score += task.duration * task.num_containers

        return score

    def schedule(self, node):
        if not self.job_queue:
            return False, (EventResult.CONTINUE,)

        while True:
            self.job_queue.sort(key=lambda x: YarnSRTFScheduler.compute_job_score(x))

            if LOG.isEnabledFor(logging.DEBUG):
                LOG.debug("QUEUE: " + ", ".join(map(lambda x: "<" +
                          x.get_name() + " " + str(x.am_launched) + " " +
                          str(x.consumption) + " " + str(YarnSRTFScheduler.compute_job_score(x)) +
                          ">", self.job_queue)))

            queue_idx = 0
            while queue_idx < len(self.job_queue):
                job = self.job_queue[queue_idx]
                task = job.pending_tasks[0]
                if not job.am_launched and task.type is not YarnContainerType.MRAM:
                    queue_idx += 1
                    continue
                if task.resource <= node.available:
                    # Adjust task, job and node properties to reflect allocation
                    self.handle_container_allocation(node, task.resource, job, task,
                                                     self.state.simulator.clock_millis)
                    if not job.pending_tasks:
                        # All of the job's containers were processed: remove it from the queue
                        self.job_queue.remove(job)
                    break
                else:
                    queue_idx += 1

            if queue_idx == len(self.job_queue):
                break

        return len(node.allocated_containers) > 0, (EventResult.CONTINUE,)

    def has_pending_jobs(self):
        return bool(self.job_queue)
