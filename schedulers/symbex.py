import copy

from events.event import EventResult
from schedulers.elastic import YarnSmartgScheduler
from events.symbex import SymbexDecisionResumeStateEvent


class YarnSymbexScheduler(YarnSmartgScheduler):

    def __init__(self, state):
        YarnSmartgScheduler.__init__(self, state, 0, None)
        self.new_states = []

    def handle_container_allocation(self, node, allocated, job, task, time_millis):
        # Check if this is a SMARTG allocation. If so, fork off a state and be fully REGULAR on it.
        if allocated.memory_mb < task.resource.memory_mb:
            # Make sure to mark that we've tried to give this container this exact amount of memory
            if job.yarn_id not in self.state.container_offers:
                self.state.container_offers[job.yarn_id] = {}
            job_container_offers = self.state.container_offers[job.yarn_id]
            if job.next_container_id not in job_container_offers:
                job_container_offers[job.next_container_id] = set()
            current_container_offers = job_container_offers[job.next_container_id]

            if allocated.memory_mb not in current_container_offers:
                # Add this offer to the existing ones
                current_container_offers.add(allocated.memory_mb)
                # Duplicate all internal state
                new_state = copy.deepcopy(self.state)
                # Add a new SymbexDecisionResumeStateEvent at the head of the simulator.
                # Skip the current job, since this state will be REGULAR on this decision.
                symbex_resume_state_event = SymbexDecisionResumeStateEvent(new_state, node.name,
                                                                           self.job_queue.index(job) + 1)
                new_state.add_event(symbex_resume_state_event)
                # Add this new state to the list of states forked.
                self.new_states.append(new_state)

        # Finish this run
        YarnSmartgScheduler.handle_container_allocation(self, node, allocated, job, task, time_millis)

    def schedule(self, node, queue_start=0):
        self.new_states = []
        scheduling_result = YarnSmartgScheduler.schedule(self, node, queue_start=queue_start)
        if not self.new_states:
            return scheduling_result
        else:
            return bool(node.allocated_containers), (EventResult.PAUSE, self.new_states)
