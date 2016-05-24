from events.event import Event


class SymbexDecisionResumeStateEvent(Event):
    def __init__(self, state, node_name, queue_start):
        super(SymbexDecisionResumeStateEvent, self).__init__(state)
        self.node_name = node_name
        self.queue_start = queue_start

    def handle(self):
        # Finish running the scheduling algorithm.
        node = self.state.nodes[self.node_name]
        scheduling_result = self.state.scheduler.schedule(node, queue_start=self.queue_start)

        # If allocation was possible, generate the next heartbeat (give other jobs
        # the chance to schedule).
        if scheduling_result[0]:
            node.next_heartbeat.generate_next_heartbeat()

        return scheduling_result[1]
