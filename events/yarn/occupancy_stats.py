from models.yarn.objects import YarnSimulation
from events.event import EventResult, Event


class YarnOccupancyStatsEvent(Event):
    def __init__(self, state, trace_started):
        super(YarnOccupancyStatsEvent, self).__init__(state)
        self.trace_started = trace_started

    def __deepcopy__(self, memo):
        new_event = super(YarnOccupancyStatsEvent, self).__deepcopy__(memo)
        new_event.trace_started = False
        return new_event

    def handle(self):
        if self.state.simulation_type is not YarnSimulation.STANDALONE:
            # Ignore event for other types of simulations
            return EventResult.CONTINUE,

        total_resource = \
            reduce(lambda x, y: x + y, (node.capacity - node.available for node in self.state.nodes.values()))
        total_containers = sum(len(node.running_containers) for node in self.state.nodes.values())
        elastic_containers = len(list(task for node in self.state.nodes.values()
                                 for task in node.running_containers if task.resource != task.task.ideal_resource))
        if total_resource.memory_mb != 0 and total_resource.vcores != 0:
            if self.state.user_config.occupancy_stats_file is not None:
                with open(self.state.user_config.occupancy_stats_file, "a") as out:
                    if not self.trace_started:
                        out.write(
                            "# clock (ms)\t total_memory (MB)\ttotal_cores\ttotal_containers\telastic_containers\n"
                        )
                        self.trace_started = True
                    out.write("{}\t{}\t{}\t{}\t{}\n".format(
                        self.state.simulator.clock_millis,
                        total_resource.memory_mb,
                        total_resource.vcores,
                        total_containers,
                        elastic_containers

                    ))

        # Create next occupancy stats gathering event.
        next_event = YarnOccupancyStatsEvent(self.state, self.trace_started)
        next_event.time_millis = self.state.simulator.clock_millis + \
            int(self.state.user_config.occupancy_stats_interval) * 1000
        # Create a new event if the collection interval has been set, and if there is still something running
        if next_event.time_millis > self.state.simulator.clock_millis and \
                (not self.trace_started or not self.state.scheduler.all_jobs_are_done()):
            self.state.simulator.add_event(next_event)

        return EventResult.CONTINUE,
