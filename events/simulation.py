from abc import ABCMeta

import time

from events.event import Event, EventResult


class SimulationStartEvent(Event):
    __metaclass__ = ABCMeta

    def __init__(self, state):
        Event.__init__(self, state)


class SimulationFinishEvent(Event):
    __metaclass__ = ABCMeta

    def __init__(self, state):
        Event.__init__(self, state)

    def handle(self):
        self.state.simulator.finish_real_clock_seconds = time.clock()
        return EventResult.FINISHED,
