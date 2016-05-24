from abc import ABCMeta

from events.event import Event


class SimulationStartEvent(Event):
    __metaclass__ = ABCMeta

    def __init__(self, state):
        Event.__init__(self, state)


class SimulationFinishEvent(Event):
    __metaclass__ = ABCMeta

    def __init__(self, state):
        Event.__init__(self, state)
