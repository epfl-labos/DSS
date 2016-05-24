#!/usr/bin/env python

import copy
import logging
from abc import ABCMeta, abstractmethod

from enum import Enum

event_formatter = logging.Formatter('%(asctime)-15s %(levelname)-8s %(name)s CLK: %(clock)-20s %(message)s')
console = logging.StreamHandler()
console.setFormatter(event_formatter)
LOG = logging.getLogger("event")
LOG.addHandler(console)
LOG.propagate = False

EventResult = Enum("EventResult", "CONTINUE FINISHED PAUSE")


class EventLoggerAdapter(logging.LoggerAdapter):
    def __init__(self, logger, event):
        logging.LoggerAdapter.__init__(self, logger, extra={'event': event})

    def process(self, msg, kwargs):
        # Need to do it like this because the clock needs refreshing each time
        self.extra['clock'] = self.extra['event'].state.simulator.clock_millis
        return logging.LoggerAdapter.process(self, msg, kwargs)


class Event(object):
    __metaclass__ = ABCMeta

    def __init__(self, state):
        self.time_millis = 0
        self.state = state
        self.log = EventLoggerAdapter(LOG, self)

    # noinspection PyArgumentList
    def __deepcopy__(self, memo):
        new_event = self.__class__(copy.deepcopy(self.state, memo))
        new_event.time_millis = self.time_millis
        return new_event

    @abstractmethod
    def handle(self):
        raise NotImplementedError()


