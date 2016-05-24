#!/usr/bin/env python

import logging

import time

from events.event import EventResult
from utils import PQueue

LOG = logging.getLogger("simulator")


class Simulator(object):
    def __init__(self):
        self.queue = PQueue()
        self.clock_millis = 0
        self.start_real_clock_seconds = 0
        self.finish_real_clock_seconds = 0

    def __deepcopy__(self, memo):
        new_simulator = Simulator()
        memo[id(self)] = new_simulator
        new_simulator.queue = self.queue.__deepcopy__(memo)
        new_simulator.clock_millis = self.clock_millis
        return new_simulator

    @property
    def duration_seconds(self):
        if self.start_real_clock_seconds == 0:
            LOG.error("Simulation has not started yet, cannot compute duration!")
            return 0
        if self.finish_real_clock_seconds == 0:
            LOG.error("Simulation has not finished yet, cannot compute duration!")
            return 0

        return self.finish_real_clock_seconds - self.start_real_clock_seconds

    def add_event(self, event):
        self.queue.push(event, event.time_millis)

    def run(self):
        self.start_real_clock_seconds = time.clock()
        try:
            while not self.queue.empty():
                queue_el = self.queue.pop()
                new_clock = queue_el[0]
                event = queue_el[1]

                if new_clock > self.clock_millis:
                    # Update the internal clock
                    self.clock_millis = new_clock
                elif new_clock < self.clock_millis and new_clock != 0:
                    LOG.warn("Parsing event in the past: " + str(new_clock))

                if event is None:
                    continue

                # Run event callback
                # NOTE: All event handlers should return a tuple (EVENT_RESULT, optional_info) with the first element
                # being the handling result and whether the simulation should continue, and additional information.
                #
                # This is used, for example, to pass information from the simulation to the simulation runner.
                event_return = event.handle()
                if event_return[0] is EventResult.FINISHED or event_return[0] is EventResult.PAUSE:
                    return event_return

            LOG.warn("Reached queue end without receiving a FINISHED event.")
            return EventResult.FINISHED,

        finally:
            self.finish_real_clock_seconds = time.clock()
