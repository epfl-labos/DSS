import copy
import logging
from abc import ABCMeta
from contextlib import contextmanager

from events.event import EventResult, LOG, event_formatter, Event


class YarnOracleSimulationEvent(Event):
    __metaclass__ = ABCMeta

    def __init__(self, state):
        super(YarnOracleSimulationEvent, self).__init__(state)

    def __deepcopy__(self, memo):
        new_event = copy.copy(self)
        return new_event

    @staticmethod
    @contextmanager
    def nested_logging(name, debug_flag):
        # Change logging output to include oracle simulation name for the rest of DSS
        current_level = LOG.getEffectiveLevel()
        LOG.setLevel(logging.DEBUG if debug_flag else current_level)
        event_log = logging.getLogger("event")
        for h in event_log.handlers:
            h.setFormatter(logging.Formatter('%(asctime)-15s %(levelname)-8s ORACLE ' + name +
                                             ' %(name)s CLK: %(clock)-20s %(message)s'))
        yarn_schedulers_log = logging.getLogger("yarn_schedulers")
        yarn_schedulers_log.setLevel(logging.DEBUG if debug_flag else current_level)
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter('%(asctime)-15s %(levelname)-8s ORACLE ' + name +
                                                       ' %(name)s %(message)s'))
        yarn_schedulers_log.addHandler(console_handler)
        yarn_schedulers_log.propagate = False

        yield

        # Revert logging back to normal
        for h in event_log.handlers:
            h.setFormatter(event_formatter)
        yarn_schedulers_log.removeHandler(console_handler)
        yarn_schedulers_log.propagate = True
        console_handler.close()
        yarn_schedulers_log.setLevel(current_level)
        LOG.setLevel(current_level)


class YarnResumeSchedulingEvent(Event):
    def __init__(self, state):
        super(YarnResumeSchedulingEvent, self).__init__(state)
        self.winner = None
        self.job = None
        self.node = None

    def __deepcopy__(self, memo):
        new_event = copy.copy(self)
        return new_event

    def handle(self):
        self.log.info("YARN_RESUME_SCHEDULING_EVENT")
        if not self.state.user_config.race_lockstep_regular:
            self.state.scheduler.allow_scheduling = True
        elif self.winner is not None:
            self.log.info(str(self.winner))
            if self.job is None and self.node is None:
                # RACE_LOCKSTEP resume scheduling
                self.state.scheduler.behavior = self.winner
            elif self.node is None:
                # RACE_JOB resume scheduling
                self.state.scheduler.set_behavior(self.job, self.winner)
            elif self.job is None:
                # RACE_NODEG resume scheduling
                self.state.scheduler.set_behavior(self.node, self.winner)
            # Generate NodeHeartbeat events for all the nodes.
            for node in self.state.nodes.values():
                if node.next_heartbeat.handled:
                    node.next_heartbeat.generate_next_heartbeat()

        return EventResult.CONTINUE,
