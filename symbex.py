#!/usr/bin/env python

import logging
from Queue import Queue, Empty
from threading import Thread, Lock

from events.event import EventResult

symbex_format = logging.Formatter('%(asctime)-15s %(levelname)-8s <%(threadName)s> %(name)s %(message)s')
LOG = logging.getLogger("symbex")
console = logging.StreamHandler()
console.setFormatter(symbex_format)
LOG.addHandler(console)
LOG.propagate = False


class SymbexRunner(object):

    def __init__(self, initial_state, num_workers):
        self.initial_state = initial_state
        self.next_id = 1
        self.id_lock = Lock()
        self.initial_state.symbex_state_id = self.get_unique_id()

        self.task_queue = Queue()
        self.task_queue.put(initial_state)
        self.num_workers = num_workers

    def get_unique_id(self):
        self.id_lock.acquire()
        try:
            result = self.next_id
            self.next_id += 1
            return result
        finally:
            self.id_lock.release()

    @staticmethod
    def symbex_worker(runner):
        task_queue = runner.task_queue
        while True:
            try:
                item = task_queue.get(block=True, timeout=1)
                LOG.info("Resuming state " + str(item.state_id))
                item_state = item.state
                item_simulator = item_state.simulator

                while True:
                    # Run the simulator.
                    sim_result = item_simulator.run()

                    if sim_result[0] is not EventResult.PAUSE:
                        # Optional argument will contain a list of forked-off states
                        for forked_state in sim_result[1]:
                            # Assign new ids to these states and store their parent state ids.
                            forked_state.symbex_state_id = runner.get_unique_id()
                            forked_state.symbex_parent_id = item.state_id
                            LOG.info("Forking state " + str(item.state_id) + " into state " +
                                     str(forked_state.state_id))
                            task_queue.put(forked_state)
                    elif sim_result[0] is EventResult.FINISHED:
                        break

                    if not item_state.user_config.symbex_dfs:
                        break

                if sim_result[0] is not EventResult.FINISHED:
                    # Add original state back to queue
                    task_queue.put(item)

                task_queue.task_done()

            except Empty:
                return

    @staticmethod
    def adjust_logging():
        # Change logging output to include thread information for the rest of DSS
        event_log = logging.getLogger("event")
        for h in event_log.handlers:
            h.setFormatter(logging.Formatter('%(asctime)-15s %(levelname)-8s <%(threadName)s> ' +
                                             '%(name)s CLK: %(clock)-20s %(message)s'))
        yarn_schedulers_log = logging.getLogger("yarn_schedulers")
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(symbex_format)
        yarn_schedulers_log.addHandler(console_handler)
        yarn_schedulers_log.propagate = False

    def run(self):
        SymbexRunner.adjust_logging()
        for i in range(self.num_workers):
            t = Thread(target=SymbexRunner.symbex_worker, args=(self,), name="SymbexWorker-" + str(i))
            t.daemon = True
            t.start()

        t = Thread(target=self.task_queue.join)
        t.daemon = True
        t.start()

        while t.isAlive():
            t.join(3600)
