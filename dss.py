#!/usr/bin/env python

import logging
import pdb
import signal
import sys

import argparse

from models.generator.yarn import YarnGenerator
from models.yarn.objects import YarnErrorType, YarnErrorMode
from models.yarn.penalties import YarnPenalty
from schedulers.oracle import PushbackStrategy
from schedulers.yarn import YarnSchedulerType
from simulator import Simulator
from symbex import SymbexRunner
from events.yarn.symbex import SymbexMode
from utils import parsenumlist, get_enumparser

logging.basicConfig(level=logging.WARNING, format='%(asctime)-15s %(levelname)-8s %(name)s : %(message)s')


# noinspection PyUnusedLocal
def signal_handler(signum, frame):
    pdb.set_trace()


def verify_user_config(args):
    if (args.penalty is not None and args.ib is None) or \
            (args.ib is not None and args.penalty is None):
        sys.stderr.write("Must specify both PENALTY and IB value.\n")
        sys.exit(-1)
    if args.duration_error is not None:
        if args.duration_error_mode is YarnErrorMode.CONSTANT and \
                args.duration_error_type is YarnErrorType.MIXED:
            sys.stderr.write("CONSTANT duration errors cannot be of the type MIXED.")
            sys.exit(-1)
        if (args.duration_error_type is YarnErrorType.NEGATIVE or args.duration_error_type is YarnErrorType.MIXED) and \
                args.duration_error >= 100:
            sys.stderr.write("Current settings would create containers with negative duration.")
            sys.exit(-1)
    if args.mem_error is not None:
        if args.mem_error_mode is YarnErrorMode.CONSTANT and \
                args.mem_error_type is YarnErrorType.MIXED:
            sys.stderr.write("CONSTANT memory errors cannot be of the type MIXED.")
            sys.exit(-1)
        if (args.mem_error_type is YarnErrorType.NEGATIVE or args.mem_error_type is YarnErrorType.MIXED) and \
                args.mem_error >= 100:
            sys.stderr.write("Current memory error settings would create containers with negative ideal memory.")
            sys.exit(-1)
        if (args.mem_error_type is YarnErrorType.POSITIVE or args.mem_error_type is YarnErrorType.MIXED) and \
                (args.penalty is None or args.ib is None):
            sys.stderr.write("Injecting positive memory errors with no penalty or IB defined will lead to no visible"
                             " changes. Please add a penalty type.")
            sys.exit(-1)
    if args.ib_error is not None:
        if args.ib_error_mode is YarnErrorMode.CONSTANT and \
                args.ib_error_type is YarnErrorType.MIXED:
            sys.stderr.write("CONSTANT IB errors cannot be of the type MIXED.")
            sys.exit(-1)
        if (args.ib_error_type is YarnErrorType.NEGATIVE or args.ib_error_type is YarnErrorType.MIXED) and \
                args.ib_error >= 100:
            sys.stderr.write("Current IB error settings would create containers with negative ideal memory.")
            sys.exit(-1)
    if args.use_gaps and not args.use_reservations:
        sys.stderr.write("WARNING: -g/--use_gaps only makes sense if reservations are used.")


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("sls_trace_file", help="Path to SLS trace file (JSON format).")
    parser.add_argument("network_topology_file", help="Path to topo file (JSON format).")
    parser.add_argument("node_mem_mb", type=int, help="Amount of memory in MB for each node.")
    parser.add_argument("node_cores", type=int, help="Amount of virtual cores for each node.")
    parser.add_argument("node_hb_ms", type=int, help="Period of the NM heartbeats in milliseconds.")
    parser.add_argument("am_hb_ms", type=int, help="Period of the AM heartbeats in milliseconds.")
    parser.add_argument("am_container_mb", type=int, help="Amount of memory in MB for each AM container.")
    parser.add_argument("am_container_cores", type=int, help="Number of cores taken by each AM container.")
    parser.add_argument("scheduler_type", type=get_enumparser(YarnSchedulerType, YarnSchedulerType.SYMBEX),
                        choices=filter(lambda x: x is not YarnSchedulerType.SYMBEX, YarnSchedulerType),
                        help="Type of scheduler to run", default=YarnSchedulerType.REGULAR.name, nargs="?")

    elastic_args = parser.add_argument_group("ELASTIC arguments")
    elastic_args.add_argument("penalty", type=get_enumparser(YarnPenalty), choices=YarnPenalty,
                              help="The performance penalty model to use", nargs="?")
    elastic_args.add_argument("ib", type=float, help="The initial bump of the penalty model", nargs="?")
    elastic_args.add_argument("--ep", type=int,
                              help="Percentage of nodes which are always elastic (works only for GREEDY & SMARTG)")

    peek_args = parser.add_argument_group("PEEK arguments")
    peek_args.add_argument("-p", "--peek-pushback-strategy", type=get_enumparser(PushbackStrategy),
                           choices=PushbackStrategy, nargs="?", help="Strategy to use to avoid pushback in PEEK.",
                           default=None)

    race_args = parser.add_argument_group("RACE arguments")
    from events.yarn.elastic.race import YarnRaceMetric
    race_args.add_argument("--race-metric", type=get_enumparser(YarnRaceMetric), choices=YarnRaceMetric,
                           help="Metric by which to compare REGULAR vs. SMARTG in RACE.",
                           default=YarnRaceMetric.TOTAL_JRT.name)
    race_args.add_argument("--race-lockstep-regular", action="store_true",
                           help="Switch flag to REGULAR until end of simulation.", default=False)
    race_args.add_argument("--race-duration-range", type=parsenumlist,
                           help="Duration range (min, max) in ms of the " +
                                "RACE simulations w.r.t. the simulation timeline.",
                           default=(0, 0))
    race_args.add_argument("--race-never-degrade", action="store_true",
                           help="Always favour REGULAR if any job gets worse running times by using SMARTG.",
                           default=False)
    race_args.add_argument("--race-cutoff-perc", type=int,
                           help="Only simulate a percentage of the remaining containers.",
                           default=0)

    symbex_args = parser.add_argument_group("SYMBEX arguments")
    symbex_args.add_argument("--symbex", action="store_true",
                             help="Run a symbex-type exploration on SMARTG vs REGULAR scheduling decisions.",
                             default=False)
    symbex_args.add_argument("--symbex-mode", type=get_enumparser(SymbexMode), choices=SymbexMode,
                             help="Symbex exploration to run.", default=SymbexMode.DECISION.name)
    symbex_args.add_argument("--symbex-dfs", action="store_true",
                             help="Do a DFS-type exploration rather than a BFS one.",
                             default=False)
    symbex_args.add_argument("--symbex-workers", type=int, help="Number of concurrent symbex worker threads to run.",
                             default=1)

    errinj_args = parser.add_argument_group("Error injection arguments")
    errinj_args.add_argument("--duration-error", type=int,
                             help="Percentage by which to mis-estimate the running times of the containers.")
    errinj_args.add_argument("--duration-error-type", type=get_enumparser(YarnErrorType), choices=YarnErrorType,
                             help="Whether duration error is positive, negative or either.",
                             default=YarnErrorType.MIXED.name)
    errinj_args.add_argument("--duration-error-mode", type=get_enumparser(YarnErrorMode), choices=YarnErrorMode,
                             help="Whether duration error is constant or random.",
                             default=YarnErrorMode.CONSTANT.name)
    el_errinj_args = parser.add_argument_group("ELASTIC Error injection arguments")
    el_errinj_args.add_argument("--duration-error-only-elastic", action="store_true",
                                help="Inject duration error only for ELASTIC containers", default=False)
    el_errinj_args.add_argument("--mem-error", type=int, help="Percentage by which to mis-estimate the ideal memory " +
                                                              "of the containers.")
    el_errinj_args.add_argument("--mem-error-type", type=get_enumparser(YarnErrorType), choices=YarnErrorType,
                                help="Whether the memory misestimation error is positive, negative or either.",
                                default=YarnErrorType.MIXED.name)
    el_errinj_args.add_argument("--mem-error-mode", type=get_enumparser(YarnErrorMode), choices=YarnErrorMode,
                                help="Whether the memory misestimation error is constant or random.",
                                default=YarnErrorMode.CONSTANT.name)
    el_errinj_args.add_argument("--ib-error", type=int, help="Percentage by which to mis-estimate the IB of tasks.")
    el_errinj_args.add_argument("--ib-error-type", type=get_enumparser(YarnErrorType), choices=YarnErrorType,
                                help="Whether IB error is positive, negative or either.",
                                default=YarnErrorType.MIXED.name)
    el_errinj_args.add_argument("--ib-error-mode", type=get_enumparser(YarnErrorMode), choices=YarnErrorMode,
                                help="Whether IB error is constant or random.", default=YarnErrorMode.CONSTANT.name)

    yarn_args = parser.add_argument_group("YARN arguments")
    yarn_args.add_argument("-r", "--use-reservations", action="store_true",
                           help="Whether an app can place a reservation on a node to guarantee its priority there.",
                           default=False)
    yarn_args.add_argument("-g", "--use-gaps", action="store_true",
                           help="Used in conjunction with reservations, allows a container to be scheduled on a " +
                                "reserved node if the duration is less than the reservation duration.", default=False)
    yarn_args.add_argument("--gaps-allow-ams", action="store_true",
                           help="Used in conjunction with reservations and gaps, allows AM containers to be scheduled" +
                                " on a reserved node.", default=False)
    yarn_args.add_argument("--assign-multiple", action="store_true",
                           help="Run the simulation as if the assignMultiple flag is set (multiple assignments per " +
                                "heartbeat are allowed).", default=False)

    parser.add_argument("--oracle-debug", action="store_true",
                        help="Display DEBUG messages for all oracle simulations (generates A LOT of output).")
    parser.add_argument("--mem-overestimate-range", type=parsenumlist, help="Range (min, max) of multipliers for " +
                                                                            "memory overestimates by users.",
                        default=(1, 1))
    parser.add_argument("--mem-overestimate-assume", type=float, help="Multiplier by which the system assumes " +
                                                                      "that users overestimate memory.", default=1)
    parser.add_argument("--meganode", action="store_true", help="Pool all node resources together and " +
                                                                "run simulation on this one node.", default=False)
    parser.add_argument("--occupancy-stats-interval", type=int,
                        help="Gather occupancy stats each interval (in seconds).", default=0)
    parser.add_argument("--occupancy-stats-file", type=str, help="File in which to output occupancy stats.",
                        default=None)
    parser.add_argument("-v", "--verbose", action="count")

    args = parser.parse_args()

    verify_user_config(args)

    if args.verbose == 1:
        logging.getLogger().setLevel(logging.INFO)
    elif args.verbose == 2:
        logging.getLogger().setLevel(logging.DEBUG)

    # Start a new simulator
    simulator = Simulator()

    # Create and run the new YarnGenerator
    yarn_generator = YarnGenerator()
    state = yarn_generator.generate_state(simulator, args)

    # Add the event to the simulator
    simulator.add_event(yarn_generator.get_simulation_start_event())

    # Catch SIGINT and drop to a console for debugging purposes.
    signal.signal(signal.SIGINT, signal_handler)

    # Run the simulation
    if not args.symbex:
        simulator.run()
    else:
        SymbexRunner(state, args.symbex_workers).run()


if __name__ == '__main__':
    main()
    logging.shutdown()
