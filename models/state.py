class State(object):
    def __init__(self, simulator=None, user_config=None):
        self.simulator = simulator
        self.user_config = user_config


class YarnState(State):
    def __init__(self, simulator=None, user_config=None, nodes=None, racks=None, generator=None, scheduler=None,
                 jobs=None, scheduler_type=None, oracle_type=None, simulation_type=None, cluster_changed=False,
                 race_containers_to_simulate=None):
        super(YarnState, self).__init__(simulator, user_config)
        self.nodes = nodes
        self.racks = racks
        self.generator = generator
        self.scheduler = scheduler
        self.jobs = jobs

        self.scheduler_type = scheduler_type
        self.oracle_type = oracle_type
        self.simulation_type = simulation_type

        self.cluster_changed = cluster_changed
        self.race_containers_to_simulate = race_containers_to_simulate
        self.use_gaps = user_config.use_gaps

    @property
    def is_oracle_simulation(self):
        return self.oracle_type is not None


class YarnRaceState(YarnState):
    def __init__(self, simulator=None, user_config=None, nodes=None, racks=None, generator=None, scheduler=None,
                 jobs=None, scheduler_type=None, oracle_type=None, simulation_type=None, cluster_changed=False,
                 race_containers_to_simulate=None):

        super(YarnRaceState, self).__init__(simulator, user_config, nodes, racks, generator, scheduler, jobs,
                                            scheduler_type, oracle_type, simulation_type, cluster_changed,
                                            race_containers_to_simulate)

        self.race_result = None
        self.race_next_resume_event = None


class SymbexState(YarnState):
    def __init__(self, simulator=None, user_config=None, nodes=None, racks=None, generator=None, scheduler=None,
                 jobs=None, scheduler_type=None, oracle_type=None, simulation_type=None, symbex_mode=None,
                 symbex_state_id=None, symbex_parent_id=None, cluster_changed=False, race_containers_to_simulate=None):
        super(SymbexState, self).__init__(simulator, user_config, nodes, racks, generator, scheduler, jobs,
                                          scheduler_type, oracle_type, simulation_type, cluster_changed,
                                          race_containers_to_simulate)

        self.symbex_mode = symbex_mode
        self.symbex_out_folder = None
        self.symbex_state_id = symbex_state_id
        self.symbex_parent_id = symbex_parent_id

        self.container_offers = dict()

    @property
    def is_initial_state(self):
        return self.symbex_parent_id is None
