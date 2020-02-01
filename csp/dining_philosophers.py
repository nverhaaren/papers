from papers.csp.io_semantics import Signal, InputGuard, CommandFailure
from papers.csp.process import Process


class Enter(Signal):
    pass


class Exit(Signal):
    pass


class PickUp(Signal):
    pass


class PutDown(Signal):
    pass


class Philosopher(Process):
    def __init__(self, controller, lifespan):
        super(Philosopher, self).__init__(controller)
        self._lifespan = lifespan

        self._room = None
        self._left_fork = None
        self._right_fork = None

    def set_room(self, room):
        assert self._room is None
        self._room = room
        self.register_outputs(room)

    def set_left_fork(self, fork):
        assert self._left_fork is None
        self._left_fork = fork
        self.register_outputs(fork)

    def set_right_fork(self, fork):
        assert self._right_fork is None
        self._right_fork = fork
        self.register_outputs(fork)

    def _is_run_ready(self):
        return self._room is not None and self._left_fork is not None and self._right_fork is not None

    def _run(self):
        for _ in xrange(self._lifespan):
            # live
            # think
            yield self.await_output(self._room, Enter())
            yield self.await_output(self._left_fork, PickUp())
            yield self.await_output(self._right_fork, PickUp())
            # eat
            yield self.await_output(self._left_fork, PutDown())
            yield self.await_output(self._right_fork, PutDown())
            yield self.await_output(self._room, Exit())


class Fork(Process):
    def __init__(self, controller):
        super(Fork, self).__init__(controller)

        self._left_philosopher = None
        self._right_philosopher = None

    def set_left_philosopher(self, philosopher):
        assert self._left_philosopher is None
        self._left_philosopher = philosopher
        self.register_inputs(philosopher)

    def set_right_philosopher(self, philosopher):
        assert self._right_philosopher is None
        self._right_philosopher = philosopher
        self.register_inputs(philosopher)

    def _is_run_ready(self):
        return self._left_philosopher is not None and self._right_philosopher is not None

    def _run(self):
        while True:
            try:
                branch, _ = yield self.await_input({InputGuard(True, self._left_philosopher, PickUp): 'left',
                                                    InputGuard(True, self._right_philosopher, PickUp): 'right'})
            except CommandFailure:
                break

            if branch == 'left':
                yield self.await_input({InputGuard(True, self._left_philosopher, PutDown): 'the'})
                continue

            assert branch == 'right'
            yield self.await_input({InputGuard(True, self._right_philosopher, PutDown): 'the'})


class Room(Process):
    def __init__(self, controller):
        super(Room, self).__init__(controller)

        self._philosophers = set()
        self._occupancy = 0

    def add_philosophers(self, *philosophers):
        self._philosophers |= set(philosophers)
        self.register_inputs(*philosophers)

    def _is_run_ready(self):
        return len(self._philosophers) != 0

    def _enter(self):
        self._occupancy += 1
        assert self._occupancy <= len(self._philosophers)

    def _exit(self):
        self._occupancy -= 1
        assert self._occupancy >= 0

    def _run(self):
        guards = {}
        for philosopher in self._philosophers:
            guards[InputGuard(True, philosopher, Enter)] = self._enter
            guards[InputGuard(True, philosopher, Exit)] = self._exit

        while True:
            try:
                branch, _ = yield self.await_input(guards)
            except CommandFailure:
                assert self._occupancy == 0
                break
