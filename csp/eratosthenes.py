from papers.csp.controller import Controller, NaiveNetwork, SequentialDispatcher
from papers.csp.io_semantics import InputGuard, CommandFailure
from papers.csp.process import Process


class Sieve(Process):
    def __init__(self, controller):
        super(Sieve, self).__init__(controller)
        self._previous = None
        self._next = None
        self._print = None

    def set_previous(self, previous):
        assert self._previous is None
        self._previous = previous
        self.register_inputs(previous)

    def set_next(self, next_):
        assert self._next is None
        self._next = next_
        self.register_outputs(next_)

    def set_print(self, print_):
        assert self._print is None
        self._print = print_
        self.register_outputs(print_)

    def _is_run_ready(self):
        return self._previous is not None and self._print is not None

    def _run(self):
        try:
            _, this_prime = yield self.await_input({InputGuard(True, self._previous, int): 'the'})
        except CommandFailure:
            return

        yield self.await_output(self._print, this_prime)
        prime_multiple = this_prime
        while True:
            try:
                _, candidate = yield self.await_input({InputGuard(True, self._previous, int): 'the'})
            except CommandFailure:
                break

            # Of course it would actually be faster just to use %
            while candidate > prime_multiple:
                prime_multiple += this_prime
            if candidate < prime_multiple:
                if self._next is None:
                    raise CommandFailure('Not enough processes')
                yield self.await_output(self._next, candidate)


class Seed(Process):
    def __init__(self, controller, limit):
        super(Seed, self).__init__(controller)
        self._limit = limit

        self._next = None
        self._print = None

    def set_next(self, next_):
        assert self._next is None
        self._next = next_
        self.register_outputs(next_)

    def set_print(self, print_):
        assert self._print is None
        self._print = print_
        self.register_outputs(print_)

    def _is_run_ready(self):
        return self._next is not None and self._print is not None

    def _run(self):
        yield self.await_output(self._print, 2)
        for n in xrange(3, self._limit, 2):
            yield self.await_output(self._next, n)


class Print(Process):
    def __init__(self, controller):
        super(Print, self).__init__(controller)
        self._inputs = set()

    def add_inputs(self, *inputs):
        self._inputs |= set(inputs)
        self.register_inputs(*inputs)

    def _is_run_ready(self):
        return len(self._inputs)

    def _run(self):
        matches = {InputGuard(True, input_, None): 'the' for input_ in self._inputs}
        while True:
            try:
                _, to_print = yield self.await_input(matches)
                print to_print
            except CommandFailure:
                break


def run(sieves=1229, limit=10000):
    controller = Controller()
    NaiveNetwork(controller)
    SequentialDispatcher(controller)

    print_ = Print(controller)

    seed = Seed(controller, limit)
    seed.set_print(print_)
    print_.add_inputs(seed)

    previous = seed
    for _ in xrange(sieves):
        sieve = Sieve(controller)
        sieve.set_print(print_)
        print_.add_inputs(sieve)

        previous.set_next(sieve)
        sieve.set_previous(previous)
        previous = sieve

    controller.wire()
    controller.run()
