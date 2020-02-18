import random
from collections import namedtuple

from papers.csp.controller import Controller, SequentialDispatcher, NaiveNetwork, DeadlockError
from papers.csp.io_semantics import InputGuard, CommandFailure, NTuple, Signal
from papers.csp.process import SingleInputProcess, SingleOutputProcess, SingleInputOutputProcess, \
    SimpleAsyncWorkerProcess, AsyncCallerProcess, Process


class SendChars(SingleOutputProcess):
    def __init__(self, controller, data):
        super(SendChars, self).__init__(controller)
        self._data = data

    def _run(self):
        for datum in self._data:
            yield self.await_output(self._output_process, datum)


class ReceiveChars(SingleInputProcess):
    def _run(self):
        while True:
            try:
                branch, value = yield self.await_input({InputGuard(self._input_process): 'print'})
                # This kind of assert is due to the library also being tested, once confident in its correctness it would not be necessary
                assert branch == 'print'
                print(repr(value))
            except CommandFailure:
                break


class Copy(SingleInputOutputProcess):
    def _run(self):
        while True:
            try:
                branch, value = yield self.await_input(InputGuard.single_match(self._input_process))
                assert branch == 'the'
                yield self.await_output(self._output_process, value)
            except CommandFailure:
                break


class Squash(SingleInputOutputProcess):
    def _run(self):
        while True:
            try:
                branch, value = yield self.await_input(InputGuard.single_match(self._input_process))
                assert branch == 'the'
                if value != '*':
                    yield self.await_output(self._output_process, value)
                    continue
                branch, value = yield self.await_input(InputGuard.single_match(self._input_process))
                assert branch == 'the'
                if value != '*':
                    yield self.await_output(self._output_process, '*')
                    yield self.await_output(self._output_process, value)
                    continue

                yield self.await_output(self._output_process, '^')
            except CommandFailure:
                break


class ImprovedSquash(SingleInputOutputProcess):
    def _run(self):
        final = None
        while True:
            try:
                branch, value = yield self.await_input(InputGuard.single_match(self._input_process))
                assert branch == 'the'
                if value != '*':
                    yield self.await_output(self._output_process, value)
                    continue

                final = '*'
                branch, value = yield self.await_input(InputGuard.single_match(self._input_process))
                assert branch == 'the'
                final = None
                if value != '*':
                    yield self.await_output(self._output_process, '*')
                    yield self.await_output(self._output_process, value)
                    continue

                yield self.await_output(self._output_process, '^')
            except CommandFailure:
                if final is not None:
                    try:
                        yield self.await_output(self._output_process, final)
                    except CommandFailure:
                        pass
                break


class CardFile(SingleOutputProcess):
    def __init__(self, controller, data):
        super(CardFile, self).__init__(controller)
        self._remaining_data = data[:]

    def _run(self):
        while self._remaining_data:
            output, self._remaining_data = self._remaining_data[:80], self._remaining_data[80:]
            yield self.await_output(self._output_process, output)


class Disassemble(SingleInputOutputProcess):
    def _run(self):
        while True:
            try:
                branch, value = yield self.await_input(InputGuard.single_match(self._input_process))
                assert branch == 'the'
                assert len(value) <= 80
                for char in value:
                    yield self.await_output(self._output_process, char)
                yield self.await_output(self._output_process, ' ')
            except CommandFailure:
                break


class Assemble(SingleInputOutputProcess):
    def _run(self):
        lineimage = []
        while True:
            try:
                _, char = yield self.await_input(InputGuard.single_match(self._input_process))
                lineimage.append(char)
                if len(lineimage) >= 125:
                    yield self.await_output(self.output_process, lineimage)
                    lineimage = []
            except CommandFailure:
                if lineimage:
                    lineimage += [' '] * (125 - len(lineimage))
                yield self.await_output(self.output_process, lineimage)
                break


class LinePrinter(SingleInputProcess):
    def _run(self):
        while True:
            try:
                _, line = yield self.await_input(InputGuard.single_match(self._input_process))
                assert len(line) == 125
                print ''.join(line)
            except CommandFailure:
                break


class DivMod(SimpleAsyncWorkerProcess):
    def _run(self):
        while True:
            try:
                _, (dividend, divisor) = yield self.await_input(InputGuard.single_match(self.caller_process, NTuple(2)))
                assert dividend >= 0 and divisor > 0
                quotient = 0
                remainder = dividend
                while remainder >= divisor:
                    remainder -= divisor
                    quotient += 1
                yield self.await_output(self.caller_process, (quotient, remainder))
            except CommandFailure:
                break


class DivModRunner(AsyncCallerProcess):
    def __init__(self, controller, problems):
        super(DivModRunner, self).__init__(controller)
        self._problems = problems

    def _run(self):
        for dividend, divisor in self._problems:
            yield self.await_output(self.get_worker('divmod'), (dividend, divisor))
            _, (quotient, remainder) = yield self.await_input(InputGuard.single_match(self.get_worker('divmod'), NTuple(2)))
            print "{}/{} = {} + {}/{}".format(dividend, divisor, quotient, remainder, divisor)


def trivial():
    controller = Controller()
    # Maybe not best interface?
    SequentialDispatcher(controller)
    NaiveNetwork(controller)

    west = SendChars(controller, 'Hello world!')
    east = ReceiveChars(controller)
    west.set_output(east)
    east.set_input(west)

    controller.wire()
    controller.run()


def ex3_1():
    controller = Controller()
    SequentialDispatcher(controller)
    NaiveNetwork(controller)

    west = SendChars(controller, 'Hello world!')
    east = ReceiveChars(controller)
    copy = Copy(controller)
    west.set_output(copy)
    copy.set_input(west)
    east.set_input(copy)
    copy.set_output(east)

    controller.wire()
    controller.run()


def ex3_2():
    controller = Controller()
    SequentialDispatcher(controller)
    NaiveNetwork(controller)

    west = SendChars(controller, 'Hello* world**!*')
    east = ReceiveChars(controller)
    squash = Squash(controller)
    west.set_output(squash)
    squash.set_input(west)
    east.set_input(squash)
    squash.set_output(east)

    controller.wire()
    controller.run()


def ex3_2_improved():
    controller = Controller()
    SequentialDispatcher(controller)
    NaiveNetwork(controller)

    west = SendChars(controller, 'Hello* world**!*')
    east = ReceiveChars(controller)
    squash = ImprovedSquash(controller)
    west.set_output(squash)
    squash.set_input(west)
    east.set_input(squash)
    squash.set_output(east)

    controller.wire()
    controller.run()


def ex3_5():
    controller = Controller()
    SequentialDispatcher(controller)
    NaiveNetwork(controller)

    data_seed = '0123456789*0246813579'
    data = ''.join(c * 10 for c in data_seed)
    west = CardFile(controller, data)
    east = LinePrinter(controller)

    disassemble = Disassemble(controller)
    west.set_output(disassemble)
    disassemble.set_input(west)

    copy = Copy(controller)
    disassemble.set_output(copy)
    copy.set_input(disassemble)

    assemble = Assemble(controller)
    copy.set_output(assemble)
    assemble.set_input(copy)

    assemble.set_output(east)
    east.set_input(assemble)

    controller.wire()
    controller.run()


def ex3_6():
    controller = Controller()
    SequentialDispatcher(controller)
    NaiveNetwork(controller)

    data_seed = '0123456789*0246813579'
    data = ''.join(c * 10 for c in data_seed)
    west = CardFile(controller, data)
    east = LinePrinter(controller)

    disassemble = Disassemble(controller)
    west.set_output(disassemble)
    disassemble.set_input(west)

    squash = Squash(controller)
    disassemble.set_output(squash)
    squash.set_input(disassemble)

    assemble = Assemble(controller)
    squash.set_output(assemble)
    assemble.set_input(squash)

    assemble.set_output(east)
    east.set_input(assemble)

    controller.wire()
    controller.run()


def ex_4_1():
    controller = Controller()
    SequentialDispatcher(controller)
    NaiveNetwork(controller)

    divmod_ = DivMod(controller)
    divmod_runner = DivModRunner(controller, [(22, 7), (81, 9), (10, 1)])
    divmod_.set_caller(divmod_runner)
    divmod_runner.set_worker('divmod', divmod_)

    controller.wire()
    controller.run()


class Factorial(Process):
    def __init__(self, controller):
        super(Factorial, self).__init__(controller)
        self._previous_process = None
        self._next_process = None

    def set_previous_process(self, process):
        self._previous_process = process
        self.register_inputs(process)
        self.register_outputs(process)

    def set_next_process(self, process):
        self._next_process = process
        self.register_inputs(process)
        self.register_outputs(process)

    @property
    def _is_run_ready(self):
        return self._previous_process is not None and self._next_process is not None

    def _run(self):
        while True:
            try:
                _, input_ = yield self.await_input(InputGuard.single_match(self._previous_process))
                if input_ == 0:
                    yield self.await_output(self._previous_process, 1)
                    continue
                yield self.await_output(self._next_process, input_ - 1)
                _, result = yield self.await_input(InputGuard.single_match(self._next_process))
                yield self.await_output(self._previous_process, input_ * result)
            except CommandFailure:
                break


class FailProcess(Process):
    def __init__(self, controller):
        super(FailProcess, self).__init__(controller)
        self._input_processes = []

    def add_input_process(self, process):
        self._input_processes.append(process)
        self.register_inputs(process)

    @property
    def _is_run_ready(self):
        return len(self._input_processes) > 0

    def _run(self):
        def fail_(input_):
            raise RuntimeError('FailProcess received input {!r}'.format(input_))
        guarded_matches = {InputGuard(process): fail_ for process in self._input_processes}
        while True:
            try:
                yield self.await_input(guarded_matches)
            except CommandFailure:
                break


class FactorialRunner(AsyncCallerProcess):
    def __init__(self, controller, inputs):
        super(FactorialRunner, self).__init__(controller)
        self._inputs = inputs

    def _run(self):
        for input_ in self._inputs:
            yield self.await_output(self.get_worker('factorial'), input_)
            # This is basically an async await, as are the Factorial processes themselves
            _, value = yield self.await_input(InputGuard.single_match(self.get_worker('factorial')))
            print '{}! = {}'.format(input_, value)


def ex_4_2():
    controller = Controller()
    SequentialDispatcher(controller)
    NaiveNetwork(controller)

    inputs = [9, 12, 0, 1, 2, 7]
    num_nodes = max(inputs) + 1

    runner = FactorialRunner(controller, inputs)
    factorial_nodes = [Factorial(controller) for _ in range(num_nodes)]
    fail = FailProcess(controller)

    for i in range(num_nodes):
        if i == 0:
            factorial_nodes[i].set_previous_process(runner)
            runner.set_worker('factorial', factorial_nodes[i])
        else:
            factorial_nodes[i].set_previous_process(factorial_nodes[i - 1])

        if i == num_nodes - 1:
            factorial_nodes[i].set_next_process(fail)
            fail.add_input_process(factorial_nodes[i])
            fail.register_outputs(factorial_nodes[i])
        else:
            factorial_nodes[i].set_next_process(factorial_nodes[i + 1])

    controller.wire()
    controller.run()


def ex_4_2_limits():
    controller = Controller()
    SequentialDispatcher(controller)
    NaiveNetwork(controller)

    inputs = [9, 12, 0, 1, 2, 7]
    num_nodes = 10

    runner = FactorialRunner(controller, inputs)
    factorial_nodes = [Factorial(controller) for _ in range(num_nodes)]
    fail = FailProcess(controller)

    for i in range(num_nodes):
        if i == 0:
            factorial_nodes[i].set_previous_process(runner)
            runner.set_worker('factorial', factorial_nodes[i])
        else:
            factorial_nodes[i].set_previous_process(factorial_nodes[i - 1])

        if i == num_nodes - 1:
            factorial_nodes[i].set_next_process(fail)
            fail.add_input_process(factorial_nodes[i])
            fail.register_outputs(factorial_nodes[i])
        else:
            factorial_nodes[i].set_next_process(factorial_nodes[i + 1])

    controller.wire()
    try:
        controller.run()
    except RuntimeError as e:
        print e


class Ex43Runner(AsyncCallerProcess):
    def _run(self):
        worker = self.get_worker('set')
        yield self.await_output(worker, Has43(0))
        _, value = yield self.await_input(InputGuard.single_match(worker))
        assert not value

        yield self.await_output(worker, Insert43(0))
        yield self.await_output(worker, Insert43(1))
        yield self.await_output(worker, Insert43(0))

        yield self.await_output(worker, Has43(1))
        _, value = yield self.await_input(InputGuard.single_match(worker))
        assert value

        yield self.await_output(worker, Has43(2))
        _, value = yield self.await_input(InputGuard.single_match(worker))
        assert not value

        yield self.await_output(worker, Has43(0))
        _, value = yield self.await_input(InputGuard.single_match(worker))
        assert value


class Ex43DeadlockRunner(AsyncCallerProcess):
    def _run(self):
        worker = self.get_worker('set')
        yield self.await_output(worker, Has43(0))
        _, value = yield self.await_input(InputGuard.single_match(worker))
        assert not value

        yield self.await_output(worker, Insert43(0))
        yield self.await_output(worker, Insert43(1))
        yield self.await_output(worker, Insert43(0))

        _, value = yield self.await_input(InputGuard.single_match(worker))


class Ex43OverflowRunner(AsyncCallerProcess):
    def _run(self):
        for i in range(100):
            yield self.await_output(self.get_worker('set'), Insert43(i))

        # TODO: where these are received - maybe nowhere as is
        try:
            yield self.await_output(self.get_worker('set'), Insert43(100))
        except CommandFailure:
            print 'Received CommandFailure in unexpected place'
            raise
        else:
            print 'This should be followed by a CommandFailure exception'


Has43 = namedtuple('Has43', 'n')
Insert43 = namedtuple('Insert43', 'n')


class Set43(SimpleAsyncWorkerProcess):
    MAX_SIZE = 100

    def __init__(self, controller):
        super(Set43, self).__init__(controller)
        self._content = set()

    def _run(self):
        while True:
            try:
                branch, value = yield self.await_input({InputGuard(self._caller_process, Has43): 'has',
                                                        InputGuard(self._caller_process, Insert43): 'insert'})
                if branch == 'has':
                    yield self.await_output(self._caller_process, value.n in self._content)
                    continue
            except CommandFailure:
                break

            assert branch == 'insert'
            if len(self._content) >= self.MAX_SIZE:
                raise CommandFailure('Set already full of {} elements'.format(self.MAX_SIZE))
            self._content.add(value.n)


def ex_4_3():
    controller = Controller()
    SequentialDispatcher(controller)
    NaiveNetwork(controller)

    # uses a Python set; essentially we are wrapping a Python set and imposing a size limit

    runner = Ex43Runner(controller)
    set_worker = Set43(controller)

    runner.set_worker('set', set_worker)
    set_worker.set_caller(runner)

    controller.wire()
    controller.run()

    print 'Ran to completion'


def ex_4_3_deadlock():
    controller = Controller()
    SequentialDispatcher(controller)
    NaiveNetwork(controller)

    # uses a Python set; essentially we are wrapping a Python set and imposing a size limit

    runner = Ex43DeadlockRunner(controller)
    set_worker = Set43(controller)

    runner.set_worker('set', set_worker)
    set_worker.set_caller(runner)

    controller.wire()
    try:
        controller.run()
    except DeadlockError:
        print 'Received expected DeadlockError'
    else:
        print 'Did not receive expected DeadlockError'


def ex_4_3_overflow():
    controller = Controller()
    SequentialDispatcher(controller)
    NaiveNetwork(controller)

    # uses a Python set; essentially we are wrapping a Python set and imposing a size limit

    runner = Ex43OverflowRunner(controller)
    set_worker = Set43(controller)

    runner.set_worker('set', set_worker)
    set_worker.set_caller(runner)

    controller.wire()
    controller.run()


class Scan44(Signal):
    pass


class NoneLeft44(Signal):
    pass


Next44 = namedtuple('Next44', ['n'])


class Set44(SimpleAsyncWorkerProcess):
    MAX_SIZE = 100

    def __init__(self, controller):
        super(Set44, self).__init__(controller)
        self._content = set()

    def _run(self):
        while True:
            try:
                branch, value = yield self.await_input({InputGuard(self._caller_process, Has43): 'has',
                                                        InputGuard(self._caller_process, Insert43): 'insert',
                                                        InputGuard(self._caller_process, Scan44): 'scan'})
                if branch == 'has':
                    yield self.await_output(self._caller_process, value.n in self._content)
                    continue
            except CommandFailure:
                break

            # Note that this does preserve insertion order, which the given code would but seems incidental as the
            # structure is termed a set
            if branch == 'scan':
                for n in self._content:
                    yield self.await_output(self._caller_process, Next44(n))
                yield self.await_output(self.caller_process, NoneLeft44())
                continue

            assert branch == 'insert'
            if len(self._content) >= self.MAX_SIZE:
                raise CommandFailure('Set already full of {} elements'.format(self.MAX_SIZE))
            self._content.add(value.n)


class Ex44Runner(AsyncCallerProcess):
    def __init__(self, controller, inputs):
        super(Ex44Runner, self).__init__(controller)
        self._inputs = inputs

    def _run(self):
        set_worker = self.get_worker('set')

        for n in self._inputs:
            yield self.await_output(set_worker, Insert43(n))

        for x in range(max(self._inputs) + 1):
            yield self.await_output(set_worker, Has43(x))
            _, value = yield self.await_input(InputGuard.single_match(set_worker))
            assert value is (x in self._inputs)

        echo = set()
        yield self.await_output(set_worker, Scan44())
        while True:
            branch, value = yield self.await_input({InputGuard(set_worker, Next44): 'next',
                                                    InputGuard(set_worker, NoneLeft44): 'none_left'})
            if branch == 'none_left':
                break
            assert branch == 'next'
            assert value.n not in echo
            echo.add(value.n)

        assert echo == self._inputs


def ex_4_4():
    test_set = set(random.sample(range(1000), k=32))

    controller = Controller()
    SequentialDispatcher(controller)
    NaiveNetwork(controller)

    # uses a Python set; essentially we are wrapping a Python set and imposing a size limit

    runner = Ex44Runner(controller, test_set)
    set_worker = Set44(controller)

    runner.set_worker('set', set_worker)
    set_worker.set_caller(runner)

    controller.wire()
    controller.run()

    print 'Ran to completion'


Has45 = namedtuple('Has45', ['n'])
Insert45 = namedtuple('Insert45', ['n'])


class Ex45Runner(Process):
    def __init__(self, controller, inputs):
        super(Ex45Runner, self).__init__(controller)
        self._inputs = inputs
        self._receiver_process = None
        self._response_processes = set()

    def set_receiver_process(self, receiver_process):
        assert self._receiver_process is None
        self.register_outputs(receiver_process)
        self._receiver_process = receiver_process

    def add_response_process(self, response_process):
        self.register_inputs(response_process)
        self._response_processes.add(response_process)

    def _is_run_ready(self):
        return self._receiver_process is not None and self._response_processes

    def _run(self):
        inserted = set()
        previous = None
        set_worker = self._receiver_process
        response_matches = {InputGuard(process): 'the' for process in self._response_processes}
        for n in self._inputs:
            yield self.await_output(set_worker, Insert45(n))
            inserted.add(n)
            if previous is not None:
                yield self.await_output(set_worker, Has45(previous))
                _, value = yield self.await_input(response_matches)
                assert value
            yield self.await_output(set_worker, Has45(n))
            _, value = yield self.await_input(response_matches)
            assert value

            some = random.sample(self._inputs, 1)[0]
            yield self.await_output(set_worker, Has45(some))
            _, value = yield self.await_input(response_matches)
            assert value is (some in inserted)


class Set45Worker(Process):
    def __init__(self, controller):
        super(Set45Worker, self).__init__(controller)
        self._previous_process = None
        self._next_process = None
        self._originating_process = None

    def set_previous_process(self, previous_process):
        assert self._previous_process is None
        self.register_inputs(previous_process)
        self._previous_process = previous_process

    def set_next_process(self, next_process):
        assert self._next_process is None
        self.register_outputs(next_process)
        self._next_process = next_process

    def set_originating_process(self, originating_process):
        assert self._originating_process is None
        self.register_outputs(originating_process)
        self._originating_process = originating_process

    def _is_run_ready(self):
        return (self._next_process is not None and
                self._previous_process is not None and
                self._originating_process is not None)

    def _run(self):
        while True:
            try:
                branch, value = yield self.await_input({InputGuard(self._previous_process, Has45): 'has',
                                                        InputGuard(self._previous_process, Insert45): 'insert'})
                if branch == 'has':
                    yield self.await_output(self._originating_process, False)
                    continue

                assert branch == 'insert'
                current_value = value.n
                while True:
                    branch, value = yield self.await_input({InputGuard(self._previous_process, Has45): 'has',
                                                            InputGuard(self._previous_process, Insert45): 'insert'})
                    if branch == 'has':
                        if value.n <= current_value:
                            yield self.await_output(self._originating_process, value.n == current_value)
                            continue
                        yield self.await_output(self._next_process, value)
                        continue

                    assert branch == 'insert'
                    if value.n < current_value:
                        yield self.await_output(self._next_process, Insert45(current_value))
                        current_value = value.n
                        continue
                    if value.n == current_value:
                        continue
                    assert value.n > current_value
                    yield self.await_output(self._next_process, value)
            except CommandFailure:
                break


def ex_4_5():
    controller = Controller()
    NaiveNetwork(controller)
    SequentialDispatcher(controller)

    inputs = random.sample(range(1000), 32)

    runner = Ex45Runner(controller, inputs)
    previous = runner
    for i in range(100):
        worker = Set45Worker(controller)
        worker.set_previous_process(previous)
        if i == 0:
            runner.set_receiver_process(worker)
        else:
            previous.set_next_process(worker)
        runner.add_response_process(worker)
        worker.set_originating_process(runner)

        previous = worker

    fail = FailProcess(controller)
    previous.set_next_process(fail)
    fail.add_input_process(previous)

    controller.wire()
    controller.run()

    print 'Ran to completion'


class Least46(Signal):
    pass


class Set46Worker(Process):
    def __init__(self, controller):
        super(Set46Worker, self).__init__(controller)
        self._previous_process = None
        self._next_process = None
        self._originating_process = None

    def set_previous_process(self, previous_process):
        assert self._previous_process is None
        self.register_inputs(previous_process)
        self.register_outputs(previous_process)
        self._previous_process = previous_process

    def set_next_process(self, next_process):
        assert self._next_process is None
        self.register_outputs(next_process)
        self.register_inputs(next_process)
        self._next_process = next_process

    def set_originating_process(self, originating_process):
        assert self._originating_process is None
        self.register_outputs(originating_process)
        self._originating_process = originating_process

    def _is_run_ready(self):
        return (self._next_process is not None and
                self._previous_process is not None and
                self._originating_process is not None)

    def _run(self):
        while True:
            try:
                branch, value = yield self.await_input({InputGuard(self._previous_process, Has45): 'has',
                                                        InputGuard(self._previous_process, Insert45): 'insert',
                                                        InputGuard(self._previous_process, Least46): 'least'})
                if branch == 'has':
                    yield self.await_output(self._originating_process, False)
                    continue

                if branch == 'least':
                    yield self.await_output(self._previous_process, NoneLeft44())
                    continue

                assert branch == 'insert'
                current_value = value.n
                while True:
                    branch, value = yield self.await_input({InputGuard(self._previous_process, Has45): 'has',
                                                            InputGuard(self._previous_process, Insert45): 'insert',
                                                            InputGuard(self._previous_process, Least46): 'least'})
                    if branch == 'has':
                        if value.n <= current_value:
                            yield self.await_output(self._originating_process, value.n == current_value)
                            continue
                        yield self.await_output(self._next_process, value)
                        continue

                    if branch == 'least':
                        yield self.await_output(self._previous_process, current_value)
                        yield self.await_output(self._next_process, value)
                        branch, value = yield self.await_input({InputGuard(self._next_process, int): 'least',
                                                                InputGuard(self._next_process, NoneLeft44): 'none_left'})
                        if branch == 'least':
                            current_value = value
                            continue
                        assert branch == 'none_left'
                        break

                    assert branch == 'insert'
                    if value.n < current_value:
                        yield self.await_output(self._next_process, Insert45(current_value))
                        current_value = value.n
                        continue
                    if value.n == current_value:
                        continue
                    assert value.n > current_value
                    yield self.await_output(self._next_process, value)
            except CommandFailure:
                break


class Ex46Runner(Process):
    def __init__(self, controller, inputs):
        super(Ex46Runner, self).__init__(controller)
        self._inputs = inputs
        self._receiver_process = None
        self._response_processes = set()

    def set_receiver_process(self, receiver_process):
        assert self._receiver_process is None
        self.register_outputs(receiver_process)
        self.register_inputs(receiver_process)
        self._receiver_process = receiver_process

    def add_response_process(self, response_process):
        self.register_inputs(response_process)
        self._response_processes.add(response_process)

    def _is_run_ready(self):
        return self._receiver_process is not None and self._response_processes

    def _run(self):
        inserted = set()
        previous = None
        set_worker = self._receiver_process
        response_matches = {InputGuard(process): 'the' for process in self._response_processes}
        for n in self._inputs:
            yield self.await_output(set_worker, Insert45(n))
            inserted.add(n)
            if previous is not None:
                yield self.await_output(set_worker, Has45(previous))
                _, value = yield self.await_input(response_matches)
                assert value
            yield self.await_output(set_worker, Has45(n))
            _, value = yield self.await_input(response_matches)
            assert value

            some = random.sample(self._inputs, 1)[0]
            yield self.await_output(set_worker, Has45(some))
            _, value = yield self.await_input(response_matches)
            assert value is (some in inserted)

        while True:
            yield self.await_output(set_worker, Least46())
            branch, value = yield self.await_input({InputGuard(set_worker, NoneLeft44): 'none_left',
                                                    InputGuard(set_worker, int): 'least'})
            if branch == 'none_left':
                break
            assert value == min(inserted)
            inserted.remove(value)
            yield self.await_output(set_worker, Has45(value))
            _, value = yield self.await_input(response_matches)
            assert not value
        assert not inserted


def ex_4_6():
    controller = Controller()
    NaiveNetwork(controller)
    SequentialDispatcher(controller)

    inputs = random.sample(range(1000), 32)

    runner = Ex46Runner(controller, inputs)
    previous = runner
    for i in range(100):
        worker = Set46Worker(controller)
        worker.set_previous_process(previous)
        if i == 0:
            runner.set_receiver_process(worker)
        else:
            previous.set_next_process(worker)
        runner.add_response_process(worker)
        worker.set_originating_process(runner)

        previous = worker

    fail = FailProcess(controller)
    previous.set_next_process(fail)
    fail.add_input_process(previous)
    # will not output, ok since it fails on any input and it would receive input first
    fail.register_outputs(previous)

    controller.wire()
    controller.run()

    print 'Ran to completion'
