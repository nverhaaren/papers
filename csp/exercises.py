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
                branch, value = yield self.await_input({InputGuard(True, self._input_process, None): 'print'})
                # This kind of assert is due to the library also being tested, once confident in its correctness it would not be necessary
                assert branch == 'print'
                print(repr(value))
            except CommandFailure:
                break


class Copy(SingleInputOutputProcess):
    def _run(self):
        while True:
            try:
                branch, value = yield self.await_input({InputGuard(True, self._input_process, None): 'the'})
                assert branch == 'the'
                yield self.await_output(self._output_process, value)
            except CommandFailure:
                break


class Squash(SingleInputOutputProcess):
    def _run(self):
        while True:
            try:
                branch, value = yield self.await_input({InputGuard(True, self._input_process, None): 'the'})
                assert branch == 'the'
                if value != '*':
                    yield self.await_output(self._output_process, value)
                    continue
                branch, value = yield self.await_input({InputGuard(True, self._input_process, None): 'the'})
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
                branch, value = yield self.await_input({InputGuard(True, self._input_process, None): 'the'})
                assert branch == 'the'
                if value != '*':
                    yield self.await_output(self._output_process, value)
                    continue

                final = '*'
                branch, value = yield self.await_input({InputGuard(True, self._input_process, None): 'the'})
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
                branch, value = yield self.await_input({InputGuard(True, self._input_process, None): 'the'})
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
                _, char = yield self.await_input({InputGuard(True, self.input_process, None): 'the'})
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
                _, line = yield self.await_input({InputGuard(True, self.input_process, None): 'the'})
                assert len(line) == 125
                print ''.join(line)
            except CommandFailure:
                break


class DivMod(SimpleAsyncWorkerProcess):
    def _run(self):
        while True:
            try:
                _, (dividend, divisor) = yield self.await_input({InputGuard(True, self.caller_process, NTuple(2)): 'the'})
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
            _, (quotient, remainder) = yield self.await_input({InputGuard(True, self.get_worker('divmod'), NTuple(2)): 'the'})
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
                _, input_ = yield self.await_input({InputGuard(True, self._previous_process, None): 'the'})
                if input_ == 0:
                    yield self.await_output(self._previous_process, 1)
                    continue
                yield self.await_output(self._next_process, input_ - 1)
                _, result = yield self.await_input({InputGuard(True, self._next_process, None): 'the'})
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
        guarded_matches = {InputGuard(True, process, None): fail_ for process in self._input_processes}
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
            _, value = yield self.await_input({InputGuard(True, self.get_worker('factorial'), None): 'the'})
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
        _, value = yield self.await_input({InputGuard(True, worker, None): 'the'})
        assert not value

        yield self.await_output(worker, Insert43(0))
        yield self.await_output(worker, Insert43(1))
        yield self.await_output(worker, Insert43(0))

        yield self.await_output(worker, Has43(1))
        _, value = yield self.await_input({InputGuard(True, worker, None): 'the'})
        assert value

        yield self.await_output(worker, Has43(2))
        _, value = yield self.await_input({InputGuard(True, worker, None): 'the'})
        assert not value

        yield self.await_output(worker, Has43(0))
        _, value = yield self.await_input({InputGuard(True, worker, None): 'the'})
        assert value


class Ex43DeadlockRunner(AsyncCallerProcess):
    def _run(self):
        worker = self.get_worker('set')
        yield self.await_output(worker, Has43(0))
        _, value = yield self.await_input({InputGuard(True, worker, None): 'the'})
        assert not value

        yield self.await_output(worker, Insert43(0))
        yield self.await_output(worker, Insert43(1))
        yield self.await_output(worker, Insert43(0))

        _, value = yield self.await_input({InputGuard(True, worker, None): 'the'})


class Ex43OverflowRunner(AsyncCallerProcess):
    def  _run(self):
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
                branch, value = yield self.await_input({InputGuard(True, self._caller_process, Has43): 'has',
                                                        InputGuard(True, self._caller_process, Insert43): 'insert'})
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
