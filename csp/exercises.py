from papers.csp.controller import Controller, SequentialDispatcher, NaiveNetwork
from papers.csp.io_semantics import InputGuard, CommandFailure, NTuple
from papers.csp.process import SingleInputProcess, SingleOutputProcess, SingleInputOutputProcess, \
    SimpleAsyncWorkerProcess, AsyncCallerProcess


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


def ex_5_1():
    controller = Controller()
    SequentialDispatcher(controller)
    NaiveNetwork(controller)

    divmod_ = DivMod(controller)
    divmod_runner = DivModRunner(controller, [(22, 7), (81, 9), (10, 1)])
    divmod_.set_caller(divmod_runner)
    divmod_runner.set_worker('divmod', divmod_)

    controller.wire()
    controller.run()
