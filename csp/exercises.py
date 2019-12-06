from papers.csp.controller import Controller, SequentialDispatcher, NaiveNetwork
from papers.csp.io_semantics import InputGuard, CommandFailure
from papers.csp.process import SingleInputProcess, SingleOutputProcess, SingleInputOutputProcess, AwaitInput


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


class Disassemble(SingleInputOutputProcess):
    def _run(self):
        while True:
            try:
                branch, value = yield self.await_input({InputGuard(True, self._input_process, None): 'the'})
                assert branch == 'the'
                assert len(value) <= 80
                for char in value:
                    self.await_output(self._output_process, char)
                self.await_output(self._output_process, ' ')
            except CommandFailure:
                break


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
