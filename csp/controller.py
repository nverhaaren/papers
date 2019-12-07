import random
import sys
from collections import defaultdict

from papers.csp.io_semantics import CommandFailure
from papers.csp.process import AwaitInput, AwaitOutput, AwaitInit


class DeadlockError(Exception):
    pass


class SequentialDispatcher(object):
    def __init__(self, controller):
        self._controller = controller
        controller.set_dispatcher(self)

    @property
    def processes_running(self):
        return False

    def run_one(self, ready_awaits):
        await_ = random.choice(list(ready_awaits))
        process = await_.origin_process
        runner = self._controller.process_runner(process)
        self._controller.remove_ready(await_.origin_process)
        try:
            if process.check_failed_await():
                return runner.throw(CommandFailure, 'Failed await: {}'.format(type(await_)))

            # noinspection PyBroadException
            try:
                sending_value = await_.get_sending_value()
            except BaseException:
                return runner.throw(*sys.exc_info())

            return runner.send(sending_value)
        except StopIteration:
            self._controller.deactivate_process(process)
            return None

    def run_some(self, ready_processes):
        # TODO - figure out how things should work with more realistic dispatcher
        return self.run_one(ready_processes)


class NaiveNetwork(object):
    def __init__(self, controller):
        self._controller = controller
        controller.set_network(self)

        self._processes = set()
        self._await_inputs_by_dest = {}
        self._await_outputs_by_source = {}
        self._active_processes = set()
        self._ready_awaits_by_process = {}
        self._process_inputs = defaultdict(set)
        self._process_outputs = defaultdict(set)

    @property
    def active_processes(self):
        return frozenset(self._active_processes)

    @property
    def ready_awaits_by_process(self):
        return dict(self._ready_awaits_by_process)

    def add_process(self, process):
        self._processes.add(process)
        self._active_processes.add(process)
        self._ready_awaits_by_process[process] = AwaitInit(process)

    def add_process_input(self, receiver, sender):
        self.add_process(receiver)
        self.add_process(sender)
        self._process_inputs[receiver].add(sender)

    def add_process_output(self, sender, receiver):
        self.add_process(sender)
        self.add_process(receiver)
        self._process_outputs[sender].add(receiver)

    def validate(self):
        for process, inputs in self._process_inputs.iteritems():
            for input_ in inputs:
                assert process in self._process_outputs[input_]
        for process, outputs in self._process_outputs.iteritems():
            for output in outputs:
                assert process in self._process_inputs[output]

    def await_(self, await_):
        if isinstance(await_, AwaitInput):
            self._await_input(await_)
        elif isinstance(await_, AwaitOutput):
            self._await_output(await_)
        else:
            raise TypeError('Unknown await type {}'.format(type(await_)))

    def _await_input(self, await_input):
        dest_process = await_input.dest_process
        guarded_matches = await_input.guarded_matches
        assert not self._controller.is_ready(dest_process)

        if not guarded_matches:
            dest_process.fail_await()
            self.add_ready(await_input)
            return

        for input_guard, action in guarded_matches.items():
            source_process = input_guard.source_process
            value = None
            if source_process is not None:
                assert dest_process in self._process_outputs[source_process]
                assert source_process in self._process_inputs[dest_process]
                if source_process not in self._await_outputs_by_source:
                    continue
                await_output = self._await_outputs_by_source[source_process]
                value = await_output.value
                if await_output.dest_process is not dest_process:
                    continue
                # TODO: better deadlock detection
                if not input_guard.matches(source_process, value):
                    continue

            if isinstance(action, str):
                dest_process.set_branch_value(action, value)
            else:
                # should be callable
                dest_process.set_callback_input(action, value)
            await_output = self._await_outputs_by_source.pop(source_process)
            self.add_ready(await_input)
            self.add_ready(await_output)
            return

        assert dest_process not in self._await_inputs_by_dest
        self._await_inputs_by_dest[dest_process] = await_input

    def _await_output(self, await_output):
        source_process = await_output.source_process
        dest_process = await_output.dest_process
        value = await_output.value
        assert not self._controller.is_ready(source_process)
        assert source_process in self._process_inputs[dest_process]
        assert dest_process in self._process_outputs[source_process]

        if not dest_process.active:
            assert dest_process not in self._await_inputs_by_dest
            source_process.fail_await()
            self.add_ready(await_output)
            return

        if dest_process in self._await_inputs_by_dest:
            for input_guard, action in self._await_inputs_by_dest[dest_process].guarded_matches.items():
                if not input_guard.matches(source_process, value):
                    continue
                if isinstance(action, str):
                    dest_process.set_branch_value(action, value)
                else:
                    dest_process.set_callback_input(action, value)
                await_input = self._await_inputs_by_dest.pop(dest_process)
                self.add_ready(await_output)
                self.add_ready(await_input)
                return

        assert source_process not in self._await_outputs_by_source
        self._await_outputs_by_source[source_process] = await_output

    def deactivate_process(self, process):
        self._active_processes.remove(process)
        for dest_process, await_input in self._await_inputs_by_dest.items():
            guarded_matches = await_input.guarded_matches
            assert dest_process is not process
            for input_guard in guarded_matches.keys():
                if not input_guard.viable:
                    del guarded_matches[input_guard]
            if not guarded_matches:
                await_input = self._await_inputs_by_dest.pop(dest_process)
                dest_process.fail_await()
                self.add_ready(await_input)

        for source_process, await_output in self._await_outputs_by_source.items():
            assert source_process is not process
            if await_output.dest_process is process:
                await_output = self._await_outputs_by_source.pop(source_process)
                source_process.fail_await()
                self.add_ready(await_output)

    def remove_ready(self, process):
        assert process not in self._await_inputs_by_dest
        assert process not in self._await_outputs_by_source
        del self._ready_awaits_by_process[process]

    def add_ready(self, await_):
        # Maybe should be private?
        process = await_.origin_process
        assert process not in self._ready_awaits_by_process
        assert process not in self._await_inputs_by_dest
        assert process not in self._await_outputs_by_source
        self._ready_awaits_by_process[process] = await_


class Controller(object):
    def __init__(self):
        self._dispatcher = None
        self._network = None

        self._processes = set()
        self._runners_by_process = None

        self._wired = False

    @property
    def active_processes(self):
        return self._network.active_processes if self._wired else frozenset()

    @property
    def ready_awaits_by_process(self):
        return self._network.ready_awaits_by_process if self._wired else {}

    def set_dispatcher(self, dispatcher):
        assert self._dispatcher is None
        self._dispatcher = dispatcher

    def set_network(self, network):
        assert self._network is None
        self._network = network

    def add_process(self, process):
        assert not self._wired
        self._network.add_process(process)
        self._processes.add(process)

    def add_process_input(self, receiver, sender):
        assert not self._wired
        self.add_process(receiver)
        self.add_process(sender)
        self._network.add_process_input(receiver, sender)

    def add_process_output(self, sender, receiver):
        assert not self._wired
        self.add_process(sender)
        self.add_process(receiver)
        self._network.add_process_output(sender, receiver)

    def wire(self):
        self._network.validate()
        self._runners_by_process = {process: process.run() for process in self._processes}
        self._wired = True

    def is_active(self, process):
        return process in self.active_processes

    def remove_ready(self, process):
        assert self._wired
        self._network.remove_ready(process)

    def add_ready(self, await_):
        assert self._wired
        self._network.add_ready(await_)

    def is_ready(self, process):
        return process in self.ready_awaits_by_process

    def process_runner(self, process):
        return self._runners_by_process[process]

    def deactivate_process(self, process):
        assert self._wired
        self._network.deactivate_process(process)

    def run(self):
        assert self._wired
        while self.active_processes:
            if not self.ready_awaits_by_process:
                if self._dispatcher.processes_running:
                    continue
                raise DeadlockError('No processes can be run')
            await_ = self._dispatcher.run_one(self.ready_awaits_by_process.itervalues())
            if await_ is None:
                continue
            self._network.await_(await_)



#TODO: locks somewhere
