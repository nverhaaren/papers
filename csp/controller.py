import random
from collections import defaultdict

from papers.csp.process import AwaitInput, AwaitOutput


class DeadlockError(Exception):
    pass


class SequentialDispatcher(object):
    def __init__(self, controller):
        self._controller = controller
        controller.set_dispatcher(self)

    @property
    def processes_running(self):
        return False

    def run_one(self, ready_processes):
        process = random.choice(list(ready_processes))
        runner = self._controller.process_runner(process)
        self._controller.mark_unready(process)
        try:
            return process, next(runner)
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
        self._input_guards_by_dest = {}
        self._outputs_by_source = {}
        self._active_processes = set()
        self._ready_processes = set()
        self._process_inputs = defaultdict(set)
        self._process_outputs = defaultdict(set)


    @property
    def active_processes(self):
        return frozenset(self._active_processes)

    @property
    def ready_processes(self):
        return frozenset(self._ready_processes)

    def add_process(self, process):
        self._processes.add(process)
        self._active_processes.add(process)
        self._ready_processes.add(process)

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

    def await_input(self, dest_process, guarded_matches):
        assert not self._controller.is_ready(dest_process)

        if not guarded_matches:
            dest_process.fail_await()
            self.mark_ready(dest_process)
            return

        for input_guard, action in guarded_matches.items():
            source_process = input_guard.source_process
            value = None
            if source_process is not None:
                assert dest_process in self._process_outputs[source_process]
                assert source_process in self._process_inputs[dest_process]
                if source_process not in self._outputs_by_source:
                    continue
                value, intended_dest = self._outputs_by_source[source_process]
                if intended_dest is not dest_process:
                    continue
                # TODO: better deadlock detection
                if not input_guard.matches(source_process, value):
                    continue

            if isinstance(action, str):
                dest_process.set_branch_value(action, value)
            else:
                # should be callable
                dest_process.set_callback_input(action, value)
            del self._outputs_by_source[source_process]
            self.mark_ready(dest_process)
            self.mark_ready(source_process)
            return

        assert dest_process not in self._input_guards_by_dest
        self._input_guards_by_dest[dest_process] = guarded_matches

    def await_output(self, source_process, dest_process, value):
        assert not self._controller.is_ready(source_process)
        assert source_process in self._process_inputs[dest_process]
        assert dest_process in self._process_outputs[source_process]

        if not dest_process.active:
            assert dest_process not in self._input_guards_by_dest
            source_process.fail_await()
            self.mark_ready(source_process)
            return

        if dest_process in self._input_guards_by_dest:
            for input_guard, action in self._input_guards_by_dest[dest_process].items():
                if not input_guard.matches(source_process, value):
                    continue
                if isinstance(action, str):
                    dest_process.set_branch_value(action, value)
                else:
                    dest_process.set_callback_input(action, value)
                del self._input_guards_by_dest[dest_process]
                self.mark_ready(source_process)
                self.mark_ready(dest_process)
                return

        assert source_process not in self._outputs_by_source
        self._outputs_by_source[source_process] = (value, dest_process)

    def deactivate_process(self, process):
        self._active_processes.remove(process)
        for dest_process, input_guards in self._input_guards_by_dest.items():
            assert dest_process is not process
            for input_guard in input_guards.keys():
                if not input_guard.viable:
                    del input_guards[input_guard]
            if not input_guards:
                del self._input_guards_by_dest[dest_process]
                dest_process.fail_await()
                self.mark_ready(dest_process)

        for source_process, (_, dest_process) in self._outputs_by_source.items():
            assert source_process is not process
            if dest_process is process:
                del self._outputs_by_source[source_process]
                source_process.fail_await()
                self.mark_ready(source_process)

    def mark_unready(self, process):
        assert process not in self._input_guards_by_dest
        assert process not in self._outputs_by_source
        self._ready_processes.remove(process)

    def mark_ready(self, process):
        # Maybe should be private?
        assert process not in self._ready_processes
        assert process not in self._input_guards_by_dest
        assert process not in self._outputs_by_source
        self._ready_processes.add(process)


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
    def ready_processes(self):
        return self._network.ready_processes if self._wired else frozenset()

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

    def mark_unready(self, process):
        assert self._wired
        self._network.mark_unready(process)

    def mark_ready(self, process):
        assert self._wired
        self._network.mark_ready(process)

    def is_ready(self, process):
        return process in self.ready_processes

    def process_runner(self, process):
        return self._runners_by_process[process]

    def deactivate_process(self, process):
        assert self._wired
        self._network.deactivate_process(process)

    def run(self):
        assert self._wired
        while self.active_processes:
            if not self.ready_processes:
                if self._dispatcher.processes_running:
                    continue
                raise DeadlockError('No processes can be run')
            await_ = self._dispatcher.run_one(self.ready_processes)
            if await_ is None:
                continue
            process, await_ = await_
            if isinstance(await_, AwaitInput):
                self._network.await_input(process, await_.guarded_matches)
            else:
                assert isinstance(await_, AwaitOutput)
                self._network.await_output(process, await_.process, await_.value)



#TODO: locks somewhere
