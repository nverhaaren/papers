from abc import ABCMeta, abstractmethod
from copy import deepcopy

from papers.csp.io_semantics import CommandFailure


class AwaitInput(object):
    def __init__(self, guarded_matches):
        self.guarded_matches = {input_guard: action for input_guard, action in guarded_matches.iteritems() if input_guard.viable}


class AwaitOutput(object):
    def __init__(self, process, value):
        self.process = process
        self.value = deepcopy(value)


class Process(object):
    __metaclass__ = ABCMeta

    def __init__(self, controller):
        super(Process, self).__init__()
        self._controller = controller

        self._callback = None
        self._branch_name = None
        self._input_value = None

        self._awaiting_input = False
        self._awaiting_output = False
        self._failed_await = False

    @property
    def active(self):
        return self._controller.is_active(self)

    @property
    def _branch_value_available(self):
        return self._branch_name is not None

    @property
    def _callback_result_available(self):
        return self._callback is not None

    @property
    def _awaiting(self):
        return self._awaiting_input or self._awaiting_output

    def register_inputs(self, *inputs):
        for input_ in inputs:
            self._controller.add_process_input(self, input_)

    def register_outputs(self, *outputs):
        for output in outputs:
            self._controller.add_process_output(self, output)

    def set_callback_input(self, callback, input_value):
        assert self._awaiting_input
        self._callback = callback
        self._input_value = input_value

    def set_branch_value(self, branch_name, input_value):
        assert self._awaiting_input
        self._branch_name = branch_name
        self._input_value = input_value

    def fail_await(self):
        assert self._awaiting
        self._failed_await = True

    def await_input(self, guarded_matches):
        assert not self._awaiting
        self._awaiting_input = True
        return AwaitInput(guarded_matches)

    def await_output(self, process, value):
        assert not self._awaiting
        self._awaiting_output = True
        return AwaitOutput(process, value)

    def get_input_callback_result(self):
        assert self._awaiting_input
        self._awaiting_input = False
        if self._failed_await:
            raise CommandFailure('Failed awaiting input: {}'.format(self))

        assert self._callback_result_available
        callback, input_value = self._callback, self._input_value
        self._callback = None
        self._input_value = None
        return callback(input_value)

    def get_input_branch_value(self):
        assert self._awaiting_input
        self._awaiting_input = False
        if self._failed_await:
            raise CommandFailure('Failed awaiting input: {}'.format(self))

        assert self._branch_value_available
        ret = (self._branch_name, self._input_value)
        self._branch_name = None
        self._input_value = None
        return ret

    def get_input_either(self):
        assert self._awaiting_input
        self._awaiting_input = False
        if self._failed_await:
            raise CommandFailure('Failed awaiting input: {}'.format(self))
        assert self._callback_result_available ^ self._branch_value_available
        if self._callback_result_available:
            return None, self.get_input_callback_result()
        else:
            return self.get_input_branch_value(), None

    def check_output_success(self):
        assert self._awaiting_output
        self._awaiting_output = False
        if self._failed_await:
            raise CommandFailure('Failed awaiting output: {}'.format(self))

    @property
    def _is_run_ready(self):
        return True

    @abstractmethod
    def _run(self):
        pass

    def run(self):
        assert self._is_run_ready
        return self._run()


class SingleOutputProcess(Process):
    def __init__(self, controller):
        super(SingleOutputProcess, self).__init__(controller)
        self._output_process = None

    def set_output(self, process):
        self._output_process = process
        self.register_outputs(process)

    @property
    def _is_run_ready(self):
        return self._output_process is not None

    @abstractmethod
    def _run(self):
        pass


class SingleInputProcess(Process):
    def __init__(self, controller):
        super(SingleInputProcess, self).__init__(controller)
        self._input_process = None

    def set_input(self, process):
        self._input_process = process
        self.register_inputs(process)

    @property
    def _is_run_ready(self):
        return self._input_process is not None

    @abstractmethod
    def _run(self):
        pass


class SingleInputOutputProcess(Process):
    def __init__(self, controller):
        super(SingleInputOutputProcess, self).__init__(controller)
        self._input_process = None
        self._output_process = None

    def set_input(self, process):
        self._input_process = process
        self.register_inputs(process)

    def set_output(self, process):
        self._output_process = process
        self.register_outputs(process)

    @property
    def _is_run_ready(self):
        return self._input_process is not None and self._output_process is not None

    @abstractmethod
    def _run(self):
        pass
