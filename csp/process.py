from abc import ABCMeta, abstractmethod
from copy import deepcopy

from papers.csp.io_semantics import CommandFailure


class Await(object):
    @property
    def origin_process(self):
        raise NotImplementedError()

    def get_sending_value(self):
        raise NotImplementedError()


class AwaitInit(Await):
    def __init__(self, process):
        self.process = process

    @property
    def origin_process(self):
        return self.process

    def get_sending_value(self):
        return None


class AwaitInput(Await):
    CALLBACK_RESULT = 'CALLBACK_RESULT'
    BRANCH_VALUE = 'BRANCH_VALUE'
    EITHER = 'EITHER'

    def __init__(self, dest_process, guarded_matches, result_format=EITHER):
        self.dest_process = dest_process
        self.guarded_matches = {input_guard: action for input_guard, action in guarded_matches.iteritems() if input_guard.viable}
        self.result_format = result_format

    @property
    def origin_process(self):
        return self.dest_process

    def get_sending_value(self):
        if self.result_format == self.CALLBACK_RESULT:
            return self.origin_process.get_input_callback_result()
        if self.result_format == self.BRANCH_VALUE:
            return self.origin_process.get_input_branch_value()
        if self.result_format == self.EITHER:
            return self.origin_process.get_input_either()
        raise TypeError('Unknown result_format: {}'.format(self.result_format))


class AwaitOutput(Await):
    def __init__(self, source_process, dest_process, value):
        self.source_process = source_process
        self.dest_process = dest_process
        self.value = deepcopy(value)

    @property
    def origin_process(self):
        return self.source_process

    def get_sending_value(self):
        self.origin_process.output_done()
        return None


class Process(object):
    __metaclass__ = ABCMeta

    def __init__(self, controller):
        super(Process, self).__init__()
        self._controller = controller

        self._callback = None
        self._branch_name = None
        self._input_value = None

        self._running = False
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
        assert not self._callback_result_available
        self._callback = callback
        self._input_value = input_value

    def set_branch_value(self, branch_name, input_value):
        assert self._awaiting_input
        assert not self._branch_value_available
        self._branch_name = branch_name
        self._input_value = input_value

    def fail_await(self):
        assert self._awaiting
        self._failed_await = True

    def await_input(self, guarded_matches, result_format=AwaitInput.EITHER):
        assert not self._awaiting
        self._awaiting_input = True
        return AwaitInput(self, guarded_matches, result_format)

    def await_output(self, process, value):
        assert not self._awaiting
        self._awaiting_output = True
        return AwaitOutput(self, process, value)

    def get_input_callback_result(self):
        assert self._awaiting_input
        assert not self._failed_await
        self._awaiting_input = False

        assert self._callback_result_available
        callback, input_value = self._callback, self._input_value
        self._callback = None
        self._input_value = None
        return callback(input_value)

    def get_input_branch_value(self):
        assert self._awaiting_input
        assert not self._failed_await
        self._awaiting_input = False

        assert self._branch_value_available
        ret = (self._branch_name, self._input_value)
        self._branch_name = None
        self._input_value = None
        return ret

    def get_input_either(self):
        assert self._callback_result_available ^ self._branch_value_available
        if self._callback_result_available:
            return None, self.get_input_callback_result()
        else:
            return self.get_input_branch_value()

    def output_done(self):
        assert self._awaiting_output
        assert not self._failed_await
        self._awaiting_output = False

    def check_failed_await(self):
        if not self._running:
            self._running = True
            return False

        assert self._awaiting
        if not self._failed_await:
            return False

        self._failed_await = False
        self._awaiting_input = False
        self._awaiting_output = False
        return True

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

    @property
    def output_process(self):
        return self._output_process

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

    @property
    def input_process(self):
        return self._input_process

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

    @property
    def input_process(self):
        return self._input_process

    @property
    def output_process(self):
        return self._output_process

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


class SimpleAsyncWorkerProcess(Process):
    def __init__(self, controller):
        super(SimpleAsyncWorkerProcess, self).__init__(controller)
        self._caller_process = None

    @property
    def caller_process(self):
        return self._caller_process

    def set_caller(self, process):
        self._caller_process = process
        self.register_inputs(process)
        self.register_outputs(process)

    @property
    def _is_run_ready(self):
        return self._caller_process is not None

    @abstractmethod
    def _run(self):
        pass


class AsyncCallerProcess(Process):
    def __init__(self, controller):
        super(AsyncCallerProcess, self).__init__(controller)
        self._workers = {}

    def get_worker(self, key):
        return self._workers.get(key)

    def set_worker(self, key, process):
        self._workers[key] = process
        self.register_inputs(process)
        self.register_outputs(process)

    @property
    def _is_run_ready(self):
        return bool(self._workers)

    @abstractmethod
    def _run(self):
        pass
