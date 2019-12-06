class CommandFailure(Exception):
    pass


class NTuple(object):
    def __init__(self, length):
        assert isinstance(length, int)
        assert length >= 0
        self._length = length

    @property
    def length(self):
        return self._length


class InputGuard(object):
    def __init__(self, boolean_result, source_process, form):
        self._boolean_result = boolean_result
        # None indicates it is a guard without an input process, so purely conditional
        self._source_process = source_process
        self._form = form

    @property
    def viable(self):
        return self._boolean_result and self._source_process.active

    @property
    def source_process(self):
        return self._source_process

    def matches(self, source_process, value):
        if not self.viable:
            return False

        if source_process is not self._source_process:
            return False

        if self._form is None:
            return True
        if isinstance(self._form, NTuple):
            return isinstance(value, tuple) and len(value) == self._form.length
        return isinstance(value, self._form)


