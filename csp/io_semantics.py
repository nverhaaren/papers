class CommandFailure(Exception):
    pass


class Signal(object):
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
    def __init__(self, source_process, form=None, boolean_result=True):
        self._form = form
        self._source_process = source_process
        # None indicates it is a guard without an input process, so purely conditional
        self._boolean_result = boolean_result

    @classmethod
    def single_match(cls, source_process, form=None, boolean_result=True):
        return {cls(source_process, form, boolean_result): 'the'}

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


class Return(Exception):
    def __init__(self, result=None):
        super(Return, self).__init__(result)
