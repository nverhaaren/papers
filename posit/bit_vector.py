class BitVector(object):
    def __init__(self, length):
        self._vector = [False] * length

    @classmethod
    def from_iterable(cls, iterable):
        l = list(iterable)
        ret = cls(len(l))
        ret._vector = l

    def __len__(self):
        return len(self._vector)

    def __getitem__(self, item):
        return self._vector[item]

    def __setitem__(self, key, value):
        self._vector[key] = bool(value)

    def __and__(self, other):
        if not isinstance(other, BitVector):
            return NotImplemented
        assert len(self) == len(other)
        return BitVector(self[i] and other[i] for i in range(len(self)))

    def __or__(self, other):
        if not isinstance(other, BitVector):
            return NotImplemented
        assert len(self) == len(other)
        return BitVector(self[i] or other[i] for i in range(len(self)))

    def __invert__(self):
        return BitVector(not b for b in self._vector)

    def __xor__(self, other):
        if not isinstance(other, BitVector):
            return NotImplemented
        assert len(self) == len(other)
        return BitVector(self[i] ^ other[i] for i in range(len(self)))

    def __nonzero__(self):
        return any(self._vector)

    def __lshift__(self, other):
        assert isinstance(other, int) and other >= 0
        return BitVector(self._vector[other:] + other * [False])

    def __rshift__(self, other):
        assert isinstance(other, int) and other >= 0
        if other == 0:
            return BitVector(self._vector)
        return BitVector(other * [False] + self._vector[:-other])

    def rotate(self, other):
        assert isinstance(other, int)
        if other >= 0:
            return BitVector(self._vector[other:] + self._vector[:other])
        else:
            return BitVector(self._vector[-other:] + self._vector[:-other])
