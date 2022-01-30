import csv
import math

class PrettyCsvDiff:
    def __init__(self, path, pk):
        self._header = None
        self._maxlen = None
        self._pks = pk

        self._data_a = self._read(path[0])
        self._data_b = self._read(path[1])

    def _read(self, path):
        with open(path, encoding='utf-8') as fp:
            reader = csv.reader(fp)

            if self._header is None:
                self._header = next(reader)
                self._maxlen = list(map(len, self._header))
                self._pks = [int(pk) if pk.isdecimal() else self._header.index(pk) for pk in self._pks]
            else:
                next(reader)

            data = []
            for row in reader:
                data.append(row)
                self._maxlen = [max(pair) for pair in zip(map(len, row), self._maxlen)]

        data.sort(key=self._get_pk)
        return data

    def _get_pk(self, row):
        return [int(row[k]) if row[k].isdecimal() else row[k] for k in self._pks]

    def _formatted(self, prefix, row, diff=None):
        BOLD = '\x1b[1m'
        RED = '\x1b[41m'
        GREEN = '\x1b[42m'
        RESET = '\x1b[0m'

        def colorize(k):
            sgr = ''
            if prefix in ('<', '>') and (not diff or diff[k]):
                sgr += RED if prefix == '<' else GREEN
            if k in self._pks:
                sgr += BOLD
            padding = ' ' * (self._maxlen[k] - len(row[k]))
            return sgr + row[k] + padding + (RESET if sgr else '')

        return (prefix,) + tuple(colorize(k) for k in range(len(row)))


    def do(self):
        yield self._formatted(' ', self._header)

        i = 0
        j = 0
        previous = None

        while i < len(self._data_a) or j < len(self._data_b):
            pk_a = self._get_pk(self._data_a[i]) if i < len(self._data_a) else [math.inf]
            pk_b = self._get_pk(self._data_b[j]) if j < len(self._data_b) else [math.inf]

            next_a = pk_a < pk_b
            next_b = pk_a > pk_b
            next_ab = pk_a == pk_b

            diff = [a != b for a, b in zip(self._data_a[i], self._data_b[j])] if next_ab else None
            diff_ab = diff and any(diff)

            current = (next_a, next_b)
            if (next_a or next_b) and previous != current or diff_ab:
                yield self._formatted(' ', ['-' * n for n in self._maxlen])
                previous = current

            if next_a or next_ab:
                if next_a or diff_ab:
                    yield self._formatted('<', self._data_a[i], diff)
                i += 1

            if next_b or next_ab:
                if next_b or diff_ab:
                    yield self._formatted('>', self._data_b[j], diff)
                j += 1
