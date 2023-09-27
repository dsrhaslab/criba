class SegmentTree:
    def __init__(self, n, list):
        self.list = [(None,None)] * (4 * n + 5)
        self.__build__(list, 0, 0, n - 1)
        self.n = n

    def minNone(self, a, b):
        if a is None:
            return b
        if b is None:
            return a

        return min(a,b)

    def maxNone(self, a, b):
        if a is None:
            return b
        if b is None:
            return a

        return max(a,b)

    def __build__(self, list, idx, l, r):
        if l == r:
            self.list[idx] = (list[l], list[l])
            return


        mid = (l + r) // 2
        self.__build__(list, 2 * idx + 1, l, mid)
        self.__build__(list, 2 * idx + 2, mid + 1, r)

        a = self.list[2 * idx + 1]
        b = self.list[2 * idx + 2]

        self.list[idx] = (self.minNone(a[0],b[0]), self.maxNone(a[1],b[1]))

    def maxInRange(self, l, r):
        return self.__max__(0, 0, self.n - 1, l, r)

    def minInRange(self, l, r):
        return self.__min__(0, 0, self.n - 1, l, r)


    def __max__(self, idx, rs, re, qs, qe):
        if re < qs or rs > qe:
            return None

        if rs == re:
            return self.list[idx][1]

        if rs >= qs and re <= qe:
            return self.list[idx][1]

        mid = (rs + re) // 2
        a = self.__max__(2 * idx + 1, rs, mid, qs, qe)
        b = self.__max__(2 * idx + 2, mid + 1, re, qs, qe)

        return self.maxNone(a,b)


    def __min__(self, idx, rs, re, qs, qe):
        if re < qs or rs > qe:
            return None

        if rs == re:
            return self.list[idx][0]

        if rs >= qs and re <= qe:
            return self.list[idx][0]

        mid = (rs + re) // 2
        a = self.__min__(2 * idx + 1, rs, mid, qs, qe)
        b = self.__min__(2 * idx + 2, mid + 1, re, qs, qe)

        return self.minNone(a,b)

    def update(self, i, val):
        self.__update__(i, val, 0, 0, self.n - 1)

    def __update__(self, i, val, idx, rs, re):

        if i < rs or i > re:
            return

        if rs == re:
            self.list[idx] = (val, val)
            return

        mid = (rs + re) // 2
        self.__update__(i, val, 2 * idx + 1, rs, mid)
        self.__update__(i, val, 2 * idx + 2, mid + 1, re)
        a = self.list[2 * idx + 1]
        b = self.list[2 * idx + 2]
        c = self.minNone(a[0], b[0])
        d = self.maxNone(a[1], b[1])
        self.list[idx] = (c,d)


    def __str__(self):
        return str(self.n) + "\n" + str(self.list)