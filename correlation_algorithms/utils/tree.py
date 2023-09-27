class Tree:
    def __init__(self, node = "/"):
        self.node = node
        self.explored = False
        self.subtrees = {}
        self.start = -1
        self.end = -1

    def size(self):
        sz = 1
        for s in self.subtrees:
            sz += self.subtrees[s].size()

        return sz

    def getTree(self, fileSplit, n):
        if n == 0:
            return self

        return self.subtrees[fileSplit[0]].getTree(fileSplit[1:], n - 1)

    def add(self, fileSplit, n):
        if n == 0:
            return
        s = fileSplit[0]
        if s in self.subtrees:
            self.subtrees[s].add(fileSplit[1:], n - 1)
        else:
            tree = Tree(node=s)
            tree.add(fileSplit[1:], n - 1)
            self.subtrees[s] = tree

    def numerate(self, n):
        self.start = n
        n += 1

        for s in self.subtrees:
            n = self.subtrees[s].numerate(n)
            n += 1
        self.end = n - 1
        return n - 1

    def __str__(self):
        res = f"Node: {self.node} Start: {self.start} End: {self.end}\n"
        for s in self.subtrees:
            res += str(self.subtrees[s])
        return res

def splitFile(file):
    s = file.split("/")
    return s[1:]

def buildTree(list):
    t = Tree()
    for f in list:
        l = splitFile(f)
        t.add(l, len(l))

    t.numerate(0)
    return t