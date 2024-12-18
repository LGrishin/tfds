def mergeVectors(first, second):
    if len(first) != len(second):
        raise ValueError("Vector clocks have different node counts")

    
    result = [0 for _ in range(len(first))]
    
    for i in range(len(first)):
        result[i] = max(first[i], second[i])
    
    return result

def lessEqList(first, second):
    assert(len(first) == len(second))
    return all(first[i] <= second[i] for i in range(len(first)))

def independentLists(self, other):
    return not self.lessEqList(other) and not other.lessEqList(self)

