#!/usr/bin/env python

import re
import copy

# http://stackoverflow.com/questions/20400818/python-trying-to-deserialize-multiple-json-objects-in-a-file-with-each-object-s
from argparse import ArgumentTypeError

from itertools import islice, chain, izip_longest, count
from more_itertools import peekable
from heapq import heappush, heappop, heapify

# read file in chunks of n lines
from enum import Enum


def lines_per_n(f, n):
    for line in f:
        yield ''.join(chain([line], islice(f, n - 1)))


# round up a value by an increment
def divide_and_ceil(a, b):
    if b == 0:
        raise Exception("Invalid divisor")
    return (a + (b - 1)) / b


def round_up(a, b):
    return divide_and_ceil(a, b) * b


# Allow sorting of names in the way that humans expect.
def convert(text): int(text) if text.isdigit() else text


def alphanum_key(key): [convert(c) for c in re.split('([0-9]+)', key)]


# Stable implementation of a single-threaded priority queue
class PQueue(object):
    def __init__(self):
        self.pq = []                         # list of entries arranged in a heap
        self.counter = peekable(count())     # unique sequence count

    def __deepcopy__(self, memo):
        new_pqueue = PQueue()
        memo[id(self)] = new_pqueue
        new_pqueue.pq = []
        for entry in self.pq:
            # noinspection PyArgumentList
            new_pqueue.pq.append([entry[0], entry[1], copy.deepcopy(entry[2], memo)])

        return new_pqueue

    def push(self, value, priority=0):
        idx = next(self.counter)
        entry = [priority, idx, value]
        heappush(self.pq, entry)

    def pop(self):
        if self.pq:
            priority, _, value = heappop(self.pq)
            return priority, value
        else:
            raise KeyError('pop from an empty priority queue')

    def unsafe_remove(self, value):
        # This method breaks the priority queue ordering.
        for item in self.pq:
            if item[2] is value:
                self.pq.remove(item)
                break

    def safe_remove(self, value):
        self.unsafe_remove(value)
        heapify(self.pq)

    @property
    def unordered_values(self):
        # Get an unordered list of values in the priority queue.
        return [x[2] for x in self.pq]

    def empty(self):
        return not self.pq


# http://stackoverflow.com/questions/23028192/how-to-argparse-a-range-specified-with-1-3-and-return-a-list

def parsenumlist(string):
    m = re.match(r'(\d+(?:\.\d+)?)(?:-(\d+(?:\.\d+)?))?$', string)
    # ^ (or use .split('-'). anyway you like.)
    if not m:
        raise ArgumentTypeError("'" + string + "' is not a float range. Expected forms like '0-5.1' or '2'.")
    start = m.group(1)
    end = m.group(2) or start
    if float(start) > float(end):
        raise ArgumentTypeError("Range start must be smaller or equal to range end.")
    return float(start), float(end)


# http://stackoverflow.com/questions/2612720/how-to-do-bitwise-exclusive-or-of-two-strings-in-python
def sxor(s1, s2):
    # convert strings to a list of character pair tuples
    # go through each tuple, converting them to ASCII code (ord)
    # perform exclusive or on the ASCII code
    # then convert the result back to ASCII (chr)
    # merge the resulting array of characters as a string
    return ''.join(chr(ord(a) ^ ord(b)) for a, b in izip_longest(s1, s2, fillvalue=' '))


def get_enumparser(enum, *exclude):
    # noinspection PyTypeChecker
    def enumparser(string):
        name = string.upper()
        if name not in enum.__members__ or name in [e.name for e in exclude]:
            raise ArgumentTypeError(' '.join([string, 'is not one of: '].extend(enum.__members__)))

        return enum.__members__.get(name)

    return enumparser


# Pretty printing of Enum values
class PEnum(Enum):
    def __str__(self):
        return self.name
