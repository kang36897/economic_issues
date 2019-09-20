# -*- coding: utf-8 -*-

class Signal:
    def __init__(self, name='dummy', min_lots=0.01, step=0.01, max_lots=5):
        self.min_lots = min_lots
        self.max_lots = max_lots
        self.step = step
        pass

    def __len__(self):
        return int((self.max_lots - self.min_lots) / self.step) + 1


    def __str__(self):
        pass