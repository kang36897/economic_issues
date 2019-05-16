# -*- coding: utf-8 -*-

class Sieve:

    def __init__(self, drawback_ratio=None, exp_return_ratio=None, sharp_ratio=None, pl_ratio=None, max_active_num=None):
        self.drawback_ratio = drawback_ratio
        self.exp_return_ratio = exp_return_ratio
        self.sharp_ratio = sharp_ratio
        self.pl_ratio = pl_ratio
        self.max_active_num = max_active_num

    def filter(self, row):
        flag = True

        if self.drawback_ratio is None \
                and self.exp_return_ratio is None \
                and self.sharp_ratio is None \
                and self.pl_ratio is None\
                and self.max_active_num is None:
            return flag

        if self.drawback_ratio is not None:
            flag = (row[u'drawback%'] <= self.drawback_ratio) and flag

        if self.exp_return_ratio is not None:
            flag = (row[u'exp_profit%'] >= self.exp_return_ratio) and flag

        if self.sharp_ratio is not None:
            flag = (row[u'sharp%'] >= self.sharp_ratio) and flag

        if self.pl_ratio is not None:
            flag = (row[u'pl%'] >= self.pl_ratio) and flag

        if self.max_active_num is not None and u'active_num' in row.index:
            flag = (row[u'active_num'] <= self.max_active_num) and flag
        return flag



