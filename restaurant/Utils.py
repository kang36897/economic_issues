# -*- coding: utf-8 -*-

def compareListIgnoreOrder(left, right):
    if len(left) != len(right):
        return False

    for item in left :
        if item not in right:
            return False

    return True