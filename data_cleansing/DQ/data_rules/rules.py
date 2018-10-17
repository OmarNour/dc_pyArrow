import datetime
import pandas as pd
import hashlib
import itertools


def rules_orchestrate(att_value,id):
    print('rules_orchestrate', att_value)
    if id == 1:

        return rule1(att_value)
    elif id == 2:
        return rule2(att_value)


def rule1(att_value):
    print('rule1', att_value, type(att_value), len(att_value))
    # att_value = None

    if att_value == "":
        print('att_value_is_none')
        return 1
    else:
        print('att_value_is_not_none')
        return 0


def rule2(att_value):
    print('rule2', att_value)
    try:
        float_att_value = float(att_value)
        # print(float_att_value)
        return 0
    except:
        return 1
