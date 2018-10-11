import datetime
import pandas as pd
import hashlib
import itertools


def rules_orchestrate(att_value,id):
    if id == 1:
        # print('data rule 1 on', att_value)
        return rule1(att_value)
    elif id == 2:
        return rule2(att_value)


def rule1(att_value):
    if att_value is None:
        return 1
    else:
        return 0


def rule2(att_value):
    try:
        float_att_value = float(att_value)
        return 0
    except:
        return 1
