import data_cleansing.dc_methods.dc_methods as dc_methods


def rules_orchestrate(rule_id, att_value, kwargs):
    # print('rules_orchestrate', att_value)
    if kwargs:
        kwargs = eval("dict(%s)" % ','.join(kwargs.split()))

    if rule_id == 1:
        return rule1(att_value, kwargs)
    elif rule_id == 2:
        return rule2(att_value, kwargs)
    elif rule_id == 3:
        return rule3(att_value, kwargs)
    elif rule_id == 100:
        return rule100(att_value, kwargs)


def rule1(att_value, kwargs):

    if att_value == "":
        # print('att_value_is_none')
        return 1
    else:
        # print('att_value_is_not_none')
        return 0


def rule2(att_value,kwargs):
    # print('rule2', att_value)
    try:
        float_att_value = float(att_value)
        # print(float_att_value)
        return 0
    except:
        return 1


def rule3(att_value, kwargs):
    # print('rule3', att_value)
    if len(att_value) <= 4:
        return 1
    else:
        return 0

def rule100(att_value, kwargs):
    # print('rule100', att_value)
    # print('kwars_parameters', kwargs)
    # print('first value in Kwargs', kwargs['Firstname'])
    if len(att_value) > 4:
        return 1
    else:
        return 0