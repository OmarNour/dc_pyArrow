import data_cleansing.dc_methods.dc_methods as dc_methods
import data_cleansing.CONFIG.Config as DNXConfig
from pydrill.client import PyDrill

def rules_orchestrate(rule_id, att_value, RowKey, kwargs):
    # print('rules_orchestrate', att_value)
    if kwargs:
        # ex: Firstname="Sita" Lastname="Sharma" Age=22 Phone=1234567890
        kwargs = eval("dict(%s)" % ','.join(kwargs.split()))

    if rule_id == 1:
        return rule1(att_value, RowKey, kwargs)
    elif rule_id == 2:
        return rule2(att_value, RowKey, kwargs)
    elif rule_id == 3:
        return rule3(att_value, RowKey, kwargs)
    elif rule_id == 100:
        return rule100(att_value, RowKey, kwargs)


def rule1(att_value, RowKey, kwargs):
    # print(RowKey)

    if att_value == "":
        # print('att_value_is_none')
        return 1
    else:
        # print('att_value_is_not_none')
        return 0


def rule2(att_value, RowKey, kwargs):
    # print('rule2', att_value)
    try:
        float_att_value = float(att_value)
        # print(float_att_value)
        return 0
    except:
        return 1


def rule3(att_value, RowKey, kwargs):
    value_length = kwargs['value_length']
    operator = kwargs['operator']

    # print('value_length', value_length)
    # print('operator', operator)
    # print('rule3', len(att_value), operator, value_length)

    if (operator == "=") and len(att_value) == value_length:
        return 1
    elif (operator == ">") and len(att_value) > value_length:
        return 1
    elif (operator == "<") and len(att_value) < value_length:
        return 1
    elif (operator == "<=") and len(att_value) <= value_length:
        return 1
    elif (operator == ">=") and len(att_value) >= value_length:
        return 1
    elif (operator == "<>") and len(att_value) != value_length:
        return 1
    else:
        return 0


def rule100_old(att_value, RowKey, kwargs):

    citizens_cards_src = kwargs['citizens_cards_src']
    citizen_src = kwargs['citizen_src']
    cards_src = kwargs['cards_src']
    # drill_parquet_db_root_path = kwargs['drill_parquet_db_root_path']
    # dnx_db_name = kwargs['dnx_db_name']
    att_410_value = att_value
    where = " where AttributeValue = " + str(att_410_value) + " and AttributeID=410"
    citizens_cards_src = "/bt_current_9898_120"
    full_query = "SELECT RowKey,AttributeValue from dfs.`/opt/parquet_db//DNX//bt_current_9898_120` " + where
    citizens_cards_src_df = dc_methods.read_from_parquet_drill(drill=None,
                                                               full_query=full_query)
    print('drill_citizens_cards_src', len(citizens_cards_src_df.index))
    return 0


def rule100(att_value, RowKey, kwargs):

    citizens_cards_src = kwargs['citizens_cards_src']
    citizen_src = kwargs['citizen_src']
    cards_src = kwargs['cards_src']

    #filters ex : [['AttributeID', [410]], ['RowKey', ['f3b24cdd53c1412d9e849e286386bfcc0b280e07']]]

    att_410_value = att_value
    #
    att_410_rowkeys_filter = [['AttributeID', [410]], ['AttributeValue', [att_410_value]]]
    # att_410_rowkeys_filter = None
    att_410_rowkeys_data = dc_methods.get_attribute_value_by_rowkey(citizens_cards_src, att_410_rowkeys_filter)
    if not att_410_rowkeys_data.empty:
        att_410_rowkeys_data = att_410_rowkeys_data['RowKey'].values.tolist()

    att_420_filter = [['AttributeID', [420]], ['RowKey', att_410_rowkeys_data]]
    att_420_value = dc_methods.get_attribute_value_by_rowkey(citizens_cards_src, att_420_filter)['AttributeValue'].values.tolist()

    citizen_src_110_a630_eq_1 = [['AttributeID', [630]], ['AttributeValue', [str(1)]]]
    citizen_src_110_a630_eq_1_data = dc_methods.get_attribute_value_by_rowkey(citizen_src, citizen_src_110_a630_eq_1)
    citizen_src_110_a630_eq_1_rowkeys = citizen_src_110_a630_eq_1_data['RowKey'].values.tolist()
    citizen_src_110_a630_eq_1_a620_eq_v410 = [['AttributeID', [620]], ['AttributeValue', [att_410_value]], ['RowKey', citizen_src_110_a630_eq_1_rowkeys]]
    citizen_src_110_a630_eq_1_a620_eq_v410_data = dc_methods.get_attribute_value_by_rowkey(citizen_src, citizen_src_110_a630_eq_1_a620_eq_v410)
    count_citizen_src_110_a630_eq_1_a620_eq_v410_data = len(citizen_src_110_a630_eq_1_a620_eq_v410_data.index)

    cards_src_100_a520_eq_1 = [['AttributeID', [520]], ['AttributeValue', [str(1)]]]
    cards_src_100_a520_eq_1_data = dc_methods.get_attribute_value_by_rowkey(cards_src, cards_src_100_a520_eq_1)
    cards_src_100_a520_eq_1_data_rowkeys = cards_src_100_a520_eq_1_data['RowKey'].values.tolist()
    cards_src_100_a520_eq_1_a510_eq_v420_filter = [['AttributeID', [510]], ['AttributeValue', att_420_value], ['RowKey', cards_src_100_a520_eq_1_data_rowkeys]]
    cards_src_100_a520_eq_1_a510_eq_v420 = dc_methods.get_attribute_value_by_rowkey(cards_src, cards_src_100_a520_eq_1_a510_eq_v420_filter)
    count_cards_src_100_a520_eq_1_a510_eq_v420_data = len(cards_src_100_a520_eq_1_a510_eq_v420.index)


    # count_citizen_src_110_a630_eq_1_a620_eq_v410_data = 0
    # count_cards_src_100_a520_eq_1_a510_eq_v420_data = 0
    # print('att_410', att_410_value)
    # print('att_420', att_420_value)
    print('count_citizen_src_110_a630_eq_1_a620_eq_v410_data', count_citizen_src_110_a630_eq_1_a620_eq_v410_data)
    print('count_cards_src_110_a520_eq_1_a510_eq_v420_data', count_cards_src_100_a520_eq_1_a510_eq_v420_data)

    if count_citizen_src_110_a630_eq_1_a620_eq_v410_data >= 2 and count_cards_src_100_a520_eq_1_a510_eq_v420_data >= 2:
        return 1
    else:
        return 0