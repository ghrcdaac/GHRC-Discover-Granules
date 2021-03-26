# Just for testing
if __name__ == "__main__":
    test_dict_1 = {
        'dict_1': {'key_a': "value_a", 'key_b': 'value_b'},
        'dict_2': {'key_a': "value_a", 'key_b': 'value_b'},
        'dict_3': {'key_a': "value_a", 'key_b': 'value_b'}
    }

    test_dict_2 = {
        'dict_4': {'key_a': "value_a", 'key_b': 'value_b'},
        'dict_5': {'key_a': "value_a", 'key_b': 'value_b'},
        'dict_6': {'key_a': "value_a", 'key_b': 'value_b'}
    }

    test_dict_1['dict_4'] = {}
    test_dict_1['dict_4']['key_a'] = 'value_a'
    test_dict_1['dict_4']['key_b'] = 'value_b'

    temp = []
    for i, k in enumerate(test_dict_1.items()):
        print(f"{i},{k}")
        # temp.append(k)
        # for k2, v2 in v.items():
        #     temp.append(v2)

    print(','.join(temp))
    pass
