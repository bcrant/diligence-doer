import pprint


def table_name_fmt(name):
    return name.replace('-', '_')


# def log(var_name, var):
#     print(var_name, '(', len(var), ')', '...')
#     pprint.pprint(var)
#     print()
#     print()
#     return


def log(var_name, var):
    return print('{}{}{}'.format(str(var_name), ': ', var))
