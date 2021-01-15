import timeit
import random
import doctest
import cpuinfo
import platform
import datetime
import numpy as np
import pandas as pd

print('python_version:', cpuinfo.get_cpu_info()['python_version'])
print('procesador:', cpuinfo.get_cpu_info()['brand_raw'], cpuinfo.get_cpu_info()['bits'], 'bits')
print('S.O.:', platform.system())


def print_time(func, data_name, num_loop):
    setup = 'from __main__ import'
    time = timeit.timeit('%s(%s)' % (func, data_name), setup="%s %s, %s" % (setup, func, data_name), number=num_loop)

    if time > 10:
        print('Time of %s:' % func, '\t', str(np.timedelta64(datetime.timedelta(seconds=time), 's')))
    elif time > 0.01:
        print('Time of %s:' % func, '\t', str(np.timedelta64(datetime.timedelta(seconds=time), 'ms')))
    else:
        print('Time of %s:' % func, '\t', str(np.timedelta64(datetime.timedelta(seconds=time), 'ns')))


def square(value):
    """Return the square of x.

    >>> square(2)
    4
    >>> square(-2)
    4
    >>> square(None)
    Traceback (most recent call last):
    ValueError: unsupported operand wirth NoneType
    """

    try:
        return value * value
    except TypeError:
        raise ValueError('unsupported operand wirth NoneType')
    except Exception as error:
        raise ValueError(error)


def group_and_sum(df):
    """Return the square of x.
    >>> df = pd.DataFrame({'city': ['London', 'London'], 'sales': [10, 20]})
    >>> group_and_sum(df)
         city  city_total_sales
    0  London                30
    """

    try:
        city_sales = df.groupby('city')['sales'].apply(sum).rename('city_total_sales').reset_index()
        return city_sales
    except KeyError:
        raise KeyError("city")
    except Exception as error:
        raise ValueError(error)


def df_count_col_by_index_use_value_count(df):
    return df['city'].value_counts().sort_index()


def df_count_col_by_index_use_groupby_and_count(df):
    return df.groupby('city')['city'].count().sort_index()


def df_count_col_by_index_use_groupby_and_size(df):
    return df.groupby('city')['city'].size().sort_index()


def group_and_count_transform(df):
    return df.groupby('city')['city'].transform('count').rename('city_count').reset_index()


def group_and_count_transform2(df):
    return df['city'].groupby(df['city']).transform('count').rename('city_count').reset_index()


def group_and_sum_apply(df):
    return df.groupby('city')['sales'].apply(sum).rename('city_total_sales').reset_index()


def group_and_sum_transform(df):
    return df.groupby('city')['sales'].transform(sum).rename('city_total_sales').reset_index()


if __name__ == '__main__':

    print(doctest.testmod(), '\n')

    multiplicator = 100000
    data = pd.DataFrame({
        'restaurant_id': [101, 102, 103, 104, 105, 106, 107] * multiplicator,
        'address': ['A', 'B', 'C', 'D', 'E', 'F', 'G'] * multiplicator,
        'city': ['London', 'Madrid', 'London', 'Oxford', 'Oxford', 'Durham', 'Durham'] * multiplicator,
        'sales': [10, 500, 48, 12, 21, 22, 14] * multiplicator
    })
    number = 1000

    print('Size of data: %s, num of loop: %s' %(data.shape[0], number))
    for x in [df_count_col_by_index_use_value_count,
              df_count_col_by_index_use_groupby_and_count,
              df_count_col_by_index_use_groupby_and_size]:
        # Results
        # print(x(data))

        # Time of funtions
        print_time(x.__name__, 'data', number)
