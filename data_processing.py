# coding: utf-8
import pandas as pd, numpy as np
import sys

def flip_dict(dict_to_flip):
    '''
    flip a dict.

    Parameters
    ----------
    dict_to_flip : dict

    Examples
    --------
    >>> flip_dict({'a': 2, 'b': 3, 'c': 4})
    {2: 'a', 3: 'b', 4: 'c'}

    Notes that duplicated data will be automaticly dropped
    >>> flip_dict({'a': 3, 'b': 3, 'c': 4})
    {3: 'a', 4: 'c'}

    See Also
    --------
    flip_dict_full
    '''
    return dict([i[::-1] for i in dict_to_flip.items()])


def split_wrd(string,sep,rep=None):
    '''
    split a string with given condition

    Parameters
    ----------
    string : str
        string to process
    sep : str or iterable
        for each item in sep, call string.split() method
    rep : None or str or iterable, default None
        - None : return a list containing all items seprated by sep
        - str : return a string with all items in sep replaced by rep
        - iterable : return a string with all sep[i] replaced by rep[i]
    '''
    if type(sep) == str:
        sep = [sep]
    if rep == None:
        string = [string]
        for j in sep:
            ts = []
            for i in string:
                ts += i.split(j)
            string = ts
        return [i for i in string if i]
    else:
        if type(rep)==str:
            for i in sep:
                string = rep.join(string.split(i))
            return string
        else:
            for i,j in zip(sep,rep):
                string = j.join(string.split(i))
            return string
    
def flip_dict_full(dict_to_flip):
    '''
    flip a dict with no data loss
    
    Parameters
    ----------
    dict_to_flip : dict

    See Also
    --------
    flit_dict

    Examples
    --------
    >>> flip_dict_full({'a': 1, 'b': 2, 'c': 3})
    {1: ['a'], 2: ['b'], 3: ['c']}
    >>> flip_dict_full({'a': 3, 'b': 3, 'c': 4})
    {3: ['a', 'b'], 4: ['c']}
    '''
    t = [i[::-1] for i in dict_to_flip.items()]
    tp = dict()
    for i in t:
        if i[0] in tp:
            tp[i[0]] = tp[i[0]] + [i[1]] if type(tp[i[0]])==list else [tp[i[0]]] + [i[1]]
        else: tp[i[0]] = [i[1]]
    return tp


def transfer_datatype(data):
    '''
    call eval(data) if data is valid
    '''
    if type(data) == list or not data: return data
    else:
        try: return eval(data)
        except: return data


def stacked_series_flatten(ser):
    '''
    flatten a series containing 1-D list-like items

    Parameters
    ----------
    ser : Series

    Returns
    -------
    sser : Series
        series item with all items flatten

    Example
    -------
    >>> import pandas as pd
    >>> ser = pd.Series([[1,2,3],[4,5,6],[7,8,9]], name='array')
    0    [1, 2, 3]
    1    [4, 5, 6]
    2    [7, 8, 9]
    Name: array, dtype: object

    >>> stacked_series_flatten(ser, name='array'))
    0    1
    1    4
    2    7
    3    2
    4    5
    5    8
    6    3
    7    6
    8    9
    Name: array, dtype: int64
    '''
    ser = ser.map(transfer_datatype)
    sser = ser[ser.map(type) != list]
    lser = ser[ser.map(type) == list]
    maxn = lser.map(len).max()
    if np.isnan(maxn): return sser
    for i in range(int(maxn)):
        sser = sser.append(lser.map(lambda x: x[i] if len(x)>i else None),ignore_index=True)
    return sser.dropna()

    
def stacked_series_map(ser,mapfunc='count',label=None,ascending=False):
    '''
    map every item in a 1-D stacked series
    
    Parameters
    ----------
    ser : Series or DataFrame
    mapfunc : str or function, default 'count'
        - 'count' : count every element in series
        - 'sum' : sum index of every element in series
        - function
    label : str, default None
        ignored if type of ser is Series.
        Do stacked_series_map on DataFrame's label column
    ascending : bool, default False

    See Also
    --------
    stacked_series_flatten

    Examples
    --------
    >>> t = pd.Series([1,[4,5,6],[6,5],[3,6],[]],name='array')
    >>> t
    0            1
    1    [4, 5, 6]
    2       [6, 5]
    3       [3, 6]
    4           []
    Name: array, dtype: object
    >>> stacked_series_map(t)
           count
    array       
    6.0        3
    5.0        2
    1.0        1
    3.0        1
    4.0        1
    >>> stacked_series_map(t,'sum')
           sum
    array     
    6.0     18
    5.0     11
    3.0      3
    4.0      1
    1.0      0

    mapfunc can also be a function applying to a Groupby object.
    >>> stacked_series_map(t,lambda x: x.max())
           result
    array        
    6.0       9.0
    5.0       6.0
    3.0       3.0
    4.0       1.0
    1.0       0.0
    '''
    ser.name = ser.name if ser.name else 'ser'
    df = stacked_series_flatten(ser).reset_index().rename_axis({'index':'result'}, axis=1)
    if type(mapfunc) == str:
        if mapfunc not in ['count','sum']: raise ValueError('mapfunc param not supported, please input count or sum')
        elif mapfunc == 'sum':
            dfp = df.groupby(ser.name).sum().rename_axis({'result':'sum'},axis=1).sort_values('sum',ascending=ascending)
        elif mapfunc == 'count':
            dfp = df.groupby(ser.name).count().rename_axis({'result':'count'},axis=1).sort_values('count',ascending=ascending)
    else:
        dfp = df.groupby(ser.name).apply(mapfunc).sort_values('result',ascending=ascending)
        del dfp[ser.name]
    if type(label) == type(None): return dfp
    else:
        if type(label) in [dict,pd.Series]: return dfp.rename_axis(label)
        elif type(label) == pd.DataFrame:
            if label.shape[1]!=2:
                raise ValueError('Length of DataFrame columns must be 2; %i detected'%label.shape[1])
            return dfp.rename_axis(dict(label.values))

def _in_list(x,ls,how='find'):
    '''
    check if x match rules in ls
    parameters:
        x : str-like variable
        ls : iterable containing rules
        how : str
            - 'find' : call x.find(rule)
            - 're' : call re.find(rule,x)
            - 'fullmatch' : x == rule
    '''
    x = str(x)
    if how == 'find':
        for item in ls:
            if x.find(item) >= 0: return True
    elif how == 're':
        for item in ls:
            if re.findall(item,x): return True
    elif how == 'fullmatch':
        for item in ls:
            item = str(item).strip()
            if item == x: return True
    return False
   
 
def df_filter(df, column, condition, how='find', include=True):
    '''
    find all records satisfying given condition

    Parameters
    ----------
    df : DataFrame
    column : str
    condition : iterable or scalar
        list of filter rules
    how : str 
        - 'find' : call x.find(rule)
        - 're' : call re.find(rule,x)
        - 'fullmatch' : x == rule
    include : bool
        True if you want to include all found data in df
        False if you want to exclude all found data in df
    '''
    if type(column)==str:
        condition = [condition] if type(condition) in (str,int,float) else list(condition)
        df.dropna(subset=[column],inplace=True)
        if include:
            df = df[df[column].map(lambda x: _in_list(x,condition,how=how))]
        else:
            df = df[df[column].map(lambda x: not _in_list(x,condition,how=how))]
        return df
    else:
        if include:
            dfs = pd.concat([df_filter(df,c,condition,how,include) for c in column],ignore_index=True)
            return dfs
        else:
            dfs = df.copy()
            for c in column:
                dfs = df_filter(dfs,c,condition,how,include)
            return dfs


def _space_fill(string,length,align='c'):
    string = str(string)
    strlen = (len(string.encode('utf-8')) + len(string))//2
    if length < strlen: length = strlen
    if align[0] == 'c':
        leftspc = (length-strlen)//2
        rightspc = length-strlen-leftspc
    elif align[0] == 'l':
        leftspc = 0
        rightspc = length-strlen
    elif align[0] == 'r':
        rightspc = 0
        leftspc = length-strlen
    else:
        raise ValueError('align not in [`c`,`l`,`r`]')
    return ' '*leftspc+string+' '*rightspc


    
def df_format_print(df,file=sys.stdout,index=False,align='c'):
    lengths = []
    if index: df = df.reset_index()
    collen = len(df.columns)
    lengths = df.columns.map(lambda x: len(str(x).encode('utf-8')))
    dfshap = df.copy()
    for c in dfshap.columns:
        dfshap[c] = dfshap[c].map(lambda x: len(str(x).encode('utf-8')))
    lengths = np.max([lengths,dfshap.max()],axis=0)+2
    print(' '.join(['-'*i for i in lengths]),file=file)
    dcfl = [_space_fill(df.columns[i],length=lengths[i],align=align) for i in range(collen)]
    print(' '.join(dcfl),file=file)
    print(' '.join(['-'*i for i in lengths]),file=file)
    ddfl = [df.ix[:,c].map(lambda x: _space_fill(x,lengths[c],align)) for c in range(collen)]
    ddfl = '\n\n'.join([' '.join(i) for i in pd.DataFrame(ddfl).T.values])
    print(ddfl,file=file)
    print('-'*(sum(lengths)+collen-1),file=file)
    
