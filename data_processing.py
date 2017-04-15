# coding: utf-8
import pandas as pd, numpy as np

def flip_dict(dict_to_flip):
    return dict([i[::-1] for i in dict_to_flip.items()])
def split_wrd(string,sep,rep=None):
    if type(sep)!=list:
        sep = [sep]
    if rep == None:
        type(sep)==list
        string = [string]
        for j in sep:
            ts = []
            for i in string:
                ts += i.split(j)
            string = ts
        return string
    else:
        if type(rep)==str:
            for i in sep:
                string = rep.join(string.split(i))
            return string
        elif type(rep)==list:
            for i,j in zip(sep,rep):
                string = j.join(string.split(i))
            return string
    
def flip_dict_full(dict_to_flip):
    t = [i[::-1] for i in dict_to_flip.items()]
    tp = dict()
    for i in t:
        if i[0] in tp:
            tp[i[0]] = tp[i[0]] + [i[1]] if type(tp[i[0]])==list else [tp[i[0]]] + [i[1]]
        else: tp[i[0]] = [i[1]]
    return tp
def transfer_datatype(data):
    if type(data) == list or not data: return data
    else:
        try: return eval(data)
        except: return data
        
def stacked_series_flatten(ser):
    ser = ser.map(transfer_datatype)
    sser = ser[ser.map(type) != list]
    lser = ser[ser.map(type) == list]
    maxn = lser.map(len).max()
    if np.isnan(maxn): return sser
    for i in range(int(maxn)):
        sser = sser.append(lser.map(lambda x: x[i] if len(x)>i else None),ignore_index=True)
    return sser.dropna()
def stacked_series_map(ser,mapfunc='count',label=None,ascending=False):
    ser.name = ser.name if ser.name else 'ser'
    df = pd.DataFrame(stacked_series_flatten(ser),columns=[ser.name])
    df['result'] = 1
    if type(mapfunc) == str:
        if mapfunc not in ['count','sum']: raise ValueError('mapfunc param not supported, please input count or sum')
        elif mapfunc == 'sum':
            dfp = df.groupby(ser.name).sum().rename_axis({'result':'sum'},axis=1).sort_values('sum',ascending=ascending)
        elif mapfunc == 'count':
            dfp = df.groupby(ser.name).count().rename_axis({'result':'count'},axis=1).sort_values('count',ascending=ascending)
    else:
        dfp = df.groupby(ser.name).map(mapfunc).sort_values('result',ascending=ascending)
    if type(label) == type(None): return dfp
    else:
        if type(label) in [dict,pd.Series]: return dfp.rename_axis(label)
        elif type(label) == pd.DataFrame:
            if label.shape[1]!=2:
                raise ValueError('Length of DataFrame columns must be 2; %i detected'%label.shape[1])
            return dfp.rename_axis(dict(label.values))
        
