# coding: utf-8
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
