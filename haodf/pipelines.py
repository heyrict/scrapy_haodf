# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pandas as pd, numpy as np
from haodf.items import *
import os
from dateutil import parser
from scrapy.exceptions import DropItem
from data_processing import *

class SaveCSVPipeline(object):
    def __init__(self):
        dirls = os.listdir()
        if 'provfile.csv' in dirls:
            self.provdf = pd.read_csv('provfile.csv')
        else:
            self.provdf = pd.DataFrame()
        if 'hospfile.csv' in dirls:
            self.hospdf = pd.read_csv('hospfile.csv')
        else:
            self.hospdf = pd.DataFrame()
        if 'sectfile.csv' in dirls:
            self.sectdf = pd.read_csv('sectfile.csv')
        else:
            self.sectdf = pd.DataFrame()
        if 'doctfile.csv' in dirls:
            self.doctdf = pd.read_csv('doctfile.csv')
        else:
            self.doctdf = pd.DataFrame()
        if 'patfile.csv' in dirls:
            self.patdf = pd.read_csv('patfile.csv')
        else:
            self.patdf = pd.DataFrame()
        if 'lllfile.csv' in dirls:
            self.llldf = pd.read_csv('lllfile.csv')
        else:
            self.llldf = pd.DataFrame()
        if 'lllsevfile.csv' in dirls:
            self.lllsevdf = pd.read_csv('lllsevfile.csv')
        else:
            self.lllsevdf = pd.DataFrame()
        

        self.codesdict = {}
        self.ilns_dict =self.get_illness()
        self.lllsevtags_dict = {}

    def serialize_item(self,item):
        try:
            for tup in list(item):
                if type(item[tup])==float and np.isnan(item[tup]): continue
                if type(item[tup])!=list: item[tup] = str(item[tup]).strip()
                if item[tup] == '暂无': item[tup] = None; continue
                if not item[tup]: continue
                # doct_ix
                if tup == 'doct_ix': continue
                # pat_time
                if tup == 'pat_time':
                    try: item[tup] = parser.parse(item[tup].split('：')[-1]).strftime('%Y%m%d')
                    except: item[tup] = np.nan
                    continue
                # pat_cost
                if tup == 'pat_cost': item[tup] = float(item[tup][:-1]); continue
                # doct_tot_sat_(eff,att)
                if tup in ('doct_tot_sat_eff','doct_tot_sat_att'):
                    item[tup] = float(split_wrd(item[tup],'%',''))
                    continue
               

                # else
                if type(item[tup]==str):
                    try: t = float(item[tup])
                    except: 
                        try:item[tup] = float(item[tup][:-1])
                        except: pass
                if type(item[tup])==list and len(item[tup])==1: item[tup]=item[tup][0]
                if tup.split('_')[-1] not in ['status','att','eff','reservation','aim','reason','ilns','class']: continue
                if tup not in self.codesdict:
                    if '%s.csv'%tup in os.listdir(): self.codesdict[tup] = pd.read_csv('%s.csv'%tup)
                    else: self.codesdict[tup] = pd.DataFrame(columns=['code','name'])


                # pat_ilns
                if tup == 'pat_ilns':
                    if item[tup] not in self.codesdict[tup]['name'].values:
                        self.codesdict[tup] = self.codesdict[tup].append({'code':-len(self.codesdict[tup]),'name':item[tup]},ignore_index=True)
                        self.codesdict[tup].to_csv('%s.csv'%tup,index=False)
                        self.ilns_dict = self.get_illness(self.codesdict[tup])
                    # preprocessing
                    # item[tup] = self.ilns_dict[item[tup]]
                    continue

                # others
                if type(item[tup])==float and np.isnan(item[tup]): pass
                elif type(item[tup])==type(None): pass
                elif type(item[tup])==str: item[tup] = split_wrd(str(item[tup]),list('，、；,; '))
                for t in item[tup]:
                    if not t: item[tup].remove(t);continue
                    if t not in self.codesdict[tup]['name'].values:
                        self.codesdict[tup] = self.codesdict[tup].append({'code':len(self.codesdict[tup]),'name':t},ignore_index=True)
                        self.codesdict[tup].to_csv('%s.csv'%tup, index=False)
                try:
                    item[tup] = [self.codesdict[tup][self.codesdict[tup]['name']==i]['code'].iloc[0] for i in item[tup]]
                except Exception as e:
                    print('%s\n%s'%(item[tup],e))

                if type(item[tup])==list and len(item[tup])==1: item[tup]=item[tup][0]
        except Exception as e:
            print('Error on processing %s:'%(tup))
            raise e
        return item

    def process_item(self, item, spider):
        item = self.serialize_item(item)

        if type(item)==type(ProvItem()):
            self.provdf = self.provdf.append(pd.Series(dict(item)),ignore_index=True)
            self.provdf.to_csv('provfile.csv',index=False)

        if type(item)==type(HospItem()):
            self.hospdf = self.hospdf.append(pd.Series(dict(item)),ignore_index=True)
            self.hospdf.to_csv('hospfile.csv',index=False)

        if type(item)==type(SectionItem()):
            self.sectdf = self.sectdf.append(pd.Series(dict(item)),ignore_index=True)
            self.sectdf.to_csv('sectfile.csv',index=False)

        if type(item)==type(DoctItem()):
            self.doctdf = self.doctdf.append(pd.Series(dict(item)),ignore_index=True)
            self.doctdf.to_csv('doctfile.csv',index=False)

        if type(item)==type(PatItem()):
            self.patdf = self.patdf.append(pd.Series(dict(item)),ignore_index=True)
            self.patdf.to_csv('patfile.csv',index=False)

        if type(item)==type(LLLItem()):
            self.llldf = self.llldf.append(pd.Series(dict(item)),ignore_index=True)
            self.llldf.to_csv('lllfile.csv',index=False)

        if type(item)==type(LLLSevItem()):
            self.lllsevdf = self.lllsevdf.append(pd.Series(dict(item)),ignore_index=True)
            self.lllsevdf.to_csv('lllsevfile.csv',index=False)

        return item

    def get_illness(self,illness_dict=None):
        try:
            if illness_dict == None: illness_dict = pd.read_csv('pat_ilns.csv')
            illness_dict_flipped = flip_dict_full(dict(illness_dict[['code','name',]].values))
        except:
            illness_dict = pd.read_csv('pat_ilns.csv')
            illness_dict_flipped = flip_dict_full(dict(illness_dict[['code','name',]].values))
        return illness_dict_flipped
