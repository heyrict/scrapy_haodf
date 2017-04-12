# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pandas as pd, numpy as np
from haodf.items import *

class SaveCSVPipeline(object):
    def __init__(self):
        self.provdf = pd.DataFrame(columns=['prov_num','prov_nam'])
        self.hospdf = pd.DataFrame(columns=['prov_num','hosp_ix','hosp_nam'])
        self.sectdf = pd.DataFrame(columns=['prov_num','hosp_ix','section_ix','section_name'])
        self.doctdf = pd.DataFrame(columns=['doct_ix','doct_nam','doct_hot','doct_tot_sat_eff',
                        'doct_tot_sat_att','doct_tot_NoP','doct_NoP_in_2weeks'])
        self.patdf = pd.DataFrame(columns=['doct_ix','pat_nam','pat_time','pat_ilns','pat_reason',
                        'pat_aim','pat_sat_eff','pat_sat_att','pat_reservation','pat_cost',
                        'pat_status'])
        
    def process_item(self, item, spider):
        if type(item)==type(ProvItem()):
            self.provdf.append(pd.Series(item),ignore_index=True)
            self.provdf.to_csv('provfile.csv',index=False)

        if type(item)==type(HospItem()):
            self.hospdf.append(pd.Series(item),ignore_index=True)
            self.hospdf.to_csv('hospfile.csv',index=False)

        if type(item)==type(SectionItem()):
            self.sectdf.append(pd.Series(item),ignore_index=True)
            self.sectdf.to_csv('sectfile.csv',index=False)

        if type(item)==type(DoctItem()):
            self.doctdf.append(pd.Series(item),ignore_index=True)
            self.doctdf.to_csv('doctfile.csv',index=False)

        if type(item)==type(PatItem()):
            self.patdf.append(pd.Series(item),ignore_index=True)
            self.patdf.to_csv('patfile.csv',index=False)

        return item
