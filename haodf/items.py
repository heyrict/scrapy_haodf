# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class HaodfItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass

class ProvItem(scrapy.Item):
    prov_num = scrapy.Field(serializer=int)
    prov_name = scrapy.Field(serializer=str)

class IllnessItem(scrapy.Item):
    illness_ix = scrapy.Field(serializer=int)
    illness_name = scrapy.Field(serializer=str)
    illness_link = scrapy.Field(serializer=str)

class SectionItem(scrapy.Item):
    prov_num = scrapy.Field(serializer=int)
    hosp_ix = scrapy.Field(serializer=int)
    section_ix = scrapy.Field(serializer=int)
    section_name = scrapy.Field(serializer=str)

class HospItem(scrapy.Item):
    prov_num = scrapy.Field(serializer=int)
    hosp_ix = scrapy.Field(serializer=int)
    hosp_name = scrapy.Field(serializer=str)
    hosp_info = scrapy.Field()

class DoctItem(scrapy.Item):
    doct_ix = scrapy.Field(serializer=str)
    doct_hot = scrapy.Field(serializer=float)
    doct_tot_sat_eff = scrapy.Field(serializer=int)
    doct_tot_sat_att = scrapy.Field(serializer=int)
    doct_tot_NoP = scrapy.Field(serializer=int)
    doct_NoP_in_2weeks = scrapy.Field(serializer=int)
    doct_q = doct_a = scrapy.Field()
    doct_res = scrapy.Field()
    doct_telp = doct_teld = scrapy.Field()
    doct_class = scrapy.Field()
    doct_site = scrapy.Field(serializer=bool)
    doct_pat_treated = scrapy.Field()
    doct_pat_on = scrapy.Field()
    doct_stars = scrapy.Field()

class PatItem(scrapy.Item):
    doct_ix = scrapy.Field(serializer=int)
    pat_time = scrapy.Field()
    pat_ilns = scrapy.Field(serializer=str)
    pat_aim = scrapy.Field(serializer=int)
    pat_reason = scrapy.Field(serializer=int)
    pat_sat_eff = scrapy.Field(serializer=int)
    pat_sat_att = scrapy.Field(serializer=int)
    pat_reservation = scrapy.Field(serializer=int)
    pat_status = scrapy.Field(serializer=int)
    pat_cost = scrapy.Field(serializer=float)
    pat_name = scrapy.Field()
    pat_sukcd = scrapy.Field()

class LLLItem(scrapy.Item):
    lll_doctix = scrapy.Field()
    lll_articles = scrapy.Field()
    lll_patients = scrapy.Field()
    lll_wechat = scrapy.Field()
    lll_check = scrapy.Field()
    lll_votes = scrapy.Field()
    lll_thxlet = scrapy.Field()
    lll_presnt = scrapy.Field()
    lll_signup = scrapy.Field()

class LLLSevItem(scrapy.Item):
    lll_sev_doctix = scrapy.Field(serializer=int)
    lll_sev_tags = scrapy.Field()
    lll_sev_date = scrapy.Field()
