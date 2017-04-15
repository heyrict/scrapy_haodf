import scrapy, re
from scrapy import log
from haodf.items import *
import time
import pandas as pd, numpy as np
from scrapy_splash import SplashRequest

def true_link(lnk):
    global count,totcount,gdr
    if type(lnk) == str:
        f = lnk[0]
        if lnk[:4] == 'http': return lnk
        elif f == '/': return 'http://www.haodf.com' + lnk
    
    else:
        return [true_link(i) for i in lnk]

##名称匹配
namspc = {'看病目的':'pat_aim','疗效':'pat_sat_eff','态度':'pat_sat_att','选择该医生就诊的理由':'pat_reason','本次挂号途径':'pat_reservation','目前病情状态':'pat_status','本次看病费用总计':'pat_cost','患者':'pat_name'}
##名称匹配
docsetnamspc = {'疗效满意度':'doct_tot_sat_eff','态度满意度':'doct_tot_sat_att','累计帮助患者数':'doct_tot_NoP','近两周帮助患者数':'doct_NoP_in_2weeks'}

class get_illness(scrapy.Spider):
    name = 'get_illness'
    def __init__(self, *args, **kwargs):
        super(get_illness, self).__init__(*args, **kwargs)
        self.start_urls = ['http://www.haodf.com/jibing/erkezonghe/list.htm']
        self.all_links = []

    def parse_sections(self, response):
        sections = response.xpath('//div[@class="kstl"]//a')
        for section in sections:
            curlnk = section.xpath('./@href').extract_first()
            if section.xpath('./@href').extract_first() not in self.all_links:
                yield SectionItem(
                    section_link=curlnk,
                    section_name=section.xpath('./text()').extract_first(),
                    section_ix=len(self.all_links)-1)
                self.all_links += curlnk
                yield scrapy.Request(response.urljoin(curlnk),callback=self.parse_sections)

class get_all_prov(scrapy.Spider):
    name = 'get_all_prov'
    def __init__(self, *args, **kwargs):
        super(get_all_prov, self).__init__(*args, **kwargs)
        self.start_urls = ['http://www.haodf.com/yiyuan/all/list.htm']
        self.curprovnum = 0
        self.curhospnum = 0
        self.curdoctnum = 0
        #self.prevsectnum = None
        self.sectdict = {}
        self.doct_counts = {}

    def parse(self, response):
        for prov in response.xpath('//div[contains(@class,"kstl")]/a'):
            provnam = prov.xpath('./text()').extract_first()
            provnum = self.curprovnum
            self.curprovnum += 1
            yield ProvItem(
                    prov_num = provnum,
                    prov_name = provnam)
            yield scrapy.Request(response.urljoin(prov.xpath('./@href').extract_first()),meta={'provnum':provnum},callback=self.parse_hosp)


    def parse_hosp(self, response):
        provnum = response.meta['provnum']
        for hosp in response.xpath('//li/a[@target="_blank"]'):
            hospnam = hosp.xpath('./text()').extract_first()
            hospnum = self.curhospnum
            self.curhospnum += 1
            yield HospItem (
                    prov_num = provnum,
                    hosp_ix = hospnum,
                    hosp_name = hospnam)
            yield scrapy.Request(response.urljoin(hosp.xpath('./@href').extract_first()),meta={'provnum':provnum,'hospnum':hospnum},callback=self.parse_sect)

    def parse_sect(self,response):
        provnum = response.meta['provnum']
        hospnum = response.meta['hospnum']
        for sect in response.xpath('//table[@id="hosbra"]//a[@class="blue"]'):
            sectnam = sect.xpath('./text()').extract_first()
            if sectnam not in self.sectdict:
                self.sectdict[sectnam] = len(self.sectdict)
            sectnum = self.sectdict[sectnam]
            yield SectionItem(
                    prov_num = provnum,
                    hosp_ix = hospnum,
                    section_ix = sectnum,
                    section_name = sectnam)
            yield scrapy.Request(response.urljoin(sect.xpath('./@href').extract_first()),meta={'provnum':provnum,'hospnum':hospnum,'sectnum':sectnum},callback=self.parse_doct)

    def parse_doct(self,response):
        provnum = response.meta['provnum']
        hospnum = response.meta['hospnum']
        sectnum = response.meta['sectnum']
        #if self.prevsectnum != sectnum:
        #    self.curdoctnum = 0
        #else: self.curdoctnum += 1
        #doctnum = self.curdoctnum
        #self.prevsectnum = sectnum
        if hospnum not in self.doct_counts:
            self.doct_counts[hospnum] = 0
        doctnum = self.doct_counts[hospnum]
        self.doct_counts[hospnum] += 1

        for doct in response.xpath('//table[@id="doc_list_index"]//a[@class="name"]'):
            doctnam = doct.xpath('./text()').extract_first()
            yield scrapy.Request('http://localhost:8050/render.html?url='+response.urljoin(doct.xpath('./@href').extract_first()),meta={'provnum':provnum,'hospnum':hospnum,'sectnum':sectnum,'doctnum':doctnum},callback=self.parse_pat)

        next_page = response.xpath('//a[text()="下一页"]/@href').extract_first()
        if next_page: 
            yield scrapy.Request(response.urljoin(next_page),meta={'provnum':provnum,'hospnum':hospnum,'sectnum':sectnum},callback=self.parse_doct)

    def parse_pat(self,response):
        provnum = str(response.meta['provnum'])
        hospnum = str(response.meta['hospnum'])
        sectnum = str(response.meta['sectnum'])
        doctnum = str(response.meta['doctnum'])
        # getdoct
        doct_ix = '%s%s%s'%(hospnum.zfill(5),sectnum.zfill(3),doctnum.zfill(3))
        if response.xpath('//div[contains(@class,"doctor-home-page")]').extract_first():
            doct_hot = response.xpath('//div[@class="fl r-p-l"]/p[@class="r-p-l-score"]/text()').extract_first()
            tscore = response.xpath('//div[@class="fl score-part"]//text()').extract()
            doctitem = DoctItem(doct_ix=doct_ix,doct_hot=doct_hot)
            for scl in tscore:
                sclp = scl.strip().split('：')
                #self.log('sclp=%s'%sclp,level=log.WARNING)
                try:
                    if len(sclp)==2: doctitem[docsetnamspc[sclp[0].strip()]] = sclp[1].strip()
                except: doctitem[docsetnamspc[sclp[0].strip()]] = None
            yield doctitem# save doct item

        for pat in response.xpath('//table[@class="doctorjy"]'):
            curpat = PatItem(doct_ix=doct_ix)
            curpat['pat_time'] = pat.xpath('.//td[contains(@style,"text-align:right;")]/text()').extract_first()
            tilns = pat.xpath('.//td[@colspan="3"]//@href').extract_first()
            if tilns: curpat['pat_ilns'] = pat.xpath('.//td[@colspan="3"]//@href').extract_first().split('/')[-1].split('.')[0]
            # reprocess data (to be replaced later)
            for inf in pat.xpath('.//td[@colspan="3"]/span/text()').extract():
                t = inf.split('：')
                if t[0] in namspc.keys():
                    curpat[namspc[t[0]]] = t[1]
            t = pat.xpath('.//td[@colspan="2"]/text()').extract_first().split('：')
            if t[0] == '患者':
                if re.findall('\*\*\*',t[1]):
                    curpat['pat_name'] = t[1][0]
                elif re.findall('\.\*',t[1]):
                    curpat['pat_name'] = t[1].split('(')[0]
                else:
                    curpat['pat_name'] = np.nan
            try:curpat['pat_sat_eff'] = pat.xpath('.//td[@class="gray"][contains(text(),"疗效")]/span/text()').extract_first()
            except:curpat['pat_sat_eff'] = None
            try:curpat['pat_sat_att'] = pat.xpath('.//td[@class="gray"][contains(text(),"态度")]/span/text()').extract_first()
            except:curpat['pat_sat_att'] = None
        
            patadditinfo = pat.xpath('.//tbody//td[@valign="top"][@height="40px"]/div')
            for info in patadditinfo:
                if not info.xpath('span/text()').extract_first(): continue
                temp = namspc[info.xpath('span/text()').extract_first()[:-1]]
                curpat[temp] = info.xpath('./text()').extract_first()

            yield curpat

        # get next page
        all_link = response.xpath('//td[@class="center orange"]/a/@href').extract_first()
        if all_link:
            yield scrapy.Request(response.urljoin(all_link),meta={'provnum':provnum,'hospnum':hospnum,'sectnum':sectnum,'doctnum':doctnum},callback=self.parse_pat)
        
        next_link = response.xpath('//a[@class="p_num"][text()="下一页"]/@href')
        if next_link:
            yield scrapy.Request('http://localhost:8050/render.html?url='+response.urljoin(next_link),meta={'provnum':provnum,'hospnum':hospnum,'sectnum':sectnum,'doctnum':doctnum},callback=self.parse_pat)

