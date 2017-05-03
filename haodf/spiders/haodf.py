import re
from scrapy import log
from haodf.items import *
import time
import pandas as pd, numpy as np
from scrapy_splash import SplashRequest
from data_processing import *

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
docsetnamspc = {'疗效满意度':'doct_tot_sat_eff','态度满意度':'doct_tot_sat_att','累计帮助患者数':'doct_tot_NoP','近两周帮助患者数':'doct_NoP_in_2weeks','诊治过的患者数':'doct_pat_treated','随访中的患者数':'doct_pat_on'}
##名称匹配
lllnamspc = {'总 文 章：':'lll_articles','总 患 者：':'lll_patients','微信诊后报道患者：':'lll_wechat','总诊后报道患者：':'lll_check','患者投票：':'lll_votes','感 谢 信：':'lll_thxlet','心意礼物：':'lll_presnt','开通时间：':'lll_signup'}


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

class haodf(scrapy.Spider):
    name = 'haodf'
    def __init__(self, *args, **kwargs):
        super(haodf, self).__init__(*args, **kwargs)
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
        for hosp in response.xpath('//div[@class="ct"]//li'):
            hospnam = hosp.xpath('./a[@target="_blank"]/text()').extract_first()
            if not hospnam: continue
            hospinfo = hosp.xpath('./span/text()').extract_first()
            hospnum = self.curhospnum
            self.curhospnum += 1
            yield HospItem (
                    prov_num = provnum,
                    hosp_ix = hospnum,
                    hosp_name = hospnam,
                    hosp_info = hospinfo)
            yield scrapy.Request(response.urljoin(hosp.xpath('./a[@target="_blank"]/@href').extract_first()),meta={'provnum':provnum,'hospnum':hospnum},callback=self.parse_sect)

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
            ## doct hot
            doct_hot = response.xpath('//div[@class="fl r-p-l"]/p[@class="r-p-l-score"]/text()').extract_first()
            ## site
            doct_site = response.xpath('.//div[contains(@class,"doctor-home-page")]//a/text()').extract_first()
            yield scrapy.Request(response.urljoin(doct_site),meta={'doctix':doct_ix},callback=self.parse_lll)
            ## class
            doct_class = np.nan
            doct_class_raw = response.xpath('.//div[@class="lt"]/table[not(@class)]/tbody/tr')
            for dc in doct_class_raw:
                if re.findall('职',''.join(dc.xpath('./td/text()').extract())):
                    doct_class = dc.xpath('./td/text()').extract()[-1].split()
                    break

            ## xxzx
            xxzx = response.xpath('.//table[@class="doct_data_xxzx"]')
            ### qraised
            qraised = xxzx.xpath('.//p[contains(text(),"患者提问")]/span/text()').extract()
            if len(qraised) == 2: patq = qraised[0]; pata = qraised[1]
            else: patq = pata = np.nan
            ### pres
            pres = xxzx.xpath('//a[contains(@href,"yuyue")]/text()').extract_first()
            pres = pres if pres else np.nan
            ### tel
            telprice = xxzx.xpath('.//span[@class="show_price"]/text()').extract()
            teldurat = xxzx.xpath('.//span[@class="show_duration"]/text()').extract()
            doct_telp = sum([re.findall('[0-9]+',i) for i in telprice],[]) if telprice else np.nan
            doct_teld = sum([re.findall('[0-9]+',i) for i in teldurat],[]) if telprice else np.nan

            doctitem = DoctItem(doct_ix=doct_ix,doct_hot=doct_hot,doct_q=patq,doct_a=pata,doct_res=pres,doct_tot_sat_eff=np.nan,doct_tot_sat_att=np.nan,doct_telp=doct_telp,doct_teld=doct_teld,doct_site=doct_site,doct_class=doct_class)
            ## tscore
            tscore = response.xpath('//div[@class="fl score-part"]/p/span[@class="r-p-score"]//text()').extract()
            for scl in tscore:
                sclp = scl.strip().split('：')
                #self.log('sclp=%s'%sclp,level=log.WARNING)
                try:
                    if len(sclp)==2: doctitem[docsetnamspc[sclp[0].strip()]] = float(sclp[1].strip()[:-1])
                except: doctitem[docsetnamspc[sclp[0].strip()]] = np.nan


            # service star
            doct_panel = response.xpath('//div[@id="bp_doctor_getvotestar"]//div[@class="rtdiv rtdivgao"]')
            if doct_panel.extract_first():
                for doct_panel_info in doct_panel.xpath('.//tr'):
                    if doct_panel_info.xpath('.//span').extract_first():
                        doctitem['doct_stars'] = len(doct_panel.xpath('.//span/img[contains(@src,"liang")]'))
                    else:
                        sclp = split_wrd(doct_panel_info.xpath('./td/text()').extract_first(),'：')
                        if len(sclp)!=2: continue
                        try:
                            doctitem[docsetnamspc[sclp[0].strip()]] = sclp[1].strip()
                        except:
                            try: doctitem[docsetnamspc[sclp[0].strip()]] = re.findall('[0-9]+',sclp[1])
                            except: doctitem[docsetnamspc[sclp[0].strip()]] = np.nan
                        
            yield doctitem

        # get all pat
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
            except:curpat['pat_sat_eff'] = np.nan
            try:curpat['pat_sat_att'] = pat.xpath('.//td[@class="gray"][contains(text(),"态度")]/span/text()').extract_first()
            except:curpat['pat_sat_att'] = np.nan

            curpat['pat_sukcd'] = 1 if pat.xpath('//a[contains(text(),"通过本站就诊")]').extract_first() else np.nan
        
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
        
        next_link = response.xpath('//a[@class="p_num"][text()="下一页"]/@href').extract_first()
        if next_link:
            yield scrapy.Request('http://localhost:8050/render.html?url='+response.urljoin(next_link),meta={'provnum':provnum,'hospnum':hospnum,'sectnum':sectnum,'doctnum':doctnum},callback=self.parse_pat)
        
    def parse_lll(self, response):
        doctix=response.meta['doctix']

        doct_info = response.xpath('//ul[@class="space_statistics"]')
        lllitem = LLLItem()
        lllitem['lll_doctix'] = doctix
        for i in doct_info.xpath('./li'):
            text = i.xpath('./text()').extract_first()
            if text in lllnamspc:
                lllitem[lllnamspc[text]] = i.xpath('./span/text()').extract_first()

        yield lllitem

        yield scrapy.Request(response.urljoin(response.url+r'/zixun/list.htm'),callback=self.parse_lllservice,meta={'doctix':doctix})

    def parse_lllservice(self, response):
        doctix=response.meta['doctix']
        
        for i in response.xpath('//div[@class="zixun_list"]/table//tr/td/p'):
            lllsevitem = LLLSevItem()
            lllsevitem['lll_sev_doctix'] = doctix
            lllsevitem['lll_sev_tags'] = i.xpath('./img/@title').extract()
            if not lllsevitem['lll_sev_tags']: lllsevitem['lll_sev_tags'] = np.nan
            yield scrapy.Request(response.urljoin(i.xpath('./a/@href').extract_first()),meta={'lllsevitem':lllsevitem},callback=self.parse_lllsevpat)

        next_page = response.xpath('//a[contains(text(),"下一页")]/@href').extract_first()
        if next_page: yield scrapy.Request(response.urljoin(next_page),meta=response.meta,callback=self.parse_lllservice)

    def parse_lllsevpat(self,response):
        lllsevitem = response.meta['lllsevitem']
        lllsevitem['lll_sev_date'] = response.xpath('//div[@class="yh_l_times"]/text()').extract_first()
        if not lllsevitem['lll_sev_date']: lllsevitem['lll_sev_date'] = np.nan
        yield lllsevitem
