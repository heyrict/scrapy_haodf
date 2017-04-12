import scrapy, re
from haodf.items import *
import pandas as pd, numpy as np

def true_link(lnk):
    global count,totcount,gdr
    if type(lnk) == str:
        f = lnk[0]
        if lnk[:4] == 'http': return lnk
        elif f == '/': return 'http://www.haodf.com' + lnk
    
    else:
        return [true_link(i) for i in lnk]

##疗效及态度满意度
sat_att = sat_eff = {'很不满意':2,'不满意':3,'一般':4,'满意':5,'很满意':6,'其他':0,'还不知道':1}
##看病目的
aim = {'治疗':3,'未填':0,'诊断':2,'其他':1,'咨询问题':4}
##选择该医生的理由
reason = {'网上评价':3,'医生推荐':2,'其他':1,'未填':0,'熟人推荐':4,}
##本次挂号途径
reservation = {'网络预约':3,'排队挂号':2,'其他':1,'未填':0}
##目前病情状态
status = {'有好转':3,'其他':1,'未见好转':2,'痊愈':4,'未填':0}
##名称匹配
namspc = {'看病目的':'pat_aim','疗效':'pat_sat_eff','态度':'pat_sat_att','选择该医生就诊的理由':'pat_reason','本次挂号途径':'pat_reservation','目前病情状态':'pat_status','本次看病费用总计':'pat_cost'}
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
        self.prevsectnum = None
        self.sectdict = {}

    def parse(self, response):
        for prov in response.xpath('//div[@class="kstl"]/a'):
            provnam = prov.xpath('./text()').extract_first()
            provnum = self.curprovnum
            self.curprovnum += 1
            yield ProvItem(
                    prov_num = provnum,
                    prov_nam = provnam,
                    prov_link = response.urljoin(prov.xpath('./@href').extract_first())) # save prov data
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
                    hosp_name = hospnam,
                    hosp_link = response.urljoin(hosp.xpath('./@href').extract_first()))
        # yield scrapy.Request(response.urljoin(hosp.xpath('./@href').extract_first()),meta={'provnum':provnum,'hospnum':hospnum},callback=self.parse_sect)

class get_all_sect(scrapy.Spider):
    name = 'get_all_sect'
    def __init__(self,*args,**kwargs):
        super(get_all_sect,self).__init__(*args,**kwargs)
        
    def parse_sect(self,response):
        provnum = response.meta['provnum']
        hospnum = response.meta['hospnum']
        for sect in response.xpath('//table[@id="hosbra"]//a[@class="blue"]'):
            sectnam = sect.xpath('./text()').extract_first()
            if sectnam not in self.sectdict:
                self.sectdict[sectnam] = len(self.sectdict)-1
            sectnum = self.sectdict[sectnam]
            yield SectionItem(
                    section_ix = sectnum,
                    section_name = sectnam)
            yield scrapy.Request(response.urljoin(sect.xpath('./@href').extract_first()),meta={'provnum':provnum,'hospnum':hospnum,'sectnum':sectnum},callback=self.parse_doct)

    def parse_doct(self,response):
        provnum = response.meta['provnum']
        hospnum = response.meta['hospnum']
        sectnum = response.meta['sectnum']
        if self.prevsectnum != sectnum:
            self.curdoctnum = 0
        else: self.curdoctnum += 1
        doctnum = self.curdoctnum
        self.prevsectnum = sectnum
        for doct in response.xpath('//table[@id="doct_list_index"]//a[@class="name"]'):
            doctnam = doct.xpath('./text()').extract_first()
            yield scrapy.Request(response.urljoin(doct.xpath('./@href').extract_first()),meta={'provnum':provnum,'hospnum':hospnum,'sectnum':sectnum,'doctnum':doctnum},callback=self.parse_pat)

        next_page = response.xpath('//a[text()="下一页"]/@href').extract_first()
        if next_page: 
            yield scrapy.Request(response.urljoin(next_page),meta={'provnum':provnum,'hospnum':hospnum,'sectnum':sectnum},callback=self.parse_doct)

    def parse_pat(self,response):
        provnum = response.meta['provnum']
        hospnum = response.meta['hospnum']
        sectnum = response.meta['sectnum']
        doctnum = response.meta['doctnum']
        
        # getdoct
        doct_ix = '%02s%03s%03s%03s'%(provnum,hospnum,sectnum,doctnum)
        if response.xpath('//div[contains(@class,"doctor-home-page")]').extract_first():
            doct_hot = response.xpath('//div[@class="fl r-p-l"]/p[@class="r-p-l-score"]/text()').extract()
            tscore = response.xpath('//div[@class="fl r-p-l"]/p[@class="r-p-l-score"]/text()').extract_first()
            doctitem = DoctItem(doct_ix=doct_ix,doct_hot=doct_hot)
            for scl in tscore:
                sclp = tscore.split('：')
                doctitem[eval(sclp[0]).strip()] = int(sclp[1]) if sclp[1][-1]!='%' else int(sclp[1][:-1])
            yield doctitem# save doct item

        for pat in response.xpath('//table[@class="doctorjy"]'):
            curpat = PatItem(doctix=doct_ix)
            curpat['pat_time'] = pat.xpath('.//td[contains(@style,"text-align:right;")]/text()').extract_first()
            curpat['ilns'] = pat.xpath('.//td[@colspan="3"]//@href').extract_first().split('/')[-1].split('.')[0]
            # reprocess data (to be replaced later)
            for inf in pat.xpath('.//td[@colspan="3"]/span/text()').extract():
                t = inf.split('：')
                if t[0] in namspc.keys():
                    curaim = [eval(namspc[t[0]])[i] if i in eval(namspc[t[0]]) else 1 for i in t[1].split('、')] 
                    curpat[namspc[t[0]]] = curaim
            t = pat.xpath('.//td[@colspan="2"]/text()').extract_first().split('：')
        if t[0] == '患者':
            if re.findall('\*\*\*',t[1]):
                curpat['pat_nam'] = t[1][0]
            elif re.findall('\.\*',t[1]):
                curpat['pat_nam'] = t[1].split('(')[0]
            else:
                curpat['pat_nam'] = np.nan
        curpat['pat_sat_eff'] = sat_eff(pat.xpath('.//td[@class="gray"][contains(text(),"疗效")]/span/text()'))
        curpat['pat_sat_att'] = sat_att(pat.xpath('.//td[@class="gray"][contains(text(),"态度")]/span/text()'))

        pass # add other cols

        yield curpat

        # get next page
        all_link = response.xpath('//td[@class="center orange"]/a/@href').extract_first()
        if all_link:
            yield scrapy.Response(response.urljoin(all_link),meta={'provnum':provnum,'hospnum':hospnum,'sectnum':sectnum,'doctnum':doctnum},callback=self.parse_pat)
        
        next_link = response.xpath('//a[@class="p_num"][text()="下一页"]/@href')
        if next_link:
            yield scrapy.Response(response.urljoin(next_link),meta={'provnum':provnum,'hospnum':hospnum,'sectnum':sectnum,'doctnum':doctnum},callback=self.parse_pat)

