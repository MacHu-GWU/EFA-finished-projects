##encoding=utf8
##version =py27
##author  =sanhe
##date    =2014-09-22

from HSH.LinearSpider.crawler import Crawler, ignore_iteritems
from HSH.Data.jt import load_jt, dump_jt, prt_jt
from bs4 import BeautifulSoup as BS4
import itertools
import re
import os

def gen_entranceURL():
    """ http://www.rxlist.com/drugs/alpha_a.htm TO
        http://www.rxlist.com/drugs/alpha_z.htm
    """
    res = list()
    for i in range(97, 123):
        res.append('http://www.rxlist.com/drugs/alpha_%s.htm' % chr(i))
    return res

def get_drug_url():
    """taskplan level1: get drug homepage url
    """
    base_url = 'http://www.rxlist.com'
    task = load_jt('task.json')
    spider = Crawler()
    
    for entrance_url in gen_entranceURL():
        html = spider.html(entrance_url)
        if html:
            soup = BS4(html)
            
            for li in soup.find_all('li'):
                try:
                    if li.span.text == '- FDA':
                        url = base_url + li.a['href']
                        task.setdefault(url, {'ref': {'drug_name' : li.a.text.strip()  }  }  )
                except:
                    pass
        dump_jt(task, 'task.json', replace = True)     

# get_drug_url()

def get_drug_category_url():
    """taskplan level2: get drug subcategory url
    """
    base_url = 'http://www.rxlist.com'
    task = load_jt('task.json')
    spider = Crawler()
    c = itertools.count()
    
    for url in task:
        print url, c.next()
        html = spider.html(url)
        if html:
            soup = BS4(html)
            for a in soup.find_all('a', class_ = 'tooltip', onclick = "wmdTrack('mono-pgn-bt');"):
                task[url].setdefault(base_url + a['href'], {'ref': {'category': a['title'] } } )
        dump_jt(task, 'task.json', replace = True)
        
# get_drug_category_url()

def download_all():
    """crawl all drugs informatio all, drug_name - subcategory
    drugname1 - category1(drug-description), category2(indications-dosage), category3(consumer-uses)...
    """
    task = load_jt('task.json')
    data = load_jt('data.json')
    spider = Crawler()
    
    for _, v in task.iteritems():
        disease_name = v['ref']['drug_name']
        data.setdefault(disease_name, {})
        for url, v1 in ignore_iteritems(v, ignore = ['ref']):
            if v1['ref']['category'] not in data[disease_name]: # if already crawled, skip it
                print url, 'has not been crawled'
                html = spider.html(url)
                if html:
                    try:
                        body = re.findall(r'(?<=<!-- start pages here -->)([\s\S]*)(?=<!-- end pages here -->)',html)[0]
                        data[disease_name].setdefault(v1['ref']['category'], body)
                    except:
                        pass
        dump_jt(data, 'data.json', fastmode = False, replace = True)
        
# download_all()

def json_to_html():
    """generate human readable html
    """
    def correct_pathname(path):
        invalid_char = ['/', '\t']
        for char in invalid_char:
            path = path.replace(char, '')
        return path
    
    base = 'data_html'
    if not os.path.exists(base):
        os.mkdir(base)
    
    data = load_jt('data.json')
    
    os.chdir(base)
    for drug_name, v in data.iteritems():
        drug_name = correct_pathname(drug_name) # eliminate invalid character
        if not os.path.exists(drug_name):
            os.mkdir(drug_name)
        for category, content in v.iteritems():
            if not os.path.exists(os.path.join(drug_name, '%s.html' % category)):
                with open(os.path.join(drug_name, '%s.html' % category), 'wb') as f:
                    f.write(content)
            
# json_to_html()

def json_to_plaintxt():
    """generate human readable html
    """
    def correct_pathname(path):
        invalid_char = ['/', '\t']
        for char in invalid_char:
            path = path.replace(char, '')
        return path
    
    base = 'data_txt'
    if not os.path.exists(base):
        os.mkdir(base)
    
    data = load_jt('data.json')
    
    os.chdir(base)
    for drug_name, v in data.iteritems():
        drug_name = correct_pathname(drug_name) # eliminate invalid character
        if not os.path.exists(drug_name):
            os.mkdir(drug_name)
        for category, content in v.iteritems():
            if not os.path.exists(os.path.join(drug_name, '%s.txt' % category)):
                with open(os.path.join(drug_name, '%s.txt' % category), 'wb') as f:
                    f.write(BS4(content).text)

# json_to_plaintxt()
     
def exam_integrity():
    task = load_jt('task.json')
    print len(task)
    ## use this code to make data.json smaller
    # data = load_jt('data.json')
    # dump_jt(data, 'data.json', fastmode = True, replace = True)
    
# exam_integrity()

print "COMPLETE!"