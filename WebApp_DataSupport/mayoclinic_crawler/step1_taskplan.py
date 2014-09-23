##encoding=utf8

from HSH.LinearSpider.crawler import Crawler, ignore_iteritems
from HSH.Data.jt import load_jt, dump_jt
from bs4 import BeautifulSoup as BS4
import os

def gen_entranceURL():
    """http://www.mayoclinic.org/diseases-conditions/index?letter=A to http://www.mayoclinic.org/diseases-conditions/index?letter=Z
    """
    prefix = 'http://www.mayoclinic.org/diseases-conditions/index?letter='
    res = list()
    for i in range(65, 91):
        res.append(prefix + chr(i))
    return res

def get_disease_url():
    """taskplan level1 get disease homepage url
    """
    base_url = 'http://www.mayoclinic.org'
    task = load_jt('task.json')
    spider = Crawler()
    
    for entrance_url in gen_entranceURL():
        html = spider.html(entrance_url)
        if html:
            soup = BS4(html)
            ol = soup.find_all('ol')[1]
            for li in ol.find_all('li'):
                url = base_url + li.a['href']
                task.setdefault(url, {'data': {'disease_name': li.text.strip()} } )
                dump_jt(task, 'task.json', replace = True)

def get_disease_category_url():
    """taskplan level2 get disease subcategory url
    """
    base_url = 'http://www.mayoclinic.org'         
    task = load_jt('task.json')   
    spider = Crawler()
    
    for url in task:
        html = spider.html(url)
        if html:
            soup = BS4(html)
            div = soup.find_all('div', id = 'main_0_left1_0_tertiarynav')[0]
            for a in div.find_all('a'):
                task[url].setdefault(base_url + a['href'], {'data': {'category': a.text.strip()}})
        dump_jt(task, 'task.json', replace = True)
        
# get_disease_category_url()   

def download_all():
    """crawl them all, disease_name - subcategory
    """
    task = load_jt('task.json')
    data = load_jt('data.json')
    spider = Crawler()
    
    for _, v in task.iteritems():
        disease_name = v['data']['disease_name']
        data.setdefault(disease_name, {})
        for url, v1 in ignore_iteritems(v, ignore = ['data']):
            print url
            html = spider.html(url)
            if html:
                soup = BS4(html)
                div = soup.find('div', id='main-content')
                data[disease_name].setdefault(v1['data']['category'], str(div))
        dump_jt(data, 'data.json', fastmode = True, replace = True)
        
# download_all()

def json_to_html():
    """generate human readable html
    """
    def correct_pathname(path):
        invalid_char = ['/', '\t', ':']
        for char in invalid_char:
            path = path.replace(char, '')
        return path
    
    base = 'data_html'
    if not os.path.exists(base):
        os.mkdir(base)
    
    data = load_jt('data.json')
    
    os.chdir(base)
    for disease_name, v in data.iteritems():
        disease_name = correct_pathname(disease_name) # eliminate invalid character
        if not os.path.exists(disease_name):
            os.mkdir(disease_name)
        for category, content in v.iteritems():
            if not os.path.exists(os.path.join(disease_name, '%s.html' % category)):
                with open(os.path.join(disease_name, '%s.html' % category), 'wb') as f:
                    f.write(content)
            
# json_to_html()

def json_to_txt():
    """generate human readable html
    """
    def correct_pathname(path):
        invalid_char = ['/', '\t', ':']
        for char in invalid_char:
            path = path.replace(char, '')
        return path
    
    base = 'data_html'
    if not os.path.exists(base):
        os.mkdir(base)
    
    data = load_jt('data.json')
    
    os.chdir(base)
    for disease_name, v in data.iteritems():
        disease_name = correct_pathname(disease_name) # eliminate invalid character
        if not os.path.exists(disease_name):
            os.mkdir(disease_name)
        for category, content in v.iteritems():
            if not os.path.exists(os.path.join(disease_name, '%s.html' % category)):
                with open(os.path.join(disease_name, '%s.html' % category), 'wb') as f:
                    f.write(content)
            
# json_to_txt()


def exam_integrity():
    data = load_jt('data.json')
    print len(data)
    
# exam_integrity()