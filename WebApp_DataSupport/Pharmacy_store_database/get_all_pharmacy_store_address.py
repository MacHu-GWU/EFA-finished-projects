##encoding=utf8

from LinearSpider.jt import *

def get_all_pharmacy_store_address():
    '''extract all address from pharmacy store database
    '''
    cvs_path = r'CVS/CVS_data.json'
    riteaid_path = r'riteaids/riteaid_data.json'
    walgreens_path = r'walgreens/walgreens_data.json'
    cvs = load_jt(cvs_path)
    riteaid = load_jt(riteaid_path)
    walgreens = load_jt(walgreens_path)
    
    address_list = list()
    for v in cvs.itervalues():
        address_list.append(v['address'])
        
    for v in riteaid.itervalues():
        address_list.append(v['address'])
    
    for v in walgreens.itervalues():
        address_list.append(', '.join([v['street'],
                                       v['city'],
                                       v['state'],
                                       v['zipcode']]))
        
    dump_jt(address_list, 'address_list.json', replace = True)

if __name__ == '__main__':
    get_all_pharmacy_store_address()