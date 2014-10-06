##encoding=utf8
##version =py27
##author  =sanhe
##date    =2014-09-12

from HSH.Data.jt import *
from datetime import datetime, timedelta
from collections import OrderedDict
import jsontree
import os
# fname_dpt = r'reference/tsk_opt_dpt.json'
# fname_arv = r'reference/tsk_opt_arv.json'
# 
# tsk_opt_dpt = load_jt(fname_dpt)
# tsk_opt_arv = load_jt(fname_arv)
'''
tsk_opt = {date1 : {airport1 : counter,
                    airport2 : counter, ...}
           date2 : {samething} }
'''

class Task_optimizer(object):
    '''对于每天的机场数据，爬虫永远优先爬那些没有被爬过的机场
    '''
    def __init__(self, fname):
        if os.path.exists(fname): # exists, then load
            with open(fname, 'rb') as f:
                self.data = jsontree.loads(f.read())
        else:
            print '%s not exists! cannot load!, initialing task optimizer...' % fname
            airport_codelist = ["ATL", "LAX", "JFK", "DEN", "DTW", "SEA", "MSP", "MIA", "DCA", "IAD", "LGA", "MCO", "HNL", "RDU", "BWI", "PIT", "CMH", "CLE", "TPA", "MCI", "FLL", "SAT", "SMF", "SNA", "BUF", "OAK", "RIC", "OMA", "ORF", "DAY", "DSM", "CHS", "TYS", "PBI", "TUS", "BHM", "RSW", "MSN", "ONT", "GSP", "BUR", "LIT", "KOA", "HPN", "GEG", "FAT", "ICT", "PNS", "JAN", "BTV", "COS", "ITO", "CRP", "PSP", "EUG", "GRB", "FSD", "MYR", "ROA", "LFT", "LBB", "CHA", "MFE", "TLH", "MRY", "ENA", "LGB", "FWA", "VPS", "MFR", "EYW", "JNU", "ILM", "TVC", "LAN", "SRQ", "GJT", "PSC", "ABE", "SBP", "EVV", "AZO", "RAP", "BGR", "AGS", "TYR", "BRO", "STS", "ISP", "AEX", "JAC", "GFK", "LSE", "LNY", "SJT", "MVY", "CLD", "ELM", "LNK", "ACT", "DRO", "ITH", "GTF", "MHK", "MBS", "ASE", "VIS", "RIW", "SBY", "SAF", "ADQ", "AKN", "MLB", "DAB", "DLG", "BGM", "PIE", "HOM", "HLN", "IPL", "MWA", "HHH", "CYS", "BFF", "HTS", "SGU", "PUW", "PGV", "LBF", "DIK", "BRW", "CPR", "TTN", "LBL", "DDC", "ACV", "TXK", "CSG", "BHB", "TBN", "DHN", "UNK", "CDV", "CGI", "EAT", "IGM", "EGE", "IPT", "LNS", "ANI", "MKL", "PIH", "OOK", "WRL", "CMX", "ALO", "CEC", "GCK", "GTR", "MCE", "COD", "JHW", "IRK", "LEB", "SGY", "HOT", "AKP", "PUB", "APF", "GCC", "BTM", "VCT", "HNH", "BJI", "SDP", "RDG", "RKD", "BED", "AKI", "AUG", "HGR", "HDN", "ABR", "WYS", "IMT", "ART", "TLT", "FKL", "CKB", "CDC", "PSG", "PSE", "CYF", "JMS", "SLK", "BKW", "CIU", "AHN", "JST", "OGS", "BFD", "SHD", "BID", "MCK", "PHO", "WWT", "DVL", "CDR", "HYS", "KVL", "BKC", "GDV", "MSL", "PFN", "LAF", "HNM", "ALM", "CCR", "AUK", "KBC", "SCM", "LKE", "CHU", "UGB", "HIB", "OFK", "MYU", "LWB", "BWD", "GGW", "KYU", "PIZ", "KEK", "NLG", "CIK", "AIN", "KQA", "NUL", "MLS", "GON", "ARC", "TNC", "OBU", "FYV", "TCT", "LMA", "QBF", "KCQ", "IKO", "SRV", "NUI", "QWY", "MNT", "IWD", "HYG", "CKO", "KPN", "WWP", "IRC", "CLM", "BYA", "TEK", "EHM", "KGX", "PCA", "SVS", "GUP", "OTM", "SMK", "WTL", "CFA", "TKA", "MPB", "CKX", "AET", "BEH", "HKB", "STC", "KOT", "NCN", "STG", "IAN", "KWF", "NUP", "ORH", "SNP", "NYC", "FOD", "CNK", "KXA", "HAE", "PTH", "QKB", "WHH", "CHI", "MTM", "MUE", "DEC", "NME", "BLV", "WAS", "SCK", "WSN", "MDJ", "SWD", "RCE", "KPV", "GUC", "KCC", "PTA", "BRL", "GPZ", "TNK", "LUR", "PML", "EUE", "HBB", "MCG", "LBE", "IYK", "KTS", "LYU", "PDB", "VAK", "SXP", "KNK", "BKX", "CZN", "ZRF", "RBH", "ZRK", "NNL", "KLL", "QWM", "FOE", "MDH", "TKE", "KLG", "WAA", "KCG"]
            self.data = {dt : { airport_code : 0 for airport_code in airport_codelist} for dt in dt_interval_generator('2014-09-10', '2014-12-30')}
        self.path = fname
        
    def _dump(self):
        dump_jt(self.data, self.path, fastmode = True, replace = True)
        
    def add(self, date, airport_code):
        try:
            self.data[date][airport_code] += 1
        except:
            print 'failed to add value in date = %s, airpot = %s' % (date, airport_code)
            
    def next_airport(self, date):
        od = OrderedDict( sorted(self.data[date].items(), 
                             key=lambda t: t[1], ## t[0]指根据key排序, t[1]指根据value排序
                             reverse = False) ) ## True指逆序排序，False指正序排序
        for airport_code in od.iterkeys():
            break
        return airport_code
        
    def opt_list(self, date):
        od = OrderedDict( sorted(self.data[date].items(), 
                             key=lambda t: t[1], ## t[0]指根据key排序, t[1]指根据value排序
                             reverse = False) ) ## True指逆序排序，False指正序排序
        return list( od.iterkeys() )
         
def dt_interval_generator(start, end):
    ''' 
    INPUT = ('2014-01-01', '2014-01-03')
    yield 2014-01-01, 2014-01-02, 2014-01-03
    '''
    start, end = datetime.strptime(start, '%Y-%m-%d'), datetime.strptime(end, '%Y-%m-%d')
    delta = timedelta(1)
    for i in xrange( (end-start).days + 1):
        dt = start + i * delta
        yield datetime.strftime( dt, '%Y-%m-%d')
        
if __name__ == '__main__':
    topt = Task_optimizer('nice.json')
    topt.add('2014-09-10', 'DCA')
    for i in xrange(5):
        airport = topt.next_airport('2014-09-10')
        print airport
        topt.add('2014-09-10', airport)
    topt._dump()
#     dump_jt(topt.data, 'topt.json', fastmode = True, replace = True)
