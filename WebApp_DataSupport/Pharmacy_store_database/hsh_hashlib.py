##encoding=utf8
##version =py27
##author  =sanhe
##date    =2014-09-07

'''This module pack some useful operations in functions
'''

import hashlib
import pickle

def md5_str(text):
    '''return md5 value from a STRING
    '''
    m = hashlib.md5()
    m.update(text)
    return m.hexdigest()

def md5_obj(obj):
    '''return md5 value from a PYTHON OBJECT
    '''
    m = hashlib.md5()
    m.update(pickle.dumps(obj) )
    return m.hexdigest()

def md5_file(fname, chunk_size = 2**10 ):
    '''return md5 value from a FILE
    Estimate processing time on:
        CPU = i7-4600U 2.10GHz - 2.70GHz, RAM = 8.00 GB
        1 second can process 0.25GB data
            0.59G - 2.43 sec
            1.3G - 5.68 sec
            1.9G - 7.72 sec
            2.5G - 10.32 sec
            3.9G - 16.0 sec
    
    ATTENTION:
        When you md5 a file, if you change the meta data (for example, the title, years information
        in audio, video), then the md5 value gonna change.
    '''
    m = hashlib.md5()
    with open(fname, 'rb') as f:
        while True:
            data = f.read(chunk_size)
            if not data:
                break
            m.update(data)
    return m.hexdigest()

if __name__ == '__main__':
    print md5_str('hello world!')
    print md5_obj(['1',2,'3',4])
    print md5_file('_note_hashlib.py')
