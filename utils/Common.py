#/usr/bin/env python

import os 
import logging
import inspect
import time
import datetime 
import re 
import math 
import locale
import ConfigParser
import StringIO
import gzip

#try:
#    ## set up for MySQLdb (assuming that this script will be executed with guse account)
#    os.environ['PYTHON_EGG_CACHE'] = os.path.join( os.environ['HOME'], '.python-eggs')
#    import MySQLdb as mdb
#except Exception,e:
#    import mysql.connector as mdb
#    from mysql.connector import errorcode
#except:
#    pass


# a class make the dictionary hashable 
class HashableDict(dict):
    def __hash__(self):
        return hash(tuple(sorted(self.items())))

def gzipContent(content):
    out = StringIO.StringIO()
    f = gzip.GzipFile(fileobj=out, mode='w', compresslevel=5)
    f.write(content)
    f.close()
    return out.getvalue()

def getMyLogger(name=None):

    if name is None:
        name = inspect.stack()[1][3]

    logging.basicConfig(format='[%(levelname)s:%(name)s] %(message)s', level=logging.ERROR)
    return logging.getLogger(name)

def getConfig(config_file='config.ini'):
    '''
    read and parse the config.ini file
    '''

    default_cfg = {
        'DB_DATA_DIR'        : '/var/log/torque/torquemon_db',
        'TORQUE_LOG_DIR'     : '/home/common/torque/job_logs',
        'TORQUE_BATCH_QUEUES': 'short,medium,long',
        'BIN_QSTAT_ALL'      : 'cluster-qstat',
        'BIN_FSHARE_ALL'     : 'cluster-fairshare'
    }

    config = ConfigParser.SafeConfigParser(default_cfg)
    config.read(config_file)

    return config

def getTimeInvolvement(mode='year', tdelta='7d', tstart=datetime.datetime.now()):
    '''resolve the years/months/days involved given the time range [tstart, tstart+tdelta]
       it returns an array of time string, for example:
         - ['2013','2014','2015']  if mode is 'year' and the time range involves years of 2013-2015
         - ['201401','201402']     if mode is 'month' and the time range involves months of Jan.-Feb. in 2014
         - ['20140101','20140102'] if mode is 'day' and the time range involves 1 Jan. and 2 Jan. of 2014
    '''

    re_td = re.compile('^([\-,\+]?[0-9\.]+)\s?(y|Y|m|M|d|D){0,1}$')  ## only recognize certain pattern of tdelta argument 

    ## default is 7 days in difference
    t_diff = 7
    t_unit = 'd'
    m = re_td.match(tdelta)
    if m:
        t_diff = float(m.group(1))
        if m.group(2):           ## override the default unit of 'd' if it's given in tdelta
            t_unit = m.group(2)

    ## convert to days, assuming
    ##  - 1 y = 365 d
    ##  - 1 m = 30 d
    if t_unit in ['y','Y']:
        t_diff = t_diff*365
    elif t_unit in ['m','M']:
        t_diff = t_diff*30 
    else:
        pass

    r_beg = int(math.floor(t_diff))
    r_end = 0
    if t_diff > 0:
        r_beg = 1 
        r_end = int(math.ceil(t_diff)) + 1 

    ts_digits = {'year':4, 'month':6, 'day':8}

    days = []
    days.append( tstart.strftime('%Y%m%d')[0:ts_digits[mode]] )
    for dt in range(r_beg, r_end):
        tnew = tstart + datetime.timedelta(days=dt)
        days.append( tnew.strftime('%Y%m%d')[0:ts_digits[mode]] )

    return sorted( list(set(days)) )

def parseTimeStringLocale(timeString):
    '''
    parse locale-dependent time string with timezone into seconds since epoch 
    '''

    t = None

    logger = getMyLogger()

    ## try parsing the time string with different locale
    for l in ['en_US', 'nl_NL']:
        locale.setlocale(locale.LC_TIME, l)
        try:
            t = time.strptime(timeString, '%a %b %d %H:%M:%S %Y %Z')
        except ValueError,e:
            try:
                t = time.strptime(timeString, '%a %b %d %H:%M:%S %Z %Y')
            except ValueError,e:
                pass
        if t:
            break

    ## set back to default locale
    locale.setlocale(locale.LC_TIME, '')

    return time.mktime(t)

def makeStructTimeUTC(value):
    '''
    Convert given time value into proper timestamp in UTC
    '''
    utc_tt = None
    if type(value) in [int,float,long]: ## value is second from epoch
        utc_tt = time.gmtime(value)
    elif type(value) == str: ## value is a string with timezone
        utc_tt = time.gmtime( parseTimeStringLocale(value) )
        if not utc_tt:
            raise ValueError('cannot parse time string: %s' % value)
    elif value == None:
        pass
    else:
        utc_tt = value

    return utc_tt

def fmtStructTimeUTC(stime):
    '''
    Format given struct time in to human readable string
    ''' 
    return time.strftime('%a %b %d %H:%M:%S %Y',stime)

#def getMySQLConnector(uid,passwd,db):
#
#    logger = getMyLogger()
#    cnx    = None
#    config = None 
#
#    if mdb.__name__ == 'MySQLdb':
#        ### use MySQLdb library
#        config = {'user'   : uid,
#                  'passwd' : passwd,
#                  'db'     : db,
#                  'host'   : 'localhost'}
#        try:
#            cnx = mdb.connect(**config)
#        except mdb.Error, e:
#            logger.error('db query error %d: %s' % (e.args[0],e.args[1]))
#
#            if cnx: cnx.close()
#    else:
#        ### use mysql-connector library
#        config = {'user'             : uid,
#                  'password'         : passwd,
#                  'database'         : db,
#                  'host'             : 'localhost',
#                  'raise_on_warnings': True }
#        try:
#            cnx = mdb.connect(**config)
#        except mdb.Error, err:
#            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
#                logger.error("something is wrong with your user name or password")
#            elif err.errno == errorcode.ER_BAD_DB_ERROR:
#                logger.error("database does not exists")
#            else:
#                logger.error(err)
#
#            if cnx: cnx.close()
#
#    return cnx
