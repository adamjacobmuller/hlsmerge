#!/usr/bin/python
try:
    import signal
    from signal import SIGPIPE, SIG_IGN
    signal.signal(signal.SIGPIPE, signal.SIG_IGN)
except ImportError:
    pass

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO

import sys
import pycurl
import re
import pprint
import os
import subprocess
import smtplib
import datetime
import time
from email.mime.text import MIMEText

from subprocess import CalledProcessError
from urlparse import urljoin,urlparse
from optparse import OptionParser
def discard(data):
    return
def curl_multi(urls,max=5):
    num_urls = len(urls)
    num_conn = min(max, num_urls)
    m = pycurl.CurlMulti()
    m.handles = []
    results=[]
    for i in range(num_conn):
        c = pycurl.Curl()
        c.fp = None
        c.setopt(pycurl.FOLLOWLOCATION, 1)
        c.setopt(pycurl.MAXREDIRS, 5)
        c.setopt(pycurl.CONNECTTIMEOUT, 10)
        c.setopt(pycurl.TIMEOUT, 15)
        c.setopt(pycurl.NOSIGNAL, 1)
        c.setopt(pycurl.HEADER,True)
        m.handles.append(c)
    # Main loop
    freelist = m.handles[:]
    num_processed = 0
    failure_count=0
    while num_processed < num_urls:
        # If there is an url to process and a free curl object, add to multi stack
        while urls and freelist:
            url = urls.pop(0)
            c = freelist.pop()
            c.data=StringIO()
            c.setopt(pycurl.URL, url['url'])
            try:
                c.setopt(pycurl.PROXY, '%s:80'%url['ip'])
                c.ip=url['ip']
            except KeyError:
                c.ip=''
                pass
            c.setopt(pycurl.WRITEFUNCTION, c.data.write)
            #c.setopt(pycurl.VERBOSE,True)
            #c.setopt(pycurl.FILENAME,'/dev/null')
            m.add_handle(c)
            # store some info
            c.url = url['url']
        # Run the internal curl state machine for the multi stack
        while 1:
            ret, num_handles = m.perform()
            if ret != pycurl.E_CALL_MULTI_PERFORM:
                break
        # Check for curl objects which have terminated, and add them to the freelist
        while 1:
            num_q, ok_list, err_list = m.info_read()
            for c in ok_list:
                m.remove_handle(c)
                httpcode=c.getinfo(pycurl.HTTP_CODE)
                c.data.seek(0)
                data=c.data.getvalue()
                c.data.seek(0)
                if httpcode==200:
                    results.append({'url':c.url,'ip':c.ip,'result':'success','data':data})
                else:
                    failure_count+=1
                    results.append({'url':c.url,'ip':c.ip,'result':'httpfail','httpcode':httpcode,'data':data})
                    
                freelist.append(c)
            for c, errno, errmsg in err_list:
                m.remove_handle(c)
                c.data.seek(0)
                data=c.data.getvalue()
                c.data.seek(0)
                #print "[ %d / %d ] Failed %s %s %s %s" % (num_processed,num_urls,errno,errmsg,c.url, c.ip)
                results.append({'url':c.url,'ip':c.ip,'result':'failed','errno':errno,'errmsg':errmsg,'data':data})
                freelist.append(c)
                failure_count+=1
            num_processed = num_processed + len(ok_list) + len(err_list)
            if num_q == 0:
                break
        # Currently no more I/O is pending, could do something in the meantime
        # (display a progress bar, etc.).
        # We just call select() to sleep until some more data is available.
        sys.stdout.write("\r%8d / %8d / %8d" % (num_processed,num_urls,failure_count))
        m.select(1.0)
    
    
    # Cleanup
    print ""
    for c in m.handles:
        if c.fp is not None:
            c.fp.close()
            c.fp = None
        c.close()
    m.close()
    return results

def curl_cat(url):
    b = StringIO()
    c = pycurl.Curl()
    c.fp=b
    c.setopt(pycurl.URL,url)
    c.setopt(pycurl.WRITEFUNCTION,b.write)
    c.perform()
    b.seek(0)
    contents=b.getvalue()
    b.close()
    return contents

parser = OptionParser()
parser.add_option("-u","--urls",dest="urls")
parser.add_option("-i","--ips",dest="ips")

(options, args) = parser.parse_args()

urls=[]
ips=[]

uc_urls=open(options.urls).readlines()
uc_ips=open(options.ips).readlines()
ips=[]
urls=[]
for uc_ip in uc_ips:
    ips.append(uc_ip.strip())

line=0
feeder_urls=[]
for uc_url in uc_urls:
    line+=1
    re_result=re.search("^(http://[^:]+):(.*)$",uc_url.strip())
    if re_result is None:
        print "INVALID url spec %s" % uc_url.strip()
        sys.exit(1)
    feeder_urls.append({
        'source': re_result.group(1),
        'pattern': re_result.group(2)
        })

feeder_http_requests=[]
for feeder_url in feeder_urls:
    feeder_http_requests.append({
        'url':feeder_url['source']
        })

icount=0
lastfailcounter={}
nodes_downat={}
lastalert=0
while 1:
    iteration_start=time.time()
    icount+=1
    doalert=False
    messages=[]
    print "entering iteration %d" % icount
    sys.stdout.write("initiating feeder http requests..")
    feeder_http_results=curl_multi(feeder_http_requests)
    
    sys.stdout.write("..parsing..")
    for feeder_url in feeder_urls:
        for feeder_http_result in feeder_http_results:
            sys.stdout.write(".")
            if feeder_url['source']==feeder_http_result['url']:
                feeder_http_result_urls=re.findall(feeder_url['pattern'],feeder_http_result['data'])
                for feeder_http_result_url in feeder_http_result_urls:
                    urls.append(feeder_http_result_url)

    sys.stdout.write("..done\n")
    
    requests=[]
    failcounter={}
    for url in urls:
        for ip in ips:
            requests.append({'url':url,'ip':ip})
            failcounter[ip]=0
    
    results=curl_multi(requests,256)
    failures=[]
    for result in results:
        if result['result']=='httpfail':
            failures.append('Failed node %s HTTP code %s (URL %s)' % (result['ip'],result['httpcode'],result['url']))
            failcounter[result['ip']]+=1
        elif result['result']=='failed':
            failures.append('Failed node %s errno %s, errmsg %s (URL %s)' % (result['ip'],result['errno'],result['errmsg'],result['url']))
            failcounter[result['ip']]+=1
    
    if len(lastfailcounter)>0:
        total_failcount=0
        for ip,failcount in failcounter.items():
            total_failcount+=failcount
            if lastfailcounter[ip]==0 and failcounter[ip]>0:
                # node entering failure state
                messages.append("node %s is now failed" % ip)
                nodes_downat[ip]=time.time()
                doalert=True
            elif lastfailcounter[ip]>0 and failcounter[ip]==0:
                # node leaving failure state
                messages.append("node %s has now recovered" % ip)
                doalert=True
            elif lastfailcounter[ip]>0 and failcounter[ip]>0:
                # node in failure state
                try:
                    downat=nodes_downat[ip]
                    downfor=time.time()-downat
                    messages.append("node %s is still down (down for %.2f seconds)" % (ip,downfor))
                except KeyError:
                    nodes_downat[ip]=time.time()
                    downfor=0
                    messages.append("node %s is still down (down since program start)" % (ip))
                    doalert=True
            #elif lastfailcounter[ip]==0 and failcounter[ip]==0:
                # node not in failure state
                #messages.append("node %s is still healthy" % ip)
        sleepable=True
        if time.time()-lastalert>(60*10) and total_failcount>0:
            doalert=True
    else:
        sleepable=False
                

    if doalert==True:
        lastalert=time.time()
        print messages
        #recipients=['support@isprime.com','pr@isprime.com','adam@isprime.com','jn@isprime.com','ilya@isprime.com']
        recipients=['pr@isprime.com','adam@isprime.com','ilya@isprime.com']
        #recipients=['adam@isprime.com']
        message=MIMEText(
        "%s\n\n\n\ncurrent failures:\n%s\n" % (
        '\n'.join(messages),
        '\n'.join(failures)
        ))
        message['Subject']='CDN failure report for %s' % datetime.datetime.now().isoformat()
        message['From']='adam@isprime.com'
        message['To']=','.join(recipients)


        smtp=smtplib.SMTP('localhost')
        smtp.sendmail('adam@isprime.com',recipients,message.as_string())
        smtp.quit()

    lastfailcounter=failcounter
    iteration_took=time.time()-iteration_start
    if iteration_took<60 and sleepable==True:
        print "iteration %d completed in %.2f seconds, goodnight" % (icount,iteration_took)
        sys.stdout.flush()
        time.sleep(60-iteration_took)
    else:
        print "iteration %d completed in %.2f seconds" % (icount,iteration_took)
        sys.stdout.flush()
