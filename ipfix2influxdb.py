#!/root/projects/pydev/bin/python3.11

import socketserver
import ipfix.reader
import ipfix.ie
import json
import os
import sys
from datetime import datetime,timezone
import logging
from logging.handlers import RotatingFileHandler
import argparse
import traceback
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS


ap = argparse.ArgumentParser(description="Dump IPFIX files collected over TCP")
ap.add_argument('--spec', metavar='specfile', help='iespec file to read')
args = ap.parse_args()

ipfix.ie.use_iana_default()
ipfix.ie.use_5103_default()
if args.spec:
    ipfix.ie.use_specfile(args.spec)


# logging with file rotation
cwd = os.path.dirname(os.path.abspath(__file__))
logfile = f'{cwd}/log.log'

logging.basicConfig(level=logging.DEBUG,
            handlers=[RotatingFileHandler(logfile, maxBytes=50000000, backupCount=5)],
            format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
            datefmt='%a, %d %b %Y %H:%M:%S')

class Log:
    @classmethod
    def debug(cls, msg):
        logging.debug(msg)

    @classmethod
    def info(cls, msg):
        logging.info(msg)

    @classmethod
    def error(cls, msg):
        logging.error(msg)


class CollectorDictHandler(socketserver.DatagramRequestHandler):

    def handle(self):
        
        user = 'checkpoint'
        tok = '5aLQmk8e5yeRqsAdF5LJp-QcG2KGPX3c-E76PMNzjl5cRc_Z3GZo_ARb1Up39x-MLRezEGHa0ZdLi97l8Mv9qQ=='
        organization = 'checkpoint'
        bucket = 'ipfix'
        url = 'http://localhost:8086'
        
        # Log.info("Got an UDP Message from {}".format(self.client_address[0]))
        
        r = ipfix.reader.from_stream(self.rfile)
        
        for rec in r.namedict_iterator():
            
            # Log.info("--- From %s ---" %
            #     (str(self.client_address)))
            
            data = []
            tmp = {}
            tmp.update({'measurement' : self.client_address[0]})
            tmp.update({'fields' : {}})
            tmp.update({'tags': {}})
            tmp.update({'time' : str(datetime.now(timezone.utc))})
            
            for key in rec:
                skip = ['_ipfix_0_501', '_ipfix_0_502']
                if key in skip:
                    break
                else:
                    # Log.info("  %30s => %s" % (key, str(rec[key])))
                    tmp['fields'][key] = str(rec[key])
            
            data.append(tmp)
            # Log.debug(f"\n[Data Print]:\n{json.dumps(data, indent=4)}\n")
            
            # with InfluxDBClient(url = url, token = tok, org = organization, debug=True) as client:
            with InfluxDBClient(url = url, token = tok, org = organization) as client:
                with client.write_api(write_options=SYNCHRONOUS) as write_api:
                    loaded = data
                    resp = write_api.write(bucket, record=loaded)
                    # Log.debug(resp)
                

def end(): 
    sys.exit(0) 
    

def main(): 
    
    ss = socketserver.UDPServer(("", 4739), CollectorDictHandler)
    ss.serve_forever()


if __name__ == "__main__": 
    try: 
        main()
    except Exception as e: 
        Log.error(e)
        Log.error(traceback.format_exc)
    finally: 
        end()