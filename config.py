
try:
   import os
   import json
   import yaml
   import requests
   import logging
   import os
   import yaml
   import socket
except Exception as e:
   importerrormessage = "Issue:    Healthcheck script failed to import necessary modules\nFilename:    config.py\nReason:    This could be a RuntimeError that occured while importing a dependency module for this script to run. See API error to know which module had issues\n API Error:     {}".format(str(e))
   print(importerrormessage)
   
requests.packages.urllib3.disable_warnings()
oumclustersurl = "https://{}/api/datacenter/cluster/clusters"


def auth(auth_type):
   with open(os.path.abspath(os.path.dirname(__file__)) + '/yamlfiles/credentials.yaml', 'r') as outfile:
      creds = yaml.load(outfile, Loader=yaml.FullLoader)
   if auth_type == "api":
      return tuple([creds["api"]["user"], creds["api"]["pass"]])
   elif auth_type == "ldap":
      return tuple([creds["ldap"]["user"], creds["ldap"]["pass"]])
   else:
      logger.error("config.auth() unknown auth_type '%s'!" % auth_type)
      return None

def hostname(query):
   "Given a query type like either 'OUM Server' or 'Netapp NameandIP' this function returns its hostname and IP address"
   with open(os.path.abspath(os.path.dirname(__file__)) + '/yamlfiles/netappinfo.yaml', 'r') as outfile:
      dictionary = yaml.load(outfile, Loader=yaml.FullLoader)
   _hostname = list(dictionary[query].keys())
   _ipaddress = list(dictionary[query].values())
   return _hostname,_ipaddress

def netappcollection():
   "If the OUM name and IP is correctly specified in the netappinfo.yaml file this function will return all the netapps registered under this OUM, its management IP and location too!"
   netappinfo = []
   try:
      resp = requests.get(url=oumclustersurl.format(hostname('OUM Server')[0][0]),auth=auth("api"), verify=False)
      resp.raise_for_status()
      for i in range(resp.json()['num_records']):
         netappinfo.append([resp.json()['records'][i]['name'],resp.json()['records'][i]['location'],resp.json()['records'][i]['management_ip']])
   except Exception as e:
      print(e)
   return netappinfo

def getallnetapps():
   "This function contacts netappcollection function in config.py and returns all netapps registered in the OUM as of its runtime"
   allnetapps = netappcollection()
   netappfullnamelist = []
   netappnames = []
   for netapp in allnetapps:
      netappnames.append(socket.gethostbyaddr(netapp[2])[0])
   for netappname in netappnames:
      netappfullnamelist.append(netappname)
   return netappfullnamelist


