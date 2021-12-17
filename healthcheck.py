try:
    import json
    import yaml
    import requests
    import pandas as pd
    import config
    import re
    import logging
    from optparse import OptionParser
    import commands
    import htmltexts
    from datetime import datetime
    import time
    import socket
    import sys
    from pathlib import Path
    import os
    import mailnotification
except Exception as e:
   print("Issue:    Healthcheck script failed to import necessary modules\nFilename:    healthcheck.py\nReason:    This could be a RuntimeError that occured while importing a dependency module for this script to run. See API error to know which module had issues\n API Error:     {}".format(str(e)))

###########################################
## Necessary settings and error handling ##
###########################################

pd.options.display.float_format = "{:.2f}".format
requests.packages.urllib3.disable_warnings()
api_auth = config.auth('api')


##############
## URl repo ##
##############

clusterurl = "https://{}/api/cluster?fields=name,version"
nodeshowurl = "https://{}/api/cluster/nodes?fields=*"
snapmirrorlsurl = "https://{}/api/private/cli/snapmirror?source_volume=*&type=LS&fields=healthy,status,state"
networkinterfaceurl = "https://{}/api/network/ip/interfaces?fields=name,state,svm,ip,location,ipspace&return_records=true&return_timeout=15"
aggrshowurl = "https://{}/api/storage/aggregates?fields=*"
svmurl = "https://{}/api/svm/svms"
networkporturl = "https://{}/api/network/ethernet/ports?fields=*"
volshowurl = "https://{}/api/storage/volumes?fields=*"
routeshowurl = "https://{}/api/network/ip/routes?fields=*"
diskshowurl = "https://{}/api/storage/disks?fields=name,usable_size,shelf,bay,type,container_type,aggregates,self_encrypting,home_node,state,firmware_version,model&order_by=name"

########################################
## Global variables for Summary table ##
########################################

#######################
## List placeholders ##
#######################

summarylist = []

#########################
## String placeholders ##
#########################

nodestoragefailovershow_html = ""
networkinterfaceshow_html = ""
aggregateshow_html = ""
networkportshow_html = ""
volshow_html = ""
routeshow_html = ""
diskshow_html = ""
dnscheck_html = ""
nfsconn_html = ""
nfstopten_html = ""
cifsconn_html = ""
cifstopten_html = ""


def mainheading():
   mainheadstylestr = '''<html><head><style>h1 {text-align: center;}p {text-align: left;}
                           div {text-align: left;}</style></head><body>    
                           '''
   mainheadstr = '''<h1>
                        Netapp Health Check
                                          </h1>  
                                           '''
   gapstr = '<br>'
   return mainheadstylestr + " " + mainheadstr + " " + gapstr


def voltype(netappcluster,rwvollist):
   volurl = "https://{}/api/storage/volumes?is_svm_root=false&fields={}".format(netappcluster,'type')
   voltype = []
   systemtype = []
   rwvols = []
   try:
      volresp = requests.get(url=volurl,auth=api_auth, verify=False)
      vols = volresp.json()
      for vol in range(len(vols["records"])): 
         voltype.append(vols["records"][vol]['type']) if vols["records"][vol]['type'] != 'ls' else ''
         rwvols.append(vols["records"][vol]['name']) if vols["records"][vol]['type'] == 'rw' else ''
      if len(voltype) > 2:
         if voltype.count('dp') > voltype.count('rw'):
            systemtype.append('backup')
         elif voltype.count('rw') > voltype.count('dp'):
            systemtype.append('prod')
      else:
         systemtype.append('unknown')
   except Exception as e:
      print(e)
   if rwvollist == "True":
      return systemtype,rwvols
   else:
      return systemtype

def svmtype(netappcluster):
   svmtypeurl = svmurl.format(netappcluster)
   svm_name = []
   systemtype = ''
   try:
      svmresp = requests.get(url=svmtypeurl,auth=api_auth, verify=False)
      svmnames = svmresp.json()
      for svmname in range(len(svmnames["records"])):
         svm_name.append(svmnames["records"][svmname]['name'])
      systemtype = 'backup' if len(list(filter(lambda x: x.endswith('backup'), svm_name)))/len(svm_name) >= 0.5 else 'prod'
   except Exception as e:
      print(e)
   return systemtype

def summarytable(netappcluster,summarylist):
   gapstr = '<br>'
   summarytable = pd.DataFrame()
   summarytable = pd.DataFrame(summarylist ,columns =['Command', 'Overall_Status'])
   summarytable.index += 1
   summarytable = summarytable.to_html()
   summarytable = summarytable.replace('\n','')
   fst = summarytable.replace('#9989;','&#9989;')
   fst = fst.replace('#10060;','&#10060;')
   fst += gapstr
   return fst

def finalwrite(filename,text):
   file = open(filename,"a")
   file.write(text)
   file.close()

def html_headtail(netapplist):
   print("html_headtail started.This function sets the html header, loops through all given netapps and then sets the tail at the end and returns the html output under results folder") if options.timeit else None
   logger.info("html_headtail function started. This function sets the html header, loops through all given netapps and then sets the tail at the end and returns the html output under results folder") if options.debug else None
   today = datetime.now()
   hcheading = mainheading()
   datetimestring = str(today.strftime("%Y%m%d::%H:%M:%S"))
   pathwithfilename = "/results/HealthCheck_Run_"+datetimestring
   print("Date time recorded for this run :"+datetimestring) if options.timeit else None
   logger.info("Date time recorded for this run :"+datetimestring) if options.debug else None
   p = Path(os.path.abspath(os.path.dirname(__file__))+pathwithfilename+".html")
   finalwrite(filename = (os.path.abspath(os.path.dirname(__file__))+pathwithfilename+".html"),text= htmltexts.openingtext+hcheading)
   for netapp in netapplist:
      subheadstr = '''<p><b>Netapp Name:</b> {}</p>'''.format(netapp)
      finalwrite(filename = (os.path.abspath(os.path.dirname(__file__))+pathwithfilename+".html"),text= subheadstr)
      htmlconstruct(netapp,today)
   finalwrite(filename = (os.path.abspath(os.path.dirname(__file__))+pathwithfilename+".html"),text= htmltexts.endingtext)
   print("final write to html file done")
   if p.exists():
      logger.info("Checked if the file created for this run exists in the folder <script_path>/results") if options.debug else None
      print("Checked if the file created for this run exists in the folder <script_path>/results") if options.debug else None
      logger.info("File successfully stored with data in path :"+(os.path.abspath(os.path.dirname(__file__))+pathwithfilename+".html")) if options.debug else None
      print("File successfully stored with data in path :"+(os.path.abspath(os.path.dirname(__file__))+pathwithfilename+".html")) if options.timeit else None
      with open(os.path.abspath(os.path.dirname(__file__))+pathwithfilename+".html", 'r') as f:
         html_payload = f.read()
         mailtext = "Health Check Script completed!\n. This report consists of netapp(s): "+str(netapplist)
         mailnotification.attachment(mailtext,"","Netapp_Health_Check.html",html_payload,options.recipient)
         logger.info('Mail with html file output sent to recipient') if options.debug else None
         print('Mail with html file output sent to recipient') if options.debug else None
         f.close()
      return "success"
   else:
      return "failed"
      logger.info("Failed to create and store the html file as expected") if options.debug else None
      print("Failed to create and store the html file as expected") if options.timeit else None

def htmlconstruct(netappcluster,dateandtime):
   gapstr = '<br>'
   nodestorageoverallstatus(netappcluster)
   nwintoverallstatus(netappcluster)
   aggroverallstatus(netappcluster)
   nwportoverallstatus(netappcluster)
   voloverallstatus(netappcluster)
   routeoverallstatus(netappcluster)
   diskoverallstatus(netappcluster)
   dnsoverallstatus(netappcluster)
   connoverallstatus(netappcluster)
   textsummary = commands.textsummary(netappcluster)
   versiontext = commands.version(netappcluster)
   summary_table = summarytable(netappcluster,summarylist)
   summarylist.clear()
   detailedinfo = "<h2>Detailed Information</h2>"
   dco_struct_nss = '''
   <button class="accordion">{}</button>
   <div class="panel">
      <p>Important information from the "node show" & "storage failover show" commands</p>
      {}
      <br>
   </div>
   '''.format("Node show & Storage Failover Status",nodestoragefailovershow_html)
   dco_struct_nws = '''
   <button class="accordion">{}</button>
   <div class="panel">
      <p>Important information from the "network interface show" command</p>
      {}
      <br>
   </div>
   '''.format("Network Interface Status",networkinterfaceshow_html)
   dco_struct_aggrs = '''
   <button class="accordion">{}</button>
   <div class="panel">
      <p>Important information from the "aggr status" command</p>
      {}
      <br>
   </div>
   '''.format("Aggregate Status",aggregateshow_html)
   dco_struct_nps = '''
   <button class="accordion">{}</button>
   <div class="panel">
      <p>Important information from the "network port show" command</p>
      {}
      <br>
   </div>
   '''.format("Network Port Status",networkportshow_html)
   dco_struct_vss = '''
   <button class="accordion">{}</button>
   <div class="panel">
      <p>Important information from the "vol show" command</p>
      <p><b>Note:</b> Volume names highlighted in orange or amber have crossed "86%" consumption and ones shown in red have crossed "90%" consumption</p>
      {}
      <br>
   </div>
   '''.format("Volume Show Status",volshow_html)
   dco_struct_rs = '''
   <button class="accordion">{}</button>
   <div class="panel">
      <p>Important information from the "route show" command</p>
      {}
      <br>
   </div>
   '''.format("Route Show Status",routeshow_html)
   dco_struct_ds = '''
   <button class="accordion">{}</button>
   <div class="panel">
      <p>Important information from the "disk show" command</p>
      {}
      <br>
   </div>
   '''.format("Disk Show Status",diskshow_html)
   dco_struct_dns =  '''
   <button class="accordion">{}</button>
   <div class="panel">
      <p>Important information from the "dns check -vserver ()" command</p>
      {}
      <br>
   </div>
   '''.format("DNS Check Status",dnscheck_html)
   dco_struct_nfsconn =  '''
   <button class="accordion">{}</button>
   <div class="panel">
      <p>Top NFS connections to this netapp</p>
      {}
      <br>
      <br>
      <p>Important information from the "network active connection show -vserver () -service nfs" command</p>
      {}
      <br>
   </div>
   '''.format("NFS Active Connections Status",nfstopten_html,nfsconn_html)
   dco_struct_cifsconn =  '''
   <button class="accordion">{}</button>
   <div class="panel">
      <p>Top CIFS connections to this netapp</p>
      {}
      <br>
      <br>
      <p>Important information from the "network active connection show -vserver () -service cifs_srv" command</p>
      {}
      <br>
   </div>
   '''.format("CIFS Active Connections Status",cifstopten_html,cifsconn_html)
   scriptruntime = '''
      <p><b>Script Run at:</b> {}</p>
   '''.format(datetime.now().strftime('%Y%m%d::%H:%M:%S')).replace('\n','')
   finaltext = scriptruntime + textsummary + versiontext + gapstr + summary_table + detailedinfo + dco_struct_nss + dco_struct_nws + dco_struct_aggrs + dco_struct_nps + dco_struct_vss + dco_struct_rs + dco_struct_ds + dco_struct_dns + dco_struct_nfsconn + dco_struct_cifsconn + gapstr
   finalwrite(filename = (os.path.abspath(os.path.dirname(__file__))+"/results/HealthCheck_Run_"+str(dateandtime.strftime("%Y%m%d::%H:%M:%S"))+".html"),text=finaltext)
   logger.info("Netapp {}'s information has been recorded to the file".format(netappcluster)) if options.debug else None
   print("Netapp {}'s information has been recorded to the file".format(netappcluster)) if options.timeit else None


def nodestorageoverallstatus(netappcluster):
   check = []
   nodeshow_final = []
   storagefailover_final = []
   start_time = time.time()
   output = commands.nodeshow(netappcluster)
   global nodestoragefailovershow_html
   nodestoragefailovershow_html = output[0]
   nodeshowtuple = output[1]
   if 'down' in nodeshowtuple[3]:
      check.append('#10060;')
   elif 'nothing_to_giveback' not in nodeshowtuple[5]:
      check.append('#10060;')
   elif 'not_attempted' not in nodeshowtuple[6]:
      check.append('#10060;')
   elif nodeshowtuple[0][0] == nodeshowtuple[-1][0]:
      check.append('#10060;')
   else:
      check.append('#9989;')
   nodeshow_final.append('  node show ')
   nodeshow_final.append(check[0])
   summarylist.append(nodeshow_final)
   storagefailover_final.append(' storage failover show ')
   storagefailover_final.append(check[0])
   summarylist.append(storagefailover_final)
   logger.info("{}'s nodestorageoverallstatus collected".format(netappcluster)) if options.debug else None
   print("{}'s nodestorageoverallstatus collected. Time taken is : {} seconds".format(netappcluster,round((time.time() - start_time),2))) if options.timeit else None
   return nodeshow_final,storagefailover_final

def nwintoverallstatus(netappcluster):
   check = []
   nwshow_final = []
   start_time = time.time()
   output = commands.networkinterfaceshow(netappcluster)
   global networkinterfaceshow_html
   networkinterfaceshow_html = output[0]
   nwshowtuple = output[1]
   if 'False' in nwshowtuple[3] or 'down' in nwshowtuple[1]:
      check.append('#10060;')
   elif 'False' in nwshowtuple[3] and 'down' in nwshowtuple[1]:
      check.append('#10060;')
   else:
      check.append('#9989;')
   nwshow_final.append('network interface show')
   nwshow_final.append(check[0])
   summarylist.append(nwshow_final)
   logger.info("{}'s nwintoverallstatus collected".format(netappcluster)) if options.debug else None
   print("{}'s nwintoverallstatus collected. Time taken is : {} seconds".format(netappcluster,round((time.time() - start_time),2))) if options.timeit else None
   return nwshow_final

def aggroverallstatus(netappcluster):
   check = []
   aggrshow_final = []
   start_time = time.time()
   output = commands.aggrstatus(netappcluster)
   global aggregateshow_html
   aggregateshow_html = output[0]
   aggrshowtuple = output[1]
   if 'online' not in aggrshowtuple[1] or len(commands.spaceissueaggr) > 0:
      check.append('#10060;')
   elif 'online' not in aggrshowtuple[1] and len(commands.spaceissueaggr) > 0:
      check.append('#10060;')
   else:
      check.append('#9989;')
   aggrshow_final.append('aggregate show')
   aggrshow_final.append(check[0])
   summarylist.append(aggrshow_final)
   logger.info("{}'s aggroverallstatus collected".format(netappcluster)) if options.debug else None
   print("{}'s aggroverallstatus collected. Time taken is : {} seconds".format(netappcluster,round((time.time() - start_time),2))) if options.timeit else None
   return aggrshow_final

def nwportoverallstatus(netappcluster):
   check = []
   nwportshow_final = []
   start_time = time.time()
   output = commands.networkportshow(netappcluster)
   global networkportshow_html
   networkportshow_html = output[0]
   nwportshowtuple = output[1]
   if 'up' not in nwportshowtuple[1] or len(commands.affectednetworkport) > 0:
      check.append('#10060;')
   elif 'up' not in nwportshowtuple[1] and len(commands.affectednetworkport) > 0:
      check.append('#10060;')
   else:
      check.append('#9989;')
   nwportshow_final.append('network port show')
   nwportshow_final.append(check[0])
   summarylist.append(nwportshow_final)
   logger.info("{}'s nwportoverallstatus collected".format(netappcluster)) if options.debug else None
   print("{}'s nwportoverallstatus collected. Time taken is : {} seconds".format(netappcluster,round((time.time() - start_time),2))) if options.timeit else None
   return nwportshow_final

def voloverallstatus(netappcluster):
   check = []
   volshow_final = []
   start_time = time.time()
   output = commands.volshow(netappcluster)
   global volshow_html
   volshow_html = output[0]
   volshowtuple = output[1]
   if 'online' not in volshowtuple[1] or len(commands.affectedvol) > 0:
      check.append('#10060;')
   elif 'online' not in volshowtuple[1] and len(commands.affectedvol) > 0:
      check.append('#10060;')
   else:
      check.append('#9989;')
   volshow_final.append('volume show')
   volshow_final.append(check[0])
   summarylist.append(volshow_final)
   logger.info("{}'s voloverallstatus collected".format(netappcluster)) if options.debug else None
   print("{}'s voloverallstatus collected. Time taken is : {} seconds".format(netappcluster,round((time.time() - start_time),2))) if options.timeit else None
   return volshow_final

def routeoverallstatus(netappcluster):
   check = []
   routeshow_final = []
   svmlist = []
   start_time = time.time()
   global routeshow_html
   svmresp = requests.get(url=svmurl.format(netappcluster),auth=api_auth,verify=False).json()
   output = commands.routeshow(netappcluster)
   routeshow_html = output[0]
   routeshowtuple = output[1]
   commands.routeinfo.clear()
   for svm in range(len(svmresp["records"])):
      svmlist.append(svmresp["records"][svm]['name'])
   if len(routeshowtuple[0]) < len(svmlist):
      commands.routeinfo.append("FYI. Not all SVMs have routes configured. This netapp has {} SVMs but only {} SVMs have routes configured".format(len(svmlist),len(routeshowtuple[0])))
   if len(routeshowtuple[0]) > 0:
      check.append('#9989;')
   else:
      check.append('#10060;')
   routeshow_final.append('route show')
   routeshow_final.append(check[0])
   summarylist.append(routeshow_final)
   logger.info("{}'s routeoverallstatus collected".format(netappcluster)) if options.debug else None
   print("{}'s routeoverallstatus collected. Time taken is : {} seconds".format(netappcluster,round((time.time() - start_time),2))) if options.timeit else None
   return routeshow_final

def diskoverallstatus(netappcluster):
   check = []
   diskshow_final = []
   start_time = time.time()
   global diskshow_html
   output = commands.diskshow(netappcluster)
   diskshow_html = output[0]
   diskshowtuple = output[1]
   if 'present' not in diskshowtuple[1] and 'spare' not in diskshowtuple[1] or len(commands.affecteddisk) > 0:
      check.append('#10060;')
   elif 'spare' not in diskshowtuple[1] and len(commands.affecteddisk) > 0:
      check.append('#10060;')
   else:
      check.append('#9989;')
   diskshow_final.append('disk show')
   diskshow_final.append(check[0])
   summarylist.append(diskshow_final)
   logger.info("{}'s diskoverallstatus collected".format(netappcluster)) if options.debug else None
   print("{}'s diskoverallstatus collected. Time taken is : {} seconds".format(netappcluster,round((time.time() - start_time),2))) if options.timeit else None
   return diskshow_final


def dnsoverallstatus(netappcluster):
   check = []
   dnscheck_final = []
   start_time = time.time()
   global dnscheck_html
   output = commands.dnscheck(netappcluster)
   dnscheck_html = output[0]
   dnschecktuple = output[1]
   if commands.dnscheckverify(commands.svmname_dns,dnschecktuple) == "success":
      logger.info("dnscheckverify function completed successfully. Now we know if all SVMs have DNS configured") if options.debug else None
      print("dnscheckverify function completed successfully. Now we know if all SVMs have DNS configured") if options.timeit else None
   else:
      logger.info("dnscheckverify function might have failed. So we dont know if all SVMs have DNS configured") if options.debug else None
      print("dnscheckverify function might have failed. So we dont know if all SVMs have DNS configured") if options.timeit else None
   if 'down' in dnschecktuple[2] or len(commands.affecteddns) > 0:
      check.append('#10060;')
   elif 'spare' not in dnschecktuple[1] and len(commands.affecteddns) > 0:
      check.append('#10060;')
   else:
      check.append('#9989;')
   dnscheck_final.append('dns check')
   dnscheck_final.append(check[0])
   summarylist.append(dnscheck_final)
   logger.info("{}'s dnsoverallstatus collected".format(netappcluster)) if options.debug else None
   print("{}'s dnsoverallstatus collected. Time taken is : {} seconds".format(netappcluster,round((time.time() - start_time),2))) if options.timeit else None
   return dnscheck_final

def connoverallstatus(netappcluster):
   check = []
   nfsconcheck_final = []
   cifsconcheck_final = []
   start_time = time.time()
   global nfsconn_html
   global nfstopten_html
   global cifsconn_html
   global cifstopten_html
   nfsoutput = commands.connshow(netappcluster,"nfs")
   cifsoutput = commands.connshow(netappcluster,"cifs_srv")
   nfsconn_html = nfsoutput[0]
   nfstopten_html = nfsoutput[2]
   cifsconn_html = cifsoutput[0]
   cifstopten_html = cifsoutput[2]
   if nfsconn_html != "nodata":
      logger.info("{}'s connoverallstatus collected for NFS sessions".format(netappcluster)) if options.debug else None
      print("{}'s connoverallstatus collected for NFS sessions. Time taken is : {} seconds".format(netappcluster,round((time.time() - start_time),2))) if options.timeit else None
      return "nfs sessions present"
   if cifsconn_html != "nodata":
      logger.info("{}'s connoverallstatus collected for CIFS sessions".format(netappcluster)) if options.debug else None
      print("{}'s connoverallstatus collected for CIFS sessions. Time taken is : {} seconds".format(netappcluster,round((time.time() - start_time),2))) if options.timeit else None
      return "cifs sessions present"
   else:
      logger.info("{}'s connoverallstatus collection failed or one of the tables are empty because there are no cifs or nfs sessions for this netapp".format(netappcluster))
      print("{}'s connoverallstatus collection failed or one of the tables are empty because there are no cifs or nfs sessions for this netapp".format(netappcluster)) if options.debug else None
      return "Failed"


#Do not forget to create a healthchecklog file under /var/log with the below name. Either give the file permissions to the user running the script or run the script as sudo
if __name__ == '__main__':
   choices = tuple(config.getallnetapps())
   logging.basicConfig(filename="/var/log/netapp_healthcheck.log", level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S', format="[%(asctime)s] %(filename)s in %(funcName)s(), line %(lineno)d (%(levelname)s): %(message)s")
   logging.getLogger("requests").setLevel(logging.WARNING)
   logging.getLogger("pandas").setLevel(logging.WARNING)
   logging.getLogger("optparse").setLevel(logging.WARNING)
   logging.getLogger("socket").setLevel(logging.WARNING)
   logging.getLogger("sys").setLevel(logging.WARNING)
   logger = logging.getLogger(__name__)
   usage = "usage: healthcheck.py arg1 arg2 arg3 etc..\n       You can choose one or many arguments"
   parser = OptionParser(usage)
   parser.add_option("--debug", dest="debug", action="store_true",
      help="Prints to screen the major milestones of each function.")
   parser.add_option("--netapp",type="choice",choices=choices,
      dest="netapp",help="Returns billing data for netapp of choice. Ex: 'python3 healthcheck.py --netapp netappname'. Choose one netapp from this list:{}".format(choices))
   parser.add_option("-t", "--timeit", dest="timeit", action="store_true",
      help="Prints out the time taken for each part of this script")
   parser.add_option("--recipient", dest="recipient", default="emailaddress",
      help="Sends the billing data as email to the recipient of choice. Default is emailaddress")
   (options, args) = parser.parse_args()
   start_time = time.time()
   print("...started....\n...running...\n...breath in...breath out..") if not options.debug and not options.timeit else ''
   netapplist = []
   logger.info("------ SCRIPT START ------")
   if options.netapp:
      netapplist.append(options.netapp)
      logger.info("Single run initiated against netapp: '%s'" % str(netapplist))
   else:
      netapplist = config.getallnetapps()
      logger.info("Full run initated against netapps : '%s'" % str(netapplist))
   html_headtail(netapplist)
   logger.info("------ SCRIPT END ------")
   print("Script ended. Total time taken = {} seconds".format(round((time.time() - start_time),2)))
