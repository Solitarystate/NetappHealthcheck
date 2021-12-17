try:
    import json
    import yaml
    import requests
    import pandas as pd
    import config
    import re
    import logging
    from optparse import OptionParser
except Exception as e:
   print("Issue:    Healthcheck script failed to import necessary modules\nFilename:    healthcheck.py\nReason:    This could be a RuntimeError that occured while importing a dependency module for this script to run. See API error to know which module had issues\n API Error:     {}".format(str(e)))

###########################################
## Necessary settings and error handling ##
###########################################

pd.options.display.float_format = "{:.2f}".format
requests.packages.urllib3.disable_warnings()
api_auth = config.auth('api')

logging.basicConfig(filename="/var/log/netapp_healthcheck.log", level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S', format="[%(asctime)s] %(filename)s in %(funcName)s(), line %(lineno)d (%(levelname)s): %(message)s")
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("pandas").setLevel(logging.WARNING)
logging.getLogger("optparse").setLevel(logging.WARNING)
logging.getLogger("socket").setLevel(logging.WARNING)
logging.getLogger("sys").setLevel(logging.WARNING)
usage = "usage: healthcheck.py arg1 arg2 arg3 etc..\n       You can choose one or many arguments"
parser = OptionParser(usage)
choices = tuple(config.getallnetapps())
parser.add_option("--debug", dest="debug", action="store_true",
   help="Prints to screen the major milestones of each function.")
parser.add_option("--netapp",type="choice",choices=choices,
   dest="netapp",help="Returns billing data for netapp of choice. Ex: 'python3 healthcheck.py --netapp netappname'. Choose one netapp from this list:{}".format(choices))
parser.add_option("-t", "--timeit", dest="timeit", action="store_true",
   help="Prints out the time taken for each part of this script")
parser.add_option("--recipient", dest="recipient", default="emailaddress",
   help="Sends the billing data as email to the recipient of choice. Default is emailaddress")
(options, args) = parser.parse_args()

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
svmurl = "https://{}/api/svm/svms?fields=name"
routeshowurl = "https://{}/api/network/ip/routes?fields=*"
diskshowurl = "https://{}/api/storage/disks?fields=name,usable_size,shelf,bay,type,container_type,aggregates,self_encrypting,home_node,state,firmware_version,model&order_by=name"
dnsurl = "https://{}/api/private/cli/dns/check?vserver={}&fields=status,status_details"
connurl = "https://{}/api/private/cli/network/connections/active?service={}&vserver={}&fields=cid,vserver,lif-name,local-address,local-port,remote-ip,remote-host,remote-port,proto,lifid,service,lru,blocks-lb"


########################################
## Global variables for Summary table ##
########################################

#######################
## List placeholders ##
#######################

affectednetworkinterface = []
affectednetworkport = []
spaceissueaggr = []
affectedaggr = []
affectedvol = []
volnotrw_prod = []
totalroutes = []
routeinfo = []
affecteddisk = []
summarylist = []
rwvollist_curr_netapp = []
svmname_dns = []
dns_unconfig_svm = []
affecteddns = []
dns_config_status = []

##########################################
# Creation of consolidated Summary Table #
##########################################
def textsummary(netappcluster):
   gapstr = '<br>'
   currentnetapptype = "This is a Production Netapp system." if svmtype(netappcluster) == 'prod' else "This is a Backup Netapp system"
   summary = "<h2>Summary</h2>" + "<p>High level findings. Some items that might need your attention</p>"
   summary += "<li>"+currentnetapptype+"</li>"
   if len(affectednetworkport) > 0:
      summary += "<li>Some network ports may not be online or as in expected state. Those are: "+str(affectednetworkport)+"</li>"
   if len(spaceissueaggr) > 0:
      summary += "<li>Aggregate(s) that could have high consumption are: "+str(spaceissueaggr)+"</li>"
   if len(affectedaggr) > 0:
      summary += "<li>Aggregate(s) that might be not be in online state are: "+str(affectedaggr)+"</li>"
   if len(affectedvol) > 0:
      summary += "<li>"+str(affectedvol[0])+"</li>"
   if len(volnotrw_prod) > 0:
      summary += "<li>Volumes not in RW state: "+str(volnotrw_prod)+"</li>"
   if len(totalroutes) > 0:
      summary += "<li>This netapp has "+str(len(totalroutes))+" route(s) in total. </li>"
   if len(routeinfo) > 0:
      summary += "<li>"+str(''.join(routeinfo))+"</li>"
   if len(affecteddisk) > 0:
      summary += "<li>"+str(affecteddisk[0])+"</li>"
   if len(rwvollist_curr_netapp) > 0:
      summary += "<li>The volumes configured as RW on this netapp are: "+str(rwvollist_curr_netapp)+"</li>"
   if len(dns_unconfig_svm) > 0:
      summary += "<li>SVM(s) with no DNS configuration : "+str(dns_unconfig_svm)+"</li>"
   if len(affecteddns) > 0:
      summary += "<li>SVM(s) with bad DNS configuration state : "+str(list(set(affecteddns)))+"</li>"
   if len(dns_config_status) > 0:
      summary += "<li>"+str(dns_config_status[0])+"</li>"
      summary += gapstr
   return "<ul>"+summary+"</ul>"

######################
## Helper functions ##
######################
def ConvertSectoDay(n): 
    days = n // (24 * 3600)
    n = n % (24 * 3600)
    hours = n // 3600
    n %= 3600
    minutes = n // 60
    n %= 60
    seconds = n
    output = (str(days)+" days", str(hours)+" hours", str(minutes)+" minutes", str(seconds)+" seconds")
    return output

def sizeconvertor(value,convertto):
   if convertto == "GB":
      return round(int(value)/1073741824, 1)
   elif convertto == "TB":
      return round(int(value)/1099511627776, 1)
   else:
      return None

def thresholdchecker(available,total):
   if round((available/total),2) <= 0.10:
      return "critical"
   elif round((available/total),2) <= 0.14:
      return "amber"
   else:
      return "good"

def returnNotMatches(a, b):
   return [[x for x in a if x not in b], [x for x in b if x not in a]]

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


###################
# version command #
#############################################################
## Shows the version of the netapp at the time of this run ##
#############################################################
def version(netappcluster):
   url = clusterurl.format(netappcluster)
   ver = []
   try:
      resp = requests.get(url=url,auth=api_auth, verify=False)
      version = resp.json()["version"]["full"]
      ver.append('version')
      ver.append(version)
      vers = pd.DataFrame([ver] ,columns =['Command', 'Version_Number'])
      vers.index += 1
      vers = vers.to_html()
      vers = vers.replace('\n','')
   except Exception as e:
      print(e)
   return vers


def nodeshow(netappcluster):
   "Returns command outputs for both node show and storage failover command."
   url = nodeshowurl.format(netappcluster)
   tuplecontent = ()
   nststr = 0
   nodecol = []
   nodelist = []
   location = []
   model = []
   status = []
   giveback_state = []
   takeover_state = []
   partnernode = []
   uptime = []
   headings = ['Node_Name', 'Location', 'Model', 'State', 'Uptime','Node_Giveback_State', 'Node_Takeover_State', 'Partner_Node']
   try:
      resp = requests.get(url=url,auth=api_auth, verify=False)
      nodes = resp.json()
      for nodenumber in range(len(nodes["records"])):
         nodelist.append(nodes['records'][nodenumber]["name"])
         location.append(nodes['records'][nodenumber]["location"])
         model.append(nodes['records'][nodenumber]["model"])
         status.append(nodes['records'][nodenumber]["state"])
         uptime.append(ConvertSectoDay(nodes['records'][nodenumber]["uptime"]))
         giveback_state.append(nodes['records'][nodenumber]['ha']['giveback']['state'])
         takeover_state.append(nodes['records'][nodenumber]['ha']['takeover']['state'])
         partnernode.append(nodes['records'][nodenumber]['ha']['partners'][0]['name']) 
      for i in range(len(nodelist)) : nodecol.append('Node'+str(i+1))
      nodeshowtable = pd.DataFrame([nodelist,location,model,status,uptime,giveback_state,takeover_state,partnernode] ,columns =nodecol)
      tuplecontent = nodelist,location,model,status,uptime,giveback_state,takeover_state,partnernode
      nodeshowtable.insert(loc=0,column='Heading',value=headings)
      nodeshowtable, nodeshowtable.columns = nodeshowtable[1:], nodeshowtable.iloc[0]
      nodeshowtable = nodeshowtable.transpose()
      newheader = nodeshowtable.iloc[0]
      nodeshowtable.columns = newheader
      nodeshowtable = nodeshowtable[1:]
      nodeshowtablestr = nodeshowtable.to_html()
      nststr = nodeshowtablestr.replace('\n','')
   except Exception as e:
      print(e)
   return nststr,tuplecontent


def networkinterfaceshow(netappcluster):
   "Returns command outputs for network interface show command."
   url = networkinterfaceurl.format(netappcluster)
   clusurl = clusterurl.format(netappcluster)
   systemvserverpattern = re.compile(r'(169.\d{1,3}\.\d{1,3}\.\d{1,3})')
   nwdf = pd.DataFrame()
   tuplecontent = ()
   lif = []
   state = []
   svm = []
   is_home = []
   current_node = []
   current_port = []
   homenode = []
   homeport = []
   headings = ['LIF_Name','State','SVM_Name','is_home','current_node','current_port','homenode','homeport']
   try:
      resp = requests.get(url=url,auth=api_auth, verify=False)
      interfaces = resp.json()
      clusterresp = requests.get(url=clusurl,auth=api_auth, verify=False)
      clusname = clusterresp.json()['name']
      for lifs in range(len(interfaces["records"])):
         lif.append(interfaces['records'][lifs]["name"])
         state.append(interfaces['records'][lifs]["state"])
         is_home.append(interfaces['records'][lifs]["location"]['is_home'])
         current_node.append(interfaces['records'][lifs]["location"]['node']["name"])
         current_port.append(interfaces['records'][lifs]["location"]["port"]["name"])
         homenode.append(interfaces['records'][lifs]["location"]['home_node']["name"])
         homeport.append(interfaces['records'][lifs]["location"]['home_port']['name'])
         if 'svm' in interfaces['records'][lifs]:
            svm.append(interfaces['records'][lifs]["svm"]["name"])
         elif bool(re.match(systemvserverpattern, interfaces['records'][lifs]['ip']['address'])) == True and interfaces['records'][lifs]['ipspace']['name'] == "Cluster":
            svm.append('Cluster')
         elif bool(re.match(systemvserverpattern, interfaces['records'][lifs]['ip']['address'])) != True and interfaces['records'][lifs]['ipspace']['name'] == "Default":
            svm.append(clusname)
      nwdf = pd.DataFrame([lif,state,svm,is_home,current_node,current_port,homenode,homeport])
      tuplecontent = lif,state,svm,is_home,current_node,current_port,homenode,homeport
      nwdf.insert(loc=0,column='Heading',value=headings)
      nwdf, nwdf.columns = nwdf[1:], nwdf.iloc[0]
      nwdf = nwdf.transpose()
      newheader = nwdf.iloc[0]
      nwdf.columns = newheader
      nwdf = nwdf[1:]
      nwdfstr = nwdf.to_html()
      nwdfstr = nwdfstr.replace('\n','')
      if '<td>False' in nwdfstr:
         nwdfstr = nwdfstr.replace('<td>False</td>','<td style="background-color:#FF0000">False')
      if '<td>down' in nwdfstr.lower():
         nwdfstr = nwdfstr.replace('<td>down','<td style="background-color:#FF0000">down')
      else:
         pass
   except Exception as e:
      print(e)
   return nwdfstr,tuplecontent


##########################
# Aggregate show command #
##################################################################################
# Aggregates that have less than 13% total free capacity can be concern in the  ##
# upcoming days. Hence those will be highlighted as orange and less than 10%    ##
# will be highlighted in red                                                    ##
##################################################################################
def aggrstatus(netappcluster):
   "Returns command outputs for aggr show command."
   url = aggrshowurl.format(netappcluster)
   aggrdf = pd.DataFrame()
   aggrstr = ""
   tuplecontent = ()
   aggrname = []
   state = []
   curr_node = []
   home_node = []
   used = []
   total = []
   available = []
   diskcount = []
   raidtype = []
   disktype = []
   plex = []
   avail_cap_below_amber_thresh = []
   avail_cap_below_critcal_thresh = []
   headings = ['Aggregate_Name','State','Current_Node','Home_Node','Total_Size(TB)','Used_Size(TB)','Available_Size(TB)','Disk_Count','Raid_Type','Disk_Type']
   try:
      resp = requests.get(url=url,auth=api_auth, verify=False)
      aggrs = resp.json()
      for aggr in range(len(aggrs["records"])):
         aggrname.append(aggrs["records"][aggr]['name'])
         state.append(aggrs["records"][aggr]['state'])
         curr_node.append(aggrs["records"][aggr]['node']['name'])
         home_node.append(aggrs["records"][aggr]['home_node']['name'])
         used.append(sizeconvertor(aggrs["records"][aggr]['space']['block_storage']['used'],convertto="TB"))
         total.append(sizeconvertor(aggrs["records"][aggr]['space']['block_storage']['size'],convertto="TB"))
         available.append(sizeconvertor(aggrs["records"][aggr]['space']['block_storage']['available'],convertto="TB"))
         if aggrs["records"][aggr]['state'] == "online":
            spaceissueaggr.append(aggrname[-1]) if thresholdchecker(available[-1],total[-1]) != "good" else ''
            avail_cap_below_amber_thresh.append(available[-1]) if thresholdchecker(available[-1],total[-1]) == "amber" else ''
            avail_cap_below_critcal_thresh.append(available[-1]) if thresholdchecker(available[-1],total[-1]) == "critical" else ''
         else:
            pass
         diskcount.append(aggrs["records"][aggr]['block_storage']['primary']['disk_count'])
         raidtype.append(aggrs["records"][aggr]['block_storage']['primary']['raid_type'])
         disktype.append(aggrs["records"][aggr]['block_storage']['primary']['disk_type'])
      aggrdf = pd.DataFrame([aggrname,state,curr_node,home_node,total,used,available,diskcount,raidtype,disktype])
      tuplecontent = aggrname,state,curr_node,home_node,total,used,available,diskcount,raidtype,disktype
      aggrdf.insert(loc=0,column='Heading',value=headings)
      aggrdf, aggrdf.columns = aggrdf[1:], aggrdf.iloc[0]
      aggrdf = aggrdf.transpose()
      newheader = aggrdf.iloc[0]
      aggrdf.columns = newheader
      aggrdf = aggrdf[1:]
      aggrstr = aggrdf.to_html()
      aggrstr = aggrstr.replace('\n','')
      aggrstr = aggrthresholdverify(aggrstr,"amber",avail_cap_below_amber_thresh)
      aggrstr = aggrthresholdverify(aggrstr,"critical",avail_cap_below_critcal_thresh)
      aggrstr = aggrstatusverify(aggrstr)
   except Exception as e:
      print(e)
   return aggrstr,tuplecontent


def aggrthresholdverify(string,thresholdtype,thresholdlist):
   if thresholdtype == "amber":
      for item in range(len(thresholdlist)):
         if str(thresholdlist[item]) in string:
            string = string.replace('<td>'+str(thresholdlist[item]),'<td style="background-color:#FF8C00">'+str(thresholdlist[item]))
   elif thresholdtype == "critical":
      for item in range(len(thresholdlist)):
         if str(thresholdlist[item]) in string:
            string = string.replace('<td>'+str(thresholdlist[item]),'<td style="background-color:#FF0000">'+str(thresholdlist[item]))
   return string


def aggrstatusverify(string):
   aggrstates = ["Offline","Restricted","Creating","Destroying","Failed","Frozen","Inconsistent","Iron restricted","Mounting","Partial","Quiescing","Quiesced","Reverted","Unmounted","Unmounting","Unknown"]
   for aggrstate in aggrstates:
      if aggrstate.lower() in string.lower():
         string = string.replace('<td>'+str(aggrstate),'<td style="background-color:#FF0000">'+str(aggrstate))
         affectedaggr.append("One of the aggregates is not in online state. Check the aggregate table below")
   return string



#############################
# Network Port show command #
##########################################################################################
# Since network ports that are not enabled is not something that needs to be monitored.  #
# Those will be excluded from the healthcheck. But if something is enabled but the state #
# is down, that will be highlighted in red color                                         #
##########################################################################################
def networkportshow(netappcluster):
   "Returns command outputs for network port show command."
   url = networkporturl.format(netappcluster)
   portdf = pd.DataFrame()
   portstr = ""
   tuplecontent = ()
   portname = []
   state = []
   broadcastdomain = []
   ipspace = []
   enabled = []
   mtu = []
   speed = []
   headings = ['Port_Name','State','Broadcast_Domain','IPSpace','Enabled','MTU','Speed']
   affectednetworkport.clear()
   try:
      resp = requests.get(url=url,auth=api_auth, verify=False)
      ports = resp.json()
      for port in range(len(ports["records"])):
         if ports["records"][port]['enabled'] == True:
            endpoint = ports["records"][port]
            portname.append(endpoint['name'])
            state.append(endpoint['state'])
            broadcastdomain.append(endpoint['broadcast_domain']['name']) if 'broadcast_domain' in endpoint else broadcastdomain.append('-')
            ipspace.append(endpoint['broadcast_domain']['ipspace']['name']) if 'broadcast_domain' in endpoint else ipspace.append('Default')
            enabled.append(endpoint['enabled'])
            mtu.append(endpoint['mtu']) if 'mtu' in endpoint else mtu.append('-')
            speed.append(endpoint['speed']) if 'speed' in endpoint else speed.append('-')
            affectednetworkport.append(endpoint['name']) if endpoint['state'].lower() != 'up' else ''
         portdf = pd.DataFrame([portname,state,broadcastdomain,ipspace,enabled,mtu,speed])
         tuplecontent = portname,state,broadcastdomain,ipspace,enabled,mtu,speed
         portdf.insert(loc=0,column='Heading',value=headings)
         portdf, portdf.columns = portdf[1:], portdf.iloc[0]
         portdf = portdf.transpose()
         newheader = portdf.iloc[0]
         portdf.columns = newheader
         portdf = portdf[1:]
         portstr = portdf.to_html()
         portstr = portstr.replace('\n','')
         if '<td>down</td>' in portstr:
            portstr = portstr.replace('<td>down</td>','<td style="background-color:#FF0000">down</td>')
         elif '<td>off</td>' in portstr:
            portstr = portstr.replace('<td>off','<td style="background-color:#FF0000">off')
         else:
            pass
   except Exception as e:
      print(e)
   return portstr,tuplecontent


#######################
# Volume show command #
##########################################################################################
# Since network ports that are not enabled is not something that needs to be monitored.  #
# Those will be excluded from the healthcheck. But if something is enabled but the state #
# is down, that will be highlighted in red color                                         #
##########################################################################################
def volshow(netappcluster):
   "Returns command outputs for vol show command."
   url = volshowurl.format(netappcluster,'*')
   voldf = pd.DataFrame()
   volstr = ""
   tuplecontent = ()
   volname = []
   state = []
   aggrname = []
   volumetype = []
   volstyle = []
   total = []
   used = []
   available = []
   avail_cap_below_amber_thresh = []
   avail_cap_below_critcal_thresh = []
   netapptype = svmtype(netappcluster)
   headings = ['Volume_Name','State','Aggregate_Name','Vol_Type','Vol_Style','Total_Size(GB)','Used_Size(GB)','Available_Size(GB)']
   try:
      resp = requests.get(url=url,auth=api_auth, verify=False)
      vols = resp.json()
      for vol in range(len(vols["records"])):
         tmplist = []
         endpoint = vols["records"][vol]
         volname.append(endpoint['name'])
         for items in range(len(endpoint['aggregates'])):
            tmplist.append(endpoint['aggregates'][items]['name'])
         aggrname.append(" , ".join(tmplist))
         volumetype.append(endpoint['type'])
         volnotrw_prod.append(endpoint['name']) if netapptype == 'prod' and endpoint['type'].lower() != 'rw' and endpoint['type'].lower() != 'ls' else ''
         volstyle.append(endpoint['style'])
         if 'state' in endpoint:
            state.append(endpoint['state'])
            if endpoint['state'] == "online":
               total.append(sizeconvertor(endpoint['space']['size'],convertto="GB"))
               used.append(sizeconvertor(endpoint['space']['used'],convertto="GB")) if 'used' in endpoint['space'] else used.append(0)
               available.append(sizeconvertor(endpoint['space']['available'],convertto="GB")) if 'available' in endpoint['space'] else available.append(0)
               avail_cap_below_amber_thresh.append(endpoint['name']) if thresholdchecker(available[-1],total[-1]) == "amber" else ''
               avail_cap_below_critcal_thresh.append(endpoint['name']) if thresholdchecker(available[-1],total[-1]) == "critical" else ''
            else:
               pass
      voldf = pd.DataFrame([volname,state,aggrname,volumetype,volstyle,total,used,available])
      tuplecontent = volname,state,aggrname,volumetype,volstyle,total,used,available
      voldf.insert(loc=0,column='Heading',value=headings)
      voldf, voldf.columns = voldf[1:], voldf.iloc[0]
      voldf = voldf.transpose()
      newheader = voldf.iloc[0]
      voldf.columns = newheader
      voldf = voldf[1:]
      volstr = voldf.to_html()
      volstr = volstr.replace('\n','')
      volstr = volthresholdverify(volstr,"amber",avail_cap_below_amber_thresh)
      volstr = volthresholdverify(volstr,"critical",avail_cap_below_critcal_thresh)
      volstr = volstatusverify(volstr)
      volstr = voltypeverify(netappcluster,volstr,volumetype)
   except Exception as e:
      print(e)
   return volstr,tuplecontent

def volthresholdverify(string,thresholdtype,thresholdlist):
   if thresholdtype == "amber":
      for item in range(len(thresholdlist)):
         if str(thresholdlist[item]) in string:
            string = string.replace('<th>'+str(thresholdlist[item]),'<th style="background-color:#FF8C00">'+str(thresholdlist[item]))
   if thresholdtype == "critical":
      for item in range(len(thresholdlist)):
         if str(thresholdlist[item]) in string:
            string = string.replace('<th>'+str(thresholdlist[item]),'<th style="background-color:#FF0000">'+str(thresholdlist[item]))
   return string

def voltypeverify(netappcluster,string,typelist):
   rwvollist_curr_netapp.clear()
   netappvoltype = voltype(netappcluster,rwvollist="True")
   netappsvmtype = svmtype(netappcluster)
   if netappvoltype[0] == 'prod' or netappsvmtype == 'prod':
      for vol_type in typelist:
         if vol_type.lower() != 'rw' and vol_type.lower() != 'ls':
            string = string.replace('<td>'+str(vol_type.lower()),'<td style="background-color:#FF0000">'+str(vol_type.lower()))
   elif netappsvmtype == 'backup':
      for eachvol in netappvoltype[1]:
         rwvollist_curr_netapp.append(eachvol)
   else:
      string = string
   return string

def volstatusverify(string):
   volstates = ["Offline","Restricted","Quiesced","None"]
   affectedvol.clear()
   for volstate in volstates:
      if volstate.lower() in string.lower():
         string = string.replace('<td>'+str(volstate),'<td style="background-color:#FF0000">'+str(volstate))
         affectedvol.append("One or more volumes are not in online state. Check the volume table below")
   return string

######################
# Route show command #
##########################################################################################
# There is nothing good or bad in the outcome of routeshow. So we fetch all the routes  ##
# configured on a given netapp and inform how many SVMs have routes and how many don't. ##
##########################################################################################
def routeshow(netappcluster):
   "Returns command outputs for route show command."
   routeurl = routeshowurl.format(netappcluster)
   routedf = pd.DataFrame()
   routestr = ""
   tuplecontent = ()
   svms = []
   destination = []
   gateway = []
   ipspace = []
   headings = ['SVM_Name','Destination','Gateway','IPSpace']
   totalroutes.clear()
   try:
      resp = requests.get(url=routeurl,auth=api_auth, verify=False)
      routes = resp.json()
      for route in range(len(routes["records"])):
         endpoint = routes["records"][route]
         svms.append('Cluster') if endpoint['scope'] == 'cluster' else svms.append(endpoint['svm']['name'])
         destination.append(endpoint['destination']['address']+"/"+endpoint['destination']['netmask'])
         gateway.append(endpoint['gateway'])
         ipspace.append(endpoint['ipspace']['name'])
         totalroutes.append(endpoint['gateway'])
      routedf = pd.DataFrame([svms,destination,gateway,ipspace])
      tuplecontent = svms,destination,gateway,ipspace
      routedf.insert(loc=0,column='Heading',value=headings)
      routedf, routedf.columns = routedf[1:], routedf.iloc[0]
      routedf = routedf.transpose()
      newheader = routedf.iloc[0]
      routedf.columns = newheader
      routedf = routedf[1:]
      routestr = routedf.to_html()
      routestr = routestr.replace('\n','')
   except Exception as e:
      print(e)   
   return routestr,tuplecontent

######################
# Disk show command #
##############################################################################################
# The state of disks, especially if something is broken or not in online state is identified #
# in this function. Otherwise a complete list of all the disks on the given netapp with all  #
# relevant information is published in the table output                                      #
##############################################################################################
def diskshow(netappcluster):
   "Returns command outputs for disk show command."
   diskurl = diskshowurl.format(netappcluster)
   diskdf = pd.DataFrame()
   diskstr = ""
   tuplecontent = ()
   diskname = []
   state = []
   size = []
   shelf = []
   bay = []
   disktype = []
   containertype = []
   aggregates = []
   seds = []
   homenode = []
   firmwareversion = []
   model = []
   headings = ['Disk_Name','State','Usable_Size(TB)','Shelf','Bay','Disk_Type','Container_Type','Aggregate(s)','Home_Node','Firmware_Version','Model','Self_Encrypting_Disks']
   try:
      resp = requests.get(url=diskurl,auth=api_auth, verify=False)
      disks = resp.json()
      for disk in range(len(disks["records"])):
         tmplist = []
         endpoint = disks["records"][disk]
         diskname.append(endpoint['name'])
         state.append(endpoint['state']) if 'state' in endpoint else state.append('No State Info')
         size.append(sizeconvertor(endpoint['usable_size'],convertto="TB")) if 'usable_size' in endpoint else size.append('No Usable Size Info')
         if 'shelf' in endpoint:
            try:
               url = "https://{}/api/storage/shelves/".format(netappcluster)
               shelfresp = requests.get(url=url+str(endpoint['shelf']['uid']),auth=api_auth, verify=False).json()
               shelf.append(shelfresp['id'])
            except Exception as e:
               print(e)
         else:
            shelf.append('No Shelf Info')
         bay.append(endpoint['bay']) if 'bay' in endpoint else bay.append('No Bay Info')
         disktype.append(endpoint['type']) if 'type' in endpoint else bay.append('No Disk_Type Info')
         containertype.append(endpoint['container_type']) if 'type' in endpoint else bay.append('No Container_Type Info')
         if 'state' in endpoint:
            if endpoint['state'] == 'spare':
               tmplist.append('spare')
            elif 'aggregates' in endpoint:
               for items in range(len(endpoint['aggregates'])):
                  tmplist.append(endpoint['aggregates'][items]['name'])
            elif endpoint['state'] != 'spare' and endpoint['type'] == 'vmdisk':
               tmplist.append('No spare needed')
            else:
               pass
         aggregates.append(" , ".join(tmplist))
         homenode.append(endpoint['home_node']['name'])
         firmwareversion.append(endpoint['firmware_version'])
         model.append(endpoint['model'])
         seds.append(endpoint['self_encrypting'])
      diskdf = pd.DataFrame([diskname,state,size,shelf,bay,disktype,containertype,aggregates,homenode,firmwareversion,model,seds])
      tuplecontent = diskname,state,size,shelf,bay,disktype,containertype,aggregates,seds,homenode,firmwareversion,model
      diskdf.insert(loc=0,column='Heading',value=headings)
      diskdf, diskdf.columns = diskdf[1:], diskdf.iloc[0]
      diskdf = diskdf.transpose()
      newheader = diskdf.iloc[0]
      diskdf.columns = newheader
      diskdf = diskdf[1:]
      diskstr = diskdf.to_html()
      diskstr = diskstr.replace('\n','') 
      diskstr = diskshowverify(diskstr)
   except Exception as e:
      print(e)
   return diskstr,tuplecontent


def diskshowverify(string):
   diskstates = ['broken','copy', 'orphan','pending']
   global affecteddisk
   affecteddisk.clear()
   for diskstate in diskstates:
      if diskstate.lower() in string.lower():
         print(diskstate + "found in html string") if options.timeit else None
         logging.info(diskstate + "found in html string") if options.debug else None
         string = string.replace('<td>'+str(diskstate),'<td style="background-color:#FF8C00">'+str(diskstate))
         affecteddisk.append("One or more disks is not in present state. Check the disk show table below")
   return string


#################################
# dns check -vserver <> command #
##############################################################################################
# dns check command checks the dns connectivity status against the pre-registered dns server #
# per vserver. It reports back if the dns connectivity is ok or not.                         #
##############################################################################################
def dnscheck(netappcluster):
   svmlisturl = svmurl.format(netappcluster)
   svmresp = requests.get(url=svmlisturl,auth=api_auth, verify=False)
   svmnames = svmresp.json()
   dnsdf = pd.DataFrame()
   tuplecontent = ()
   dnsstr = ""
   global svmname_dns
   svmname_dns.clear()
   dns_svmname = []
   dns_name = []
   dns_status = []
   dns_statusdetails = []
   global affecteddns
   affecteddns.clear()
   headings = ['SVM_Name','Name_Server','Status','Details']
   try:
      for svmname in range(len(svmnames["records"])):
         svmname_dns.append(svmnames["records"][svmname]['name'])
         dnscheckurl = dnsurl.format(netappcluster,svmnames["records"][svmname]['name'])
         dnsnames = requests.get(url=dnscheckurl,auth=api_auth, verify=False).json()
         for dnsname in range(len(dnsnames["records"])):
            dns_svmname.append(dnsnames["records"][dnsname]['vserver'])
            dns_name.append(dnsnames["records"][dnsname]['name_server'])
            dns_status.append(dnsnames["records"][dnsname]['status'])
            affecteddns.append(dnsnames["records"][dnsname]['vserver']) if dnsnames["records"][dnsname]['status'] != 'up' else ''
            dns_statusdetails.append(dnsnames["records"][dnsname]['status_details'])
      dnsdf = pd.DataFrame([dns_svmname,dns_name,dns_status,dns_statusdetails])
      tuplecontent = dns_svmname,dns_name,dns_status,dns_statusdetails
      dnsdf.insert(loc=0,column='Heading',value=headings)
      dnsdf, dnsdf.columns = dnsdf[1:], dnsdf.iloc[0]
      dnsdf = dnsdf.transpose()
      newheader = dnsdf.iloc[0]
      dnsdf.columns = newheader
      dnsdf = dnsdf[1:]
      dnsstr = dnsdf.to_html()
      dnsstr = dnsstr.replace('\n','')
      dnsstr = dnsdowncheck(dnsstr)
   except Exception as e:
      print(e)
   return dnsstr,tuplecontent


def dnscheckverify(svmnamelist,tuplecontent):
   global dns_unconfig_svm
   global dns_config_status
   dns_unconfig_svm.clear()
   dns_config_status.clear()
   list_a = list(set(svmnamelist))
   list_b = list(set(tuplecontent[0]))
   if len(list_a) > len(list_b):
      dns_unconfig_svm = returnNotMatches(list_a,list_b)[0]
   if len(list_a) == len(list_b):
      dns_config_status.append("All SVMs have DNS configured on them")
   if len(dns_config_status) > 0 or len(dns_unconfig_svm) > 0:
      return "success"
   else:
      return "failed"

def dnsdowncheck(string):
   if 'down' in string.lower():
      string = string.replace('<td>down','<td style="background-color:#FF8C00">down')
   return string


####################################################
# network connections active show -service command #
##############################################################################################
# network connections active show command checks list of current active nfs connections for  #
# a given vserver. It captures all available information found in the command. Works for     #
# both NFS and CIFS sessions.                                                                #
##############################################################################################
def connshow(netappcluster,servicetype):
   condf = pd.DataFrame()
   topcondf = pd.DataFrame()
   constr = ""
   topconstr = ""
   tuplecontent = ()
   nodename = []
   cid = []
   vserver = []
   lif_name = []
   local_address = []
   remote_ip = []
   remote_host = []
   remote_port = []
   proto = []
   lifid = []
   service = []
   blocks_lb = []
   headings = ['Node_Name','Connection_ID','SVM_Name','LIF_Name','Local_Address','Remote_IP','Remote_host','Remote_Port','Protocol','LIF_ID','Service','Connection_Blocks_Load_Balance_Migrate']
   try:
      for svm in svmname_dns:
         connresp = requests.get(url=connurl.format(netappcluster,servicetype,svm),auth=api_auth, verify=False)
         conns = connresp.json()
         for con in range(len(conns['records'])):
            nodename.append(conns['records'][con]['node'])
            cid.append(conns['records'][con]['cid'])
            vserver.append(conns['records'][con]['vserver'])
            lif_name.append(conns['records'][con]['lif_name'])
            local_address.append(conns['records'][con]['local_address'])
            remote_ip.append(conns['records'][con]['remote_ip'])
            remote_host.append(conns['records'][con]['remote_host'])
            remote_port.append(conns['records'][con]['remote_port'])
            proto.append(conns['records'][con]['proto'])
            lifid.append(conns['records'][con]['lifid'])
            service.append(conns['records'][con]['service'])
            blocks_lb.append(conns['records'][con]['blocks_lb'])
      condf = pd.DataFrame([nodename,cid,vserver,lif_name,local_address,remote_ip,remote_host,remote_port,proto,lifid,service,blocks_lb])
      if condf.empty != True:
         tuplecontent = nodename,cid,vserver,lif_name,local_address,remote_ip,remote_host,remote_port,proto,lifid,service,blocks_lb
         condf.insert(loc=0,column='Heading',value=headings)
         condf, condf.columns = condf[1:], condf.iloc[0]
         condf = condf.transpose()
         newheader = condf.iloc[0]
         condf.columns = newheader
         condf = condf[1:]
         topcondf = condf.groupby('SVM_Name')['LIF_Name'].value_counts().groupby(level=0).head(5).sort_values(ascending=False).to_frame('Connection Count').reset_index().head(10)
         topcondf.index += 1
         constr = condf.to_html()
         topconstr = topcondf.to_html()
         constr = constr.replace('\n','')
         topconstr = topconstr.replace('\n','')
      else:
         constr = "nodata"
         tuplecontent = ("nodata")
         topconstr = "nodata"
   except Exception as e:
      print(e)
   return constr,tuplecontent,topconstr

