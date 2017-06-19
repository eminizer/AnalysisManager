import subprocess

#Sample class (for all samples of all types)
class Sample(object) :

	def __init__(self) :
		self._group = ''
		self._shortname = ''
		self._fullname = ''
		self._eosdir = ''
		self._xSec = 1.

	def populateFromLists(self,cats,dats,uname) :
		xseci = cats.index('xsec')
		self._group = dats[cats.index('group')]
		self._shortname = dats[cats.index('sname')]
		self._fullname = dats[cats.index('fname')]
		self._eosdir = '/store/user/'+uname+'/'+dats[cats.index('eosdir')]+'/'+self._fullname
		self._xSec = float(dats[cats.index('xsec')])
		self._kfactor = float(dats[cats.index('kfactor')])
		self._nrecojobs = int(dats[cats.index('nrecojobs')])
		#print 'Added sample with shortname %s in group %s to analysis'%(self._shortname,self._group)

	#make a list of tuples that's file URLs and sizes
	def getRawFileTuples(self) :
		raw_file_tuples = []
		#first subdir is the outputdatasettag from crab, but only for MC
		outputdatasettag = ''
		if self._group.lower()!='singleel' and self._group.lower()!='singlemu' :
			outputdatasettags = subprocess.check_output('eos root://cmseos.fnal.gov ls '+self._eosdir+' | grep -v ".root"',shell=True).split('\n')[:-1]
			if len(outputdatasettags)<1 :
				print 'ERROR! could not find any directories that are outputdatasettags in eos directory %s'%self._eosdir
				return
			outputdatasettag = outputdatasettags[0]
			if len(outputdatasettags)>1 :
				for outputdatasettag in outputdatasettags :
					check = raw_input('		WARNING: Multiple outputdatasettags found for sample %s, type "yes" to use outputdatasettag %s or enter to skip this outputdatasettag --->'%(self._shortname,outputdatasettag))
					if check.lower()=='yes' : break
		#second subdir is the timestamp of the run
		timestamps = subprocess.check_output('eos root://cmseos.fnal.gov ls '+self._eosdir+'/'+outputdatasettag+' | grep -v ".root"',shell=True).split('\n')[:-1]
		timestamp = timestamps[0]
		if len(timestamps)>1 :
			for timestamp in timestamps :
				check = raw_input('		WARNING: Multiple timestamps found for sample %s, type "yes" to use timestamp %s or enter to skip this timestamp --->'%(self._shortname,timestamp))
				if check.lower()=='yes' : break
		#subdirs off the base URL are the splits based on number of files; we want all of those
		orgdirs = subprocess.check_output('eos root://cmseos.fnal.gov ls '+self._eosdir+'/'+outputdatasettag+'/'+timestamp+' | grep -v ".root"',shell=True).split('\n')[:-1]
		for orgdir in orgdirs :
			file_entries = subprocess.check_output('eos root://cmseos.fnal.gov ls -l '+self._eosdir+'/'+outputdatasettag+'/'+timestamp+'/'+orgdir+' | grep ".root"',shell=True).split('\n')[:-1]
			#for every one of the files in this directory
			for file_entry in file_entries :
				fes = file_entry.split()
				thisfileurl = 'root://cmseos.fnal.gov/'+self._eosdir+'/'+outputdatasettag+'/'+timestamp+'/'+orgdir+'/'+fes[8]
				raw_file_tuples.append((thisfileurl,float(fes[4]))) #append a tuple with its URL, size in bytes
		return raw_file_tuples

	#get the list of aggregated files
	def getAggregatedFileTuples(self) :
		try :
			filelist = subprocess.check_output('eos root://cmseos.fnal.gov ls -l '+self._eosdir+' | grep "aggregated"',shell=True).split('\n')[:-1]
		except subprocess.CalledProcessError, e:
			filelist = []
		returnlist = []
		for item in filelist :
			isplit = item.split()
			returnlist.append((self.getEOSBaseURL()+'/'+isplit[8],float(isplit[4])))
		return returnlist

	def setGroup(self,g) :
		self._group=g
	def setShortName(self,s) :
		self._shortname=s
	def setLongName(self,l) :
		self._longname=l
	def getGroup(self) :
		return self._group
	def getShortName(self) :
		return self._shortname
	def getEOSDir(self) :
		return self._eosdir
	def getEOSBaseURL(self) :
		return subprocess.check_output('xrdfs root://cmseos.fnal.gov/ ls -u '+self._eosdir,shell=True).split('\n')[0][:len('root://xxx.xxx.xxx.xxx:xxxx/')]+self._eosdir
	def getXSec(self) :
		return self._xSec
	def getKFactor(self) :
		return self._kfactor
	def getNRecoJobs(self) :
		return self._nrecojobs


