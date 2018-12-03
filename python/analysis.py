from sample import Sample
from action_functions import *

class Analysis(object) :

	def __init__(self,uname) :
		self._username = uname
		self._samplelist = []
		#add the actions to the dictionary
		self._possible_actions = {'haddntuples':haddNTuples}
		self._possible_actions['setuprecoruns']=setupRecoRuns
		self._possible_actions['runreco']=runReco
		self._possible_actions['findfailedjobs']=findFailedJobs
		self._possible_actions['haddrecofiles']=haddRecoFiles 
		self._possible_actions['skimrecofiles']=skimRecoFiles 
		self._possible_actions['skimhaddrecofiles']=skimHaddRecoFiles 

	#adds a sample to the analysis from one of the lines in the samples input file
	def addSampleFromFileInfo(self,cats,datal) :
		newSample = Sample()
		newSample.populateFromLists(cats,datal,self._username)
		self._samplelist.append(newSample)

	def doAction(self,sample,astring,dojec,donominal) :
		astring = astring.lower()
		if astring in self._possible_actions.keys()<0 :
			print 'ERROR! action %s not available'%(astring)
			return
		return self._possible_actions[astring](sample,dojec,donominal)

	def getSampleList(self) :
		return self._samplelist