import os
import subprocess
import glob
import multiprocessing
from ROOT import TFile

def haddNtuplesParallel(haddjob,sampleeosurl,dojec) :
	#the hadd command
	#print 'command has %d files to add'%(len(haddjob[1])) #DEBUG
	cmd = 'hadd '+haddjob[0]
	for f in haddjob[1] :
		cmd+=' '+f
	os.system(cmd)
	#the xrdcp command
	os.system('xrdcp '+haddjob[0]+' '+sampleeosurl+'/'+haddjob[0])
#hadd a sample's individual nTuple root files into a smaller number of files, each about 5GB, then copy back to EOS
def haddNTuples(sample,dojec) :
	print 'hadding nTuples for sample '+sample.getShortName()
	cwd = os.getcwd()
	#remove the aggregated files that might already be in the directory
	agg_filelist = sample.getAggregatedFileTuples()
	for agg_file in agg_filelist :
		subprocess.call('eos root://cmseos.fnal.gov rm '+agg_file[0][len('root://xxx.xxx.xxx.xxx:xxxx/'):],shell=True)
	#get the list of raw file tuples (URL,size)
	rawfile_tuples = sample.getRawFileTuples()
	#make the list of hadd jobs
	hadd_jobs = []
	size_counter = 0.;
	for rft in rawfile_tuples :
		if size_counter==0. or size_counter+rft[1]>5000000000. or len(hadd_jobs[len(hadd_jobs)-1][1])>=500 : #5GB or 500 file limit
		#	print 'size_counter is at %d'%(size_counter) #DEBUG
			hadd_jobs.append(['aggregated_'+sample.getShortName()+'_'+str(len(hadd_jobs))+'.root',[]])
			size_counter=0.
		size_counter+=rft[1]
		hadd_jobs[len(hadd_jobs)-1][1].append(rft[0])
	#execute the hadd jobs to aggregate files in this directory
	os.chdir(subprocess.check_output('echo $CMSSW_BASE',shell=True).rstrip('\n')+'/src/Analysis/AnalysisManager/test')
	os.system('mkdir '+sample.getShortName())
	os.chdir(sample.getShortName())
	procs = []
	for i in range(len(hadd_jobs)) :
		if len(procs)>5 :
			for proc in procs :
				proc.join()
			procs = []
		haddjob = hadd_jobs[i]
		p = multiprocessing.Process(target=haddNtuplesParallel,args=(haddjob,sample.getEOSBaseURL()))
		p.start()
		procs.append(p)
	for proc in procs :
		proc.join()
	#back up and remove
	os.chdir('..')
	os.system('rm -rf '+sample.getShortName())
	os.chdir(cwd)
	print 'Done.'

#setup runs of the reconstructor: regenerate the directories, scripts, input file and ana.listOfJobs
def setupRecoRuns(sample,dojec) :	
	name = sample.getShortName()
	print 'setting up reconstructor runs for sample '+name
	#first find out where we are right now so we can return here
	cwd = os.getcwd()
	#next move to the reconstructor test area
	os.chdir(subprocess.check_output('echo $CMSSW_BASE',shell=True).rstrip('\n')+'/src/Analysis/Reconstructor/test')
	#remove the old directory
	print '	removing old directory'
	os.system('rm -rf '+name)
	#make the new directory for the sample
	print '	making new directory'
	os.mkdir(name)
	os.chdir(name)
	#copy the cleanup and submit scripts from the reconstructor/test directory locally
	print '	copying scripts'
	os.system('cp ../cleanup.bash .')
	os.system('cp ../runMany* .')
	#make the input file for the sample
	print '	making new input file'
	agg_filelist = sample.getAggregatedFileTuples()
	total_filedata = 0.
	for item in agg_filelist :
		os.system('echo "'+item[0]+'" >> input.txt')
		total_filedata+=item[1]
	#make the list of jobs for the sample based on the amount of data
	njobs = sample.getNRecoJobs()
	if njobs==1 :
		njobs = int(total_filedata/2500000.) #one job per 2.5 MB (20,000 jobs for a 50GB sample)
	print '	making new listOfJobs; sample %s will have %d jobs (%d total)'%(name,njobs,njobs*5 if (dojec=='yes' and name.find('Run2016')==-1) else njobs)
	for i in range(njobs) :
		cmd = 'echo "python ./tardir/run_reconstructor.py --name '+sample.getShortName()+' --xSec '+str(sample.getXSec())
		cmd+= ' --kFac '+str(sample.getKFactor())+' --on_grid yes --n_jobs '+str(njobs)+' --i_job '+str(i)
		os.system(cmd+'" >> ana.listOfJobs')
		if dojec=='yes' and name.find('Run2016')==-1 :
			#And the JEC-wiggled jobs
			os.system(cmd+' --JES up" >> ana.listOfJobs_JES_up')
			os.system(cmd+' --JES down" >> ana.listOfJobs_JES_down')
			os.system(cmd+' --JER up" >> ana.listOfJobs_JER_up')
			os.system(cmd+' --JER down" >> ana.listOfJobs_JER_down')
	#return to the previous working directory
	os.chdir(cwd)
	print 'Done.'

#run the new runs of the reconstructor
def runReco(sample,dojec) :
	print 'submitting reconstructor jobs for sample '+sample.getShortName()
	#first find out where we are right now so we can return here
	cwd = os.getcwd()
	#next move to the reconstructor test area
	os.chdir(subprocess.check_output('echo $CMSSW_BASE',shell=True).rstrip('\n')+'/src/Analysis/Reconstructor/test')
	#change to the sample directory
	os.chdir(sample.getShortName())
	#tar up files, create proxy, create command file, submit jobs
	os.system('tar czvf tarball.tgz ./input.txt -C ../../python/ . -C ../test/other_input_files/ .')
	os.system('voms-proxy-init --voms cms')
	os.system('python ./runManySections.py --createCommandFile --addLog --setTarball=tarball.tgz ana.listOfJobs commands.cmd')
	os.system('python ./runManySections.py --submitCondor --noDeleteCondor commands.cmd')
	#and for the JEC wiggled jobs
	if dojec=='yes' :
		apps = ['JES_up','JES_down','JER_up','JER_down']
		for app in apps :
			if os.path.isfile('ana.listOfJobs_'+app) :
				print 'submitting jobs for '+app+' wiggled samples'
				os.system('python ./runManySections.py --createCommandFile --addLog --setTarball=tarball.tgz ana.listOfJobs_'+app+' commands_'+app+'.cmd')
				os.system('python ./runManySections.py --submitCondor --noDeleteCondor commands_'+app+'.cmd')
	#return to the previous working directory
	os.chdir(cwd)
	print 'Done.'

#find the jobs that failed in the sample's reco directory and make a new ana.listOfJobs with just those
def findFailedJobs(sample,dojec) :
	name = sample.getShortName()
	print 'finding failed jobs for sample '+name
	#first find out where we are right now so we can return here
	cwd = os.getcwd()
	#next move to the reconstructor run directory 
	os.chdir(subprocess.check_output('echo $CMSSW_BASE',shell=True).rstrip('\n')+'/src/Analysis/Reconstructor/test/'+name)
	#did this directory have JEC wiggled runs in it or not?
	includeJEC = len(glob.glob('*JES_up*'))>0 and len(glob.glob('*JES_down*'))>0 and len(glob.glob('*JER_up*'))>0 and len(glob.glob('*JER_down*'))>0  
	#get the list of all the root files, and the number of original jobs
	rootfilelist = glob.glob('*_tree.root')
	nJobs = int(os.popen('cat ana.listOfJobs_all | wc -l').read()) if len(glob.glob('ana.listOfJobs_all'))!=0 else int(os.popen('cat ana.listOfJobs | wc -l').read())
	#make a dictionary of the failed job numbers/which JEC wiggles they are
	failedjobnumbers = {'nom':[],'JES_up':[],'JES_down':[],'JER_up':[],'JER_down':[]}
	#first look for jobs that didn't return anything
	print 'len(rootfilelist)=%d, nJobs=%d'%(len(rootfilelist),nJobs)
	if (includeJEC and len(rootfilelist) < nJobs*5) or len(rootfilelist) < nJobs :
		#for each job number
		for i in range(nJobs) :
			theseRootFiles = glob.glob('*_'+str(i)+'_tree.root')
			if includeJEC :
				#there should be five files per job
				if len(theseRootFiles) < 5 :
					print 'Missing some output from job number '+str(i)+', checking which of the JEC wiggles it is'
					missingjobtypes = ['nom','JES_up','JES_down','JER_up','JER_down']
					for rfilename in theseRootFiles :
						if rfilename.find('JES')==-1 and rfilename.find('JER')==-1: missingjobtypes.pop(missingjobtypes.index('nom'))
						elif rfilename.find('JES_up')!=-1 : missingjobtypes.pop(missingjobtypes.index('JES_up'))
						elif rfilename.find('JES_down')!=-1 : missingjobtypes.pop(missingjobtypes.index('JES_down'))
						elif rfilename.find('JER_up')!=-1 : missingjobtypes.pop(missingjobtypes.index('JER_up'))
						elif rfilename.find('JER_down')!=-1 : missingjobtypes.pop(missingjobtypes.index('JER_down'))
					for jtype in missingjobtypes :
						failedjobnumbers[jtype].append(i)
			elif len(theseRootFiles)==0 :
				print 'Missing some output from job number '+str(i)
				failedjobnumbers['nom'].append(i)
	#now check the file sizes to find any that are abnormally small
	totalsize = 0.
	for rootfile in rootfilelist :
		totalsize+=os.path.getsize(rootfile)
	expected_contribution = totalsize/len(rootfilelist)
	for rootfile in rootfilelist :
		filesize = os.path.getsize(rootfile)
		if filesize/expected_contribution<0.50 :
			print 'File '+rootfile+' is too small, its size is '+str(filesize)+' bytes, contributing '+str(filesize/expected_contribution)+' of its expectation'
			jobnumber = int(rootfile.rstrip('_tree.root').split('_')[len(rootfile.rstrip('_tree.root').split('_'))-1])
			if includeJEC :
				if rootfile.find('JES_up')!=-1 : failedjobnumbers['JES_up'].append(jobnumber)
				elif rootfile.find('JES_down')!=-1 : failedjobnumbers['JES_down'].append(jobnumber)
				elif rootfile.find('JER_up')!=-1 : failedjobnumbers['JER_up'].append(jobnumber)
				elif rootfile.find('JER_down')!=-1 : failedjobnumbers['JER_down'].append(jobnumber)
				else : failedjobnumbers['nom'].append(jobnumber)
			else : failedjobnumbers['nom'].append(jobnumber)
	print 'Jobs to rerun: %s'%(failedjobnumbers)
	#if there are no jobs to rerun, print that this run is finished!
	if len(failedjobnumbers['nom'])==0 and len(failedjobnumbers['JES_up'])==0 and len(failedjobnumbers['JES_down'])==0 and len(failedjobnumbers['JER_up'])==0 and len(failedjobnumbers['JER_down'])==0 :
		print 'All jobs complete!'
	else :
		#sort the lists of failed job numbers
		for thislist in failedjobnumbers.values() :
			thislist.sort()
		#open the list of all the jobs and add the failed ones to the new file
		if not os.path.isfile('ana.listOfJobs_all') :
			#print 'TOTAL LIST OF JOBS DOES NOT EXIST YET, COPYING CURRENT LIST OF JOBS!!'
			os.system('mv ana.listOfJobs ana.listOfJobs_all')
			if includeJEC :
				for app in failedjobnumbers.keys() :
					if app!='nom' :
						os.system('mv ana.listOfJobs_'+app+' ana.listOfJobs_'+app+'_all')
		else :
			os.system('rm -rf ana.listOfJobs')
		intjobs=[]
		for app in failedjobnumbers.keys() :
			joblist = None
			if app=='nom' : joblist = open('ana.listOfJobs_all','r')
			else : 
				if dojec=='yes' :
					joblist = open('ana.listOfJobs_'+app+'_all','r')
				else :
					continue
			linecount = 0
			for job in joblist.readlines() :
				jobreal = job.rstrip('\n')
				if linecount in failedjobnumbers[app] : 
					os.system('echo "'+jobreal+'" >> ana.listOfJobs')
					intjob ='python ../../python/'
					for nexts in jobreal[len('python ./tardir/'):].split('--on_grid yes') :
						intjob+=nexts
					intjobs.append(intjob)
				linecount+=1
		check = raw_input('Would you like to print the list of jobs to rerun ('+str(len(intjobs))+' total)? (y/n) : ')
		if check.lower().find('y')!=-1 :
			print '---------------------'
			for i in range(len(intjobs)) :
				print 'cd ~/reconstructortest/'+name+'; cmsenv'
				print intjobs[i]
				print ''
			print '---------------------'
	os.system('bash cleanup.bash')
	#return to the previous working directory
	os.chdir(cwd)
	print 'Done.'

def haddRecoFilesParallel(haddjob) :
	print haddjob[0]
	#the hadd command
	cmd = 'hadd -f '+haddjob[0]
	for f in haddjob[1] :
		cmd+=' '+f
	os.system(cmd)
def haddRecoFiles(sample,dojec) :
	name = sample.getShortName()
	print 'hadd-ing reconstructor files for sample '+name
	#first find out where we are right now so we can return here
	cwd = os.getcwd()
	#next move to the reconstructor run directory 
	os.chdir(subprocess.check_output('echo $CMSSW_BASE',shell=True).rstrip('\n')+'/src/Analysis/Reconstructor/test/'+name)
	#get the list of reco files
	includeJEC = len(glob.glob('*JES_up*'))>0 and len(glob.glob('*JES_down*'))>0 and len(glob.glob('*JER_up*'))>0 and len(glob.glob('*JER_down*'))>0 and dojec=='yes'
	listsOfFiles = [glob.glob(name+'_*_tree.root')]
	names = [name]
	i=0
	while i<len(listsOfFiles[0]) :
		thisfile = listsOfFiles[0][i]
		if thisfile.find('JES')!= -1 or thisfile.find('JER')!=-1 :
			listsOfFiles[0].pop(i)
		else :
			i+=1
	if includeJEC :
		listsOfFiles.append(glob.glob(name+'_JES_up_*_tree.root')); names.append(name+'_JES_up')
		listsOfFiles.append(glob.glob(name+'_JES_down_*_tree.root')); names.append(name+'_JES_down')
		listsOfFiles.append(glob.glob(name+'_JER_up_*_tree.root')); names.append(name+'_JER_up')
		listsOfFiles.append(glob.glob(name+'_JER_down_*_tree.root')); names.append(name+'_JER_down')
	for i in range(len(listsOfFiles)) :
		listOfFiles = listsOfFiles[i]
		name = names[i]
		#make the list of hadd jobs
		hadd_jobs = []
		size_counter = 0.;
		for thisfile in listOfFiles :
			nextsize = os.path.getsize(thisfile)
			if size_counter==0. or size_counter+nextsize>250000000. or len(hadd_jobs[len(hadd_jobs)-1][1])>=500 : #250MB or 500 file limit
				hadd_jobs.append(['aggregated_'+name+'_'+str(len(hadd_jobs))+'.root',[]])
				size_counter=0.
			size_counter+=nextsize
			hadd_jobs[len(hadd_jobs)-1][1].append(thisfile)
		#execute the hadd jobs to aggregate files in this directory
		procs = []
		for i in range(len(hadd_jobs)) :
			if len(procs)>5 :
				for proc in procs :
					proc.join()
				procs = []
			haddjob = hadd_jobs[i]
			p = multiprocessing.Process(target=haddRecoFilesParallel,args=(haddjob,))
			p.start()
			procs.append(p)
		for proc in procs :
			proc.join()
		#make sure it ran correctly and if so delete the non-hadded files
		isGood = True
		for haddjob in hadd_jobs :
			if (not os.path.isfile(haddjob[0])) or os.path.getsize(haddjob[0])<100000. : #gotta exist and be at least 100kB
				isGood = False
				break
		if isGood :
			print 'SUCCESS! Deleting old files.'
			for thisfile in listOfFiles :
				os.system('rm -rf '+thisfile)
		else :
			print 'FAILED! Old files not deleted; try again after you fix whatever is wrong.'

def skimRecoFilesParallel(thisfile) :
	f = TFile(thisfile); t = f.Get('tree')
	newname = thisfile.rstrip('.root')+'_skim.root'
	newFile = TFile(newname,'recreate')
	#newTree = t.CopyTree('weight!=0.')
	#newTree	= t.CopyTree('metfilters==1 && trigger==1 && onelepton==1 && btags==1 && ak4jetmult==1 && ak4jetcuts==1 && validminimization==1')
	newTree = t.CopyTree('fullselection==1 || wjets_cr_selection==1 || qcd_A_SR_selection==1 || qcd_B_SR_selection==1 || qcd_C_SR_selection==1 || qcd_A_CR_selection==1 || qcd_B_CR_selection==1 || qcd_C_CR_selection==1')
	#newTree = t.CopyTree('fullselection==1')
	#newTree = t.CopyTree('eventTopology==1')
	newTree.Write()
	newFile.Purge()
	newFile.Close()
def skimRecoFiles(sample,dojec) :
	name = sample.getShortName()
	print 'skimming reconstructor files for sample '+name
	#first find out where we are right now so we can return here
	cwd = os.getcwd()
	#next move to the reconstructor run directory 
	os.chdir(subprocess.check_output('echo $CMSSW_BASE',shell=True).rstrip('\n')+'/src/Analysis/Reconstructor/test/'+name)
	#skim files
	os.system('rm -rf *_skim.root')
	includeJEC = len(glob.glob('*JES_up*'))>0 and len(glob.glob('*JES_down*'))>0 and len(glob.glob('*JER_up*'))>0 and len(glob.glob('*JER_down*'))>0 and dojec=='yes' 
	filelist = glob.glob('aggregated_'+name+'_*.root')
	if len(filelist)==0 :
		filelist = glob.glob('*_tree.root')
	if not includeJEC :
		i = 0
		while i<len(filelist) :
			if filelist[i].find('JES')!=-1 or filelist[i].find('JER')!=-1 :
				filelist.pop(i)
			else :
				i+=1
	procs = []
	for i in range(len(filelist)) :
		if i%5==0 :
			for proc in procs :
				proc.join()
			procs = []
		thisfile = filelist[i]
		print ' '+str(i)+': '+str(thisfile)
		p = multiprocessing.Process(target=skimRecoFilesParallel,args=(thisfile,))
		p.start()
		procs.append(p)
	for proc in procs :
		proc.join()
	os.chdir(cwd)
	

def skimHaddRecoFilesParallel(cmdlist) :
	for cmd in cmdlist :
		print 'new hadd command: %s'%(cmd)
		os.system(cmd)
def skimHaddRecoFiles(sample,dojec) :
	skimRecoFiles(sample,dojec)
	name = sample.getShortName()
	print 'hadd-ing skimmed reconstructor files for sample '+name
	cwd = os.getcwd()
	#move to the reconstructor run directory 
	os.chdir(subprocess.check_output('echo $CMSSW_BASE',shell=True).rstrip('\n')+'/src/Analysis/Reconstructor/test/'+name)
	filelist = glob.glob('*_skim.root')
	includeJEC = len(glob.glob('*JES_up*'))>0 and len(glob.glob('*JES_down*'))>0 and len(glob.glob('*JER_up*'))>0 and len(glob.glob('*JER_down*'))>0 and dojec=='yes'
	i = 0
	while i<len(filelist) :
		if filelist[i].find('JES')!=-1 or filelist[i].find('JER')!=-1 :
			filelist.pop(i)
		else :
			i+=1
	ifiles = []; ifiles_JES_up = []; ifiles_JES_down = []; ifiles_JER_up = []; ifiles_JER_down = []
	if len(filelist)==1 :
		os.system('cp '+filelist[0]+' '+name+'_skim_all.root ')
		if includeJEC :
			fnamesplit = filelist[0].split('_')
			os.system('cp '+filelist[0].replace(fnamesplit[-2]+'_'+fnamesplit[-1],'')+'JES_up_'+fnamesplit[-2]+'_'+fnamesplit[-1]+' '+name+'_JES_up_skim_all.root')
			os.system('cp '+filelist[0].replace(fnamesplit[-2]+'_'+fnamesplit[-1],'')+'JES_down_'+fnamesplit[-2]+'_'+fnamesplit[-1]+' '+name+'_JES_down_skim_all.root')
			os.system('cp '+filelist[0].replace(fnamesplit[-2]+'_'+fnamesplit[-1],'')+'JER_up_'+fnamesplit[-2]+'_'+fnamesplit[-1]+' '+name+'_JER_up_skim_all.root')
			os.system('cp '+filelist[0].replace(fnamesplit[-2]+'_'+fnamesplit[-1],'')+'JER_down_'+fnamesplit[-2]+'_'+fnamesplit[-1]+' '+name+'_JER_down_skim_all.root')
	else :
		procs = []
		for i in range(len(filelist)) :
			if len(procs)>5 :
				for proc in procs :
					proc.join()
				procs = []
			thisfile = filelist[i]
			if i%100==0 :
				cmd = 'hadd -f '+name+'_skim_all_'+str(len(ifiles))+'.root '+thisfile; ifiles.append(name+'_skim_all_'+str(len(ifiles))+'.root')
				if includeJEC :
					tfs = thisfile.split('_')
					cmd_JES_up   = 'hadd -f '+name+'_JES_up_skim_all_'+str(len(ifiles_JES_up))+'.root aggregated_'+name+'_JES_up_'+tfs[-2]+'_'+tfs[-1]
					ifiles_JES_up.append(name+'_JES_up_skim_all_'+str(len(ifiles_JES_up))+'.root')
					cmd_JES_down = 'hadd -f '+name+'_JES_down_skim_all_'+str(len(ifiles_JES_down))+'.root aggregated_'+name+'_JES_down_'+tfs[-2]+'_'+tfs[-1]
					ifiles_JES_down.append(name+'_JES_down_skim_all_'+str(len(ifiles_JES_down))+'.root')
					cmd_JER_up   = 'hadd -f '+name+'_JER_up_skim_all_'+str(len(ifiles_JER_up))+'.root aggregated_'+name+'_JER_up_'+tfs[-2]+'_'+tfs[-1]
					ifiles_JER_up.append(name+'_JER_up_skim_all_'+str(len(ifiles_JER_up))+'.root')
					cmd_JER_down = 'hadd -f '+name+'_JER_down_skim_all_'+str(len(ifiles_JER_down))+'.root aggregated_'+name+'_JER_down_'+tfs[-2]+'_'+tfs[-1]
					ifiles_JER_down.append(name+'_JER_down_skim_all_'+str(len(ifiles_JER_down))+'.root')
			else :
				cmd+=' '+thisfile
				if includeJEC :
					tfs = thisfile.split('_')
					cmd_JES_up+=' aggregated_'+name+'_JES_up_'+tfs[-2]+'_'+tfs[-1]
					cmd_JES_down+=' aggregated_'+name+'_JES_down_'+tfs[-2]+'_'+tfs[-1]
					cmd_JER_up+=' aggregated_'+name+'_JER_up_'+tfs[-2]+'_'+tfs[-1]
					cmd_JER_down+=' aggregated_'+name+'_JER_down_'+tfs[-2]+'_'+tfs[-1]
			if (i+1)%100==0 or i==len(filelist)-1 :
				cmdlist = [cmd]
				if includeJEC :
					cmdlist.append(cmd_JES_up); cmdlist.append(cmd_JES_down); cmdlist.append(cmd_JER_up); cmdlist.append(cmd_JER_down)
				p = multiprocessing.Process(target=skimHaddRecoFilesParallel,args=(cmdlist,))
				p.start()
				procs.append(p)
		for proc in procs :
			proc.join()
		#hadd all of the intermediately-sized files (or rename them if there's only one of each)
		cmd_dict = {}
		cmd_dict['']=ifiles
		if includeJEC :
			cmd_dict['JES_up']=ifiles_JES_up
			cmd_dict['JES_down']=ifiles_JES_down
			cmd_dict['JER_up']=ifiles_JER_up
			cmd_dict['JER_down']=ifiles_JER_down
		for cmd_key in cmd_dict :
			list_of_files = cmd_dict[cmd_key]
			cmd_stub = name
			if cmd_key!='' :
				cmd_stub+='_'+cmd_key
			cmd_stub+='_skim_all.root'
			if len(list_of_files)==1 :
				cmd='mv '+list_of_files[0]+' '+cmd_stub
			else :
				cmd='hadd -f '+cmd_stub
				for ifile in list_of_files :
					cmd+=' '+ifile
			print 'new hadd command: %s'%(cmd)
			os.system(cmd)
	if not os.path.isdir('../total_ttree_files') : os.mkdir('../total_ttree_files')
	os.system('mv *_all.root ../total_ttree_files')
	os.system('rm -rf *_skim_*.root')
	os.system('rm -rf *_skim.root')
	#return to the previous working directory
	os.chdir(cwd)
	print 'Done.'
