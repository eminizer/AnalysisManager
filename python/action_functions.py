import os
import subprocess
import glob
import multiprocessing
from ROOT import TFile

JEC_STEMS = ['nominal',
			 'JES_up','JES_dn','JER_up','JER_dn',
			 #'AK4JESPU_up','AK4JESEta_up','AK4JESPt_up','AK4JESScale_up','AK4JESTime_up','AK4JESFlav_up','AK4JERStat_up','AK4JERSys_up',
			 #'AK4JESPU_dn','AK4JESEta_dn','AK4JESPt_dn','AK4JESScale_dn','AK4JESTime_dn','AK4JESFlav_dn','AK4JERStat_dn','AK4JERSys_dn',
			 #'AK8JESPU_up','AK8JESEta_up','AK8JESPt_up','AK8JESScale_up','AK8JESTime_up','AK8JESFlav_up','AK8JERStat_up','AK8JERSys_up',
			 #'AK8JESPU_dn','AK8JESEta_dn','AK8JESPt_dn','AK8JESScale_dn','AK8JESTime_dn','AK8JESFlav_dn','AK8JERStat_dn','AK8JERSys_dn',
			 ]

def haddNtuplesParallel(haddjob,sampleeosurl) :
	#the hadd command
	#print 'command has %d files to add'%(len(haddjob[1])) #DEBUG
	cmd = 'hadd '+haddjob[0]
	for f in haddjob[1] :
		cmd+=' '+f
	os.system(cmd)
	#the xrdcp command
	os.system('xrdcp '+haddjob[0]+' '+sampleeosurl+'/'+haddjob[0])
#hadd a sample's individual nTuple root files into a smaller number of files, each about 5GB, then copy back to EOS
def haddNTuples(sample,dojec,donominal) :
	print 'hadding nTuples for sample '+sample.getShortName()
	cwd = os.getcwd()
	##remove the aggregated files that might already be in the directory
	#agg_filelist = sample.getAggregatedFileTuples()
	#for agg_file in agg_filelist :
	#	subprocess.call('eos root://cmseos.fnal.gov rm '+agg_file[0][len('root://xxx.xxx.xxx.xxx:xxxx/'):],shell=True)
	#get the list of raw file tuples (URL,size)
	rawfile_tuples = sample.getRawFileTuples()
	#make the list of hadd jobs
	hadd_jobs = []
	size_counter = 0.;
	for rft in rawfile_tuples :
		if size_counter==0. or size_counter+rft[1]>5000000000. or len(hadd_jobs[len(hadd_jobs)-1][1])>=250 : #5GB or 250 file limit
			#print 'size_counter is at %d'%(size_counter) #DEBUG
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
def setupRecoRuns(sample,dojec,donominal) :	
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
	nJobsJECWiggled = njobs/10 if njobs>50 else njobs
	nTotalJobs = njobs if donominal else 0
	if dojec :
		nTotalJobs+=(len(JEC_STEMS)-1)*nJobsJECWiggled
	print '	making new listOfJobs files; sample %s requested %d jobs (will be %d really, since doNominal=%s and doJEC=%s)'%(name,njobs,nTotalJobs,donominal,dojec)
	for JEC_STEM in JEC_STEMS :
		if (JEC_STEM=='nominal' and not donominal) or (JEC_STEM!='nominal' and not dojec) :
			continue
		thisNJobs = njobs if JEC_STEM=='nominal' else nJobsJECWiggled
		for i in range(thisNJobs) :
			cmd = 'echo "python ./tardir/run_reconstructor.py --name '+sample.getShortName()+' --xSec '+str(sample.getXSec())
			cmd+= ' --kFac '+str(sample.getKFactor())+' --on_grid yes --n_jobs '+str(thisNJobs)+' --i_job '+str(i)+' --JEC '+JEC_STEM
			os.system(cmd+'" >> ana.listOfJobs_'+JEC_STEM)
	#return to the previous working directory
	os.chdir(cwd)
	print 'Done.'

#run the new runs of the reconstructor
def runReco(sample,dojec,donominal) :
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
	for JEC_STEM in JEC_STEMS :
		if (JEC_STEM=='nominal' and not donominal) or (JEC_STEM!='nominal' and not dojec) :
			continue
		os.system('python ./runManySections.py --createCommandFile --addLog --setTarball=tarball.tgz ana.listOfJobs_'+JEC_STEM+' commands_'+JEC_STEM+'.cmd')
		os.system('python ./runManySections.py --submitCondor --noDeleteCondor commands_'+JEC_STEM+'.cmd')
	#return to the previous working directory
	os.chdir(cwd)
	print 'Done.'

#find the jobs that failed in the sample's reco directory and make a new ana.listOfJobs with just those
def findFailedJobs(sample,dojec,donominal) :
	name = sample.getShortName()
	print 'finding failed jobs for sample '+name
	#first find out where we are right now so we can return here
	cwd = os.getcwd()
	#next move to the reconstructor run directory 
	os.chdir(subprocess.check_output('echo $CMSSW_BASE',shell=True).rstrip('\n')+'/src/Analysis/Reconstructor/test/'+name) 
	#get the lists of root files, and the numbers of original jobs
	rootfilelists = {}
	nJobs={}
	for JEC_STEM in JEC_STEMS :
		if (JEC_STEM=='nominal' and not donominal) or (JEC_STEM!='nominal' and not dojec) :
			continue
		if JEC_STEM=='nominal' :
			allrootfiles=glob.glob(name+'_*_tree.root')
			rootfilelists[JEC_STEM] = []
			for rfile in allrootfiles :
				thisfilenom = True
				for JEC_STEM2 in JEC_STEMS :
					if JEC_STEM2!='nominal' and rfile.find(JEC_STEM2)!=-1 :
						thisfilenom=False; break
				if thisfilenom :
					rootfilelists[JEC_STEM].append(rfile)
		else :
			rootfilelists[JEC_STEM]=glob.glob(name+'_'+JEC_STEM+'_*tree.root')
		nJobs[JEC_STEM]=int(os.popen('cat ana.listOfJobs_'+JEC_STEM+' | wc -l').read())
	#make a dictionary of the failed job numbers/which JEC wiggles they are
	failedjobnumbers = {}
	for JEC_STEM in JEC_STEMS :
		if (JEC_STEM=='nominal' and not donominal) or (JEC_STEM!='nominal' and not dojec) :
			continue
		print 'checking output for JEC stem %s: len(rootfilelist)=%d, nJobs=%d'%(JEC_STEM,len(rootfilelists[JEC_STEM]),nJobs[JEC_STEM])
		failedjobnumbers[JEC_STEM]=[]
		#first look for jobs that didn't return anything
		if len(rootfilelists[JEC_STEM]) < nJobs[JEC_STEM] : #something's missing
			#for each job number
			for i in range(nJobs[JEC_STEM]) :
				checkfile = [f for f in rootfilelists[JEC_STEM] if '_'+str(i)+'_tree.root' in f]
				if len(checkfile)!=1 :
					print 'Missing some output from job number '+str(i)+' for JEC '+JEC_STEM
					failedjobnumbers[JEC_STEM].append(i)
		#now check the file sizes to find any that are abnormally small
		totalsize = 0.
		for rootfile in rootfilelists[JEC_STEM] :
			totalsize+=os.path.getsize(rootfile)
		expected_contribution = totalsize/len(rootfilelists[JEC_STEM]) if len(rootfilelists[JEC_STEM])!=0 else 0.
		for rootfile in rootfilelists[JEC_STEM] :
			filesize = os.path.getsize(rootfile)
			if filesize/expected_contribution<0.80 :
				print 'File '+rootfile+' is too small, its size is '+str(filesize)+' bytes, contributing '+str(filesize/expected_contribution)+' of its expectation'
				failedjobnumbers[JEC_STEM].append(int(rootfile.rstrip('_tree.root').split('_')[-1]))
	#print 'Jobs to rerun: %s'%(failedjobnumbers)
	#if there are no jobs to rerun, print that this run is finished!
	nFailedJobs=sum([len(fjn) for fjn in failedjobnumbers.values()])
	if nFailedJobs==0 :
		print 'All jobs complete!'
	else :
		intjobs=[]
		if not os.path.isfile('ana.listOfJobs_rerun') :
			os.system('touch ana.listOfJobs_rerun')
		for JEC_STEM in failedjobnumbers :
			linecount=0
			for job in open('ana.listOfJobs_'+JEC_STEM).readlines() :
				if linecount in failedjobnumbers[JEC_STEM] :
					os.system('echo "'+job.rstrip('\n')+'" >> ana.listOfJobs_rerun')
					intjob ='python ../../python/'
					for nexts in job.rstrip('\n')[len('python ./tardir/'):].split('--on_grid yes') :
						intjob+=nexts
					intjobs.append(intjob)
				linecount+=1
		check = raw_input('Would you like to print the list of jobs to rerun interactively ('+str(len(intjobs))+' total)? (y/n) : ')
		if check.lower().find('y')!=-1 :
			print '---------------------'
			for i in range(len(intjobs)) :
				print 'cd ~/reconstructortest/'+name+'; cmsenv'
				print intjobs[i]
				print ''
			print '---------------------'
		check = raw_input('Would you like to resubmit the failed jobs on condor now? If you wait you will have to submit ana.listOfJobs_rerun manually! (y/n) : ')
		if check.lower() in ['y','yes'] :
			os.system('tar czvf tarball.tgz ./input.txt -C ../../python/ . -C ../test/other_input_files/ .')
			os.system('voms-proxy-init --voms cms')
			os.system('python ./runManySections.py --createCommandFile --addLog --setTarball=tarball.tgz ana.listOfJobs_rerun commands_rerun.cmd')
			os.system('python ./runManySections.py --submitCondor --noDeleteCondor commands_rerun.cmd')
	if not os.path.isdir('output') :
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
def haddRecoFiles(sample,dojec,donominal) :
	name = sample.getShortName()
	print 'hadd-ing reconstructor files for sample '+name
	#first find out where we are right now so we can return here
	cwd = os.getcwd()
	#next move to the reconstructor run directory 
	os.chdir(subprocess.check_output('echo $CMSSW_BASE',shell=True).rstrip('\n')+'/src/Analysis/Reconstructor/test/'+name)
	#for each JEC wiggle
	for JEC_STEM in JEC_STEMS :
		if (JEC_STEM=='nominal' and not donominal) or (JEC_STEM!='nominal' and not dojec) :
			continue
		#get the list of rootfiles
		rootfilelist=None
		if JEC_STEM=='nominal' :
			allrootfiles=glob.glob(name+'_*_tree.root')
			rootfilelist = []
			for rfile in allrootfiles :
				thisfilenom = True
				for JEC_STEM2 in JEC_STEMS :
					if JEC_STEM2!='nominal' and rfile.find(JEC_STEM2)!=-1 :
						thisfilenom=False; break
				if thisfilenom :
					rootfilelist.append(rfile)
		else :
			rootfilelist=glob.glob(name+'_'+JEC_STEM+'_*_tree.root')
		#make the list of hadd jobs
		hadd_jobs = []
		size_counter = 0.;
		for thisfile in rootfilelist :
			nextsize = os.path.getsize(thisfile)
			if size_counter==0. or size_counter+nextsize>250000000. or len(hadd_jobs[len(hadd_jobs)-1][1])>=500 : #250MB or 500 file limit
				if JEC_STEM=='nominal' :
					hadd_jobs.append(['aggregated_'+name+'_'+str(len(hadd_jobs))+'.root',[]])
				else :
					hadd_jobs.append(['aggregated_'+name+'_'+JEC_STEM+'_'+str(len(hadd_jobs))+'.root',[]])
				size_counter=0.
			size_counter+=nextsize
			hadd_jobs[-1][1].append(thisfile)
		#execute the hadd jobs to aggregate files in this directory
		procs = []
		for i in range(len(hadd_jobs)) :
			if len(procs)>1 : #honestly this should not be in parallel
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
			if (not os.path.isfile(haddjob[0])) or os.path.getsize(haddjob[0])<10000. : #gotta exist and be at least 10kB
				isGood = False
				break
		if isGood :
			print 'SUCCESS! (I think.) Please double-check the output above and make sure things went okay.'
			check = raw_input('Are you sure you want to delete the old files? (y/n): ')
			if check.lower() in ['y','yes'] :
				for thisfile in rootfilelist :
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
def skimRecoFiles(sample,dojec,donominal) :
	name = sample.getShortName()
	print 'skimming reconstructor files for sample '+name
	#first find out where we are right now so we can return here
	cwd = os.getcwd()
	#next move to the reconstructor run directory 
	os.chdir(subprocess.check_output('echo $CMSSW_BASE',shell=True).rstrip('\n')+'/src/Analysis/Reconstructor/test/'+name)
	#skim files
	os.system('rm -rf *_skim.root')
	filelist = glob.glob('aggregated_'+name+'_*.root')
	if len(filelist)==0 :
		filelist = glob.glob('*_tree.root')
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
def skimHaddRecoFiles(sample,dojec,donominal) :
	skimRecoFiles(sample,dojec,donominal)
	name = sample.getShortName()
	print 'hadd-ing skimmed reconstructor files for sample '+name
	cwd = os.getcwd()
	#move to the reconstructor run directory 
	os.chdir(subprocess.check_output('echo $CMSSW_BASE',shell=True).rstrip('\n')+'/src/Analysis/Reconstructor/test/'+name)
	#for each JEC wiggle
	for JEC_STEM in JEC_STEMS :
		if (JEC_STEM=='nominal' and not donominal) or (JEC_STEM!='nominal' and not dojec) :
			continue
		#get the list of rootfiles
		rootfilelist=None
		if JEC_STEM=='nominal' :
			allrootfiles=glob.glob('*_skim.root')
			rootfilelist = []
			for rfile in allrootfiles :
				thisfilenom = True
				for JEC_STEM2 in JEC_STEMS :
					if JEC_STEM2!='nominal' and rfile.find(JEC_STEM2)!=-1 :
						thisfilenom=False; break
				if thisfilenom :
					rootfilelist.append(rfile)
		else :
			rootfilelist=glob.glob('*_'+JEC_STEM+'_*_skim.root')
		#if there's only one file just copy it
		if len(rootfilelist)==1 :
			if JEC_STEM=='nominal' :
				os.system('cp '+rootfilelist[0]+' '+name+'_skim_all.root ')
			else :
				os.system('cp '+rootfilelist[0]+' '+name+'_'+JEC_STEM+'_skim_all.root ')
		#otherwise we have to hadd all of them together
		else :
			ifiles=[]
			procs = []
			for i in range(len(rootfilelist)) :
				if len(procs)>5 :
					for proc in procs :
						proc.join()
					procs = []
				thisfile=rootfilelist[i]
				if i%100==0 :
					if JEC_STEM=='nominal' :
						cmd = 'hadd -f '+name+'_skim_all_'+str(len(ifiles))+'.root '+thisfile; ifiles.append(name+'_skim_all_'+str(len(ifiles))+'.root')
					else :
						cmd = 'hadd -f '+name+'_'+JEC_STEM+'_skim_all_'+str(len(ifiles))+'.root '+thisfile; ifiles.append(name+'_'+JEC_STEM+'_skim_all_'+str(len(ifiles))+'.root')
				else :
					cmd+=' '+thisfile
				if (i+1)%100==0 or i==len(rootfilelist)-1 :
					p = multiprocessing.Process(target=skimHaddRecoFilesParallel,args=([cmd],))
					p.start()
					procs.append(p)
			for proc in procs :
				proc.join()
			#hadd all of the intermediately-sized files (or rename them if there's only one of each)
			cmd_stub = name
			if JEC_STEM!='nominal' :
				cmd_stub+='_'+JEC_STEM
			cmd_stub+='_skim_all.root'
			if len(ifiles)==1 :
				cmd='mv '+ifiles[0]+' '+cmd_stub
			else :
				cmd='hadd -f '+cmd_stub
				for ifile in ifiles :
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
