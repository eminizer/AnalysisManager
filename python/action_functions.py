import os
import subprocess
import glob
from ROOT import TFile

#hadd a sample's individual nTuple root files into a smaller number of files, each about 5GB, then copy back to EOS
def haddNTuples(sample) :
	print 'hadding nTuples for sample '+sample.getShortName()
	cwd = os.getcwd()
	#remove the aggregated files that might already be in the directory
	agg_filelist = sample.getAggregatedFileTuples()
	for agg_file in agg_filelist :
		subprocess.call('eos root://cmseos.fnal.gov rm '+agg_file[0][len('root://cmseos.fnal.gov/'):],shell=True)
	#get the list of raw file tuples (URL,size)
	rawfile_tuples = sample.getRawFileTuples()
	#make the list of hadd jobs
	hadd_jobs = []
	size_counter = 0.;
	for rft in rawfile_tuples :
		if size_counter==0. or size_counter+rft[1]>5000000000.: #5GB limit
			hadd_jobs.append(['aggregated_'+sample.getShortName()+'_'+str(len(hadd_jobs))+'.root',[]])
			size_counter=0.
		size_counter+=rft[1]
		hadd_jobs[len(hadd_jobs)-1][1].append(rft[0])
	#execute the hadd jobs to aggregate files in this directory
	os.chdir(subprocess.check_output('echo $CMSSW_BASE',shell=True).rstrip('\n')+'/src/Analysis/AnalysisManager/test')
	os.system('mkdir '+sample.getShortName())
	os.chdir(sample.getShortName())
	for haddjob in hadd_jobs :
		#the hadd command
		cmd = 'hadd '+haddjob[0]
		for f in haddjob[1] :
			cmd+=' '+f
		os.system(cmd)
		#the xrdcp command
		os.system('xrdcp '+haddjob[0]+' '+sample.getEOSBaseURL()+'/'+haddjob[0])
	#back up and remove
	os.chdir('..')
	os.system('rm -rf '+sample.getShortName())
	os.chdir(cwd)
	print 'Done.'

#setup runs of the reconstructor: regenerate the directories, scripts, input file and ana.listOfJobs
def setupRecoRuns(sample) :	
	print 'setting up reconstructor runs for sample '+sample.getShortName()
	#first find out where we are right now so we can return here
	cwd = os.getcwd()
	#next move to the reconstructor test area
	os.chdir(subprocess.check_output('echo $CMSSW_BASE',shell=True).rstrip('\n')+'/src/Analysis/Reconstructor/test')
	#remove the old directory
	print '	removing old directory'
	os.system('rm -rf '+sample.getShortName())
	#make the new directory for the sample
	print '	making new directory'
	os.mkdir(sample.getShortName())
	os.chdir(sample.getShortName())
	#copy the cleanup and submit scripts from the reconstructor/test directory locally
	print '	copying scripts'
	os.system('cp ../cleanup.bash .')
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
	print '	making new listOfJobs; sample %s will have %d jobs'%(sample.getShortName(),njobs)
	for i in range(njobs) :
		cmd = 'echo "python ./tardir/run_reconstructor.py --name '+sample.getShortName()+' --xSec '+str(sample.getXSec())
		cmd+= ' --on_grid yes --n_jobs '+str(njobs)+' --i_job '+str(i)
		os.system(cmd+'" >> ana.listOfJobs')
		#And the JEC-wiggled jobs
	#	os.system(cmd+' --JES up" >> ana.listOfJobs')
	#	os.system(cmd+' --JES down" >> ana.listOfJobs')
	#	os.system(cmd+' --JER up" >> ana.listOfJobs')
	#	os.system(cmd+' --JER down" >> ana.listOfJobs')
	#return to the previous working directory
	os.chdir(cwd)
	print 'Done.'

#run the new runs of the reconstructor
def runReco(sample) :
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
	os.system('python /uscms_data/d3/eminizer/runManySections/runManySections.py --createCommandFile --cmssw --addLog --setTarball=tarball.tgz ana.listOfJobs commands.cmd')
	os.system('python /uscms_data/d3/eminizer/runManySections/runManySections.py --submitCondor commands.cmd')
	#return to the previous working directory
	os.chdir(cwd)
	print 'Done.'

#find the jobs that failed in the sample's reco directory and make a new ana.listOfJobs with just those
def findFailedJobs(sample) :
	print 'finding failed jobs for sample '+sample.getShortName()
	#first find out where we are right now so we can return here
	cwd = os.getcwd()
	#next move to the reconstructor run directory 
	os.chdir(subprocess.check_output('echo $CMSSW_BASE',shell=True).rstrip('\n')+'/src/Analysis/Reconstructor/test/'+sample.getShortName())
	#did this directory have JEC wiggled runs in it or not?
	includeJEC = len(glob.glob('*JES_up*'))>0 and len(glob.glob('*JES_down*'))>0 and len(glob.glob('*JER_up*'))>0 and len(glob.glob('*JER_down*'))>0  
	#get the list of all the root files, and the number of original jobs
	rootfilelist = glob.glob('*_tree.root')
	nJobs = int(os.popen('cat ana.listOfJobs_all | wc -l').read()) if len(glob.glob('ana.listOfJobs_all'))!=0 else int(os.popen('cat ana.listOfJobs | wc -l').read())
	#make a list of the failed job numbers
	failedjobnumbers = []
	#first look for jobs that didn't return anything
	print 'len(rootfilelist)=%d, nJobs=%d'%(len(rootfilelist),nJobs)
	if len(rootfilelist) < nJobs :
		#if there were JEC files run 
		if includeJEC :
			#for each job number
			for i in range(nJobs/5) :
				theseRootFiles = glob.glob('*_'+str(i)+'_tree.root')
				#there should be five files per job
				if len(theseRootFiles) < 5 :
					print 'Missing some output from job number '+str(i)+', checking which of the JEC wiggles it is'
					newfailedjobnumbers = [5*i,5*i+1,5*i+2,5*i+3,5*i+4]
					for rfilename in theseRootFiles :
						if rfilename.find('JES')==-1 and rfilename.find('JER')==-1: newfailedjobnumbers.pop(newfailedjobnumbers.index(5*i))
						elif rfilename.find('JES_up')!=-1 : newfailedjobnumbers.pop(newfailedjobnumbers.index(5*i+1))
						elif rfilename.find('JES_down')!=-1 : newfailedjobnumbers.pop(newfailedjobnumbers.index(5*i+2))
						elif rfilename.find('JER_up')!=-1 : newfailedjobnumbers.pop(newfailedjobnumbers.index(5*i+3))
						elif rfilename.find('JER_down')!=-1 : newfailedjobnumbers.pop(newfailedjobnumbers.index(5*i+4))
					failedjobnumbers+=newfailedjobnumbers
		#otherwise it's simpler
		else :
			#for each job number
			for i in range(nJobs) :
				#check if the there's an outputted file
				if len(glob.glob('*_'+str(i)+'_tree.root'))==0 :
					print 'Job number '+str(i)+' had no output!'
					failedjobnumbers.append(i)
	#now check the file sizes to find any that are abnormally small
	totalsize = 0.
	for rootfile in rootfilelist :
		totalsize+=os.path.getsize(rootfile)
	expected_contribution = totalsize/len(rootfilelist)
	for rootfile in rootfilelist :
		filesize = os.path.getsize(rootfile)
		if filesize/expected_contribution<0.80 :
			print 'File '+rootfile+' is too small, its size is '+str(filesize)+' bytes, contributing '+str(filesize/expected_contribution)+' of its expectation'
			jobnumber = int(rootfile.rstrip('_tree.root').split('_')[len(rootfile.rstrip('_tree.root').split('_'))-1])
			if includeJEC and (not 'singleel' in rootfile.lower() and not 'singlemu' in rootfile.lower()) :
				jobnumber *= 5
				if rootfile.find('JES_up')!=-1 : jobnumber+=1
				elif rootfile.find('JES_down')!=-1 : jobnumber+=2
				elif rootfile.find('JER_up')!=-1 : jobnumber+=3
				elif rootfile.find('JER_down')!=-1 : jobnumber+=4
			if not jobnumber in failedjobnumbers : failedjobnumbers.append(jobnumber)
	#sort the list of failed job numbers
	failedjobnumbers.sort()
	#open the list of all the jobs and add the failed ones to the new file
	linecount = 0
	if not os.path.isfile('ana.listOfJobs_all') :
		print 'TOTAL LIST OF JOBS DOES NOT EXIST YET, COPYING CURRENT LIST OF JOBS!!'
		os.system('mv ana.listOfJobs ana.listOfJobs_all')
	os.system('rm -rf ana.listOfJobs')
	joblist = open('ana.listOfJobs_all','r')
	for job in joblist.readlines() :
		jobreal = job.rstrip('\n')
		if linecount in failedjobnumbers : os.system('echo "'+jobreal+'" >> ana.listOfJobs')
		linecount+=1
	print 'Total new list of jobs: '
	os.system('cat ana.listOfJobs')
	os.system('bash cleanup.bash')
	#return to the previous working directory
	os.chdir(cwd)
	print 'Done.'

def haddRecoFiles(sample) :
	name = sample.getShortName()
	print 'hadd-ing reconstructor files for sample '+name
	#first find out where we are right now so we can return here
	cwd = os.getcwd()
	#next move to the reconstructor run directory 
	os.chdir(subprocess.check_output('echo $CMSSW_BASE',shell=True).rstrip('\n')+'/src/Analysis/Reconstructor/test/'+name)
	#get the list of reco files
	includeJEC = len(glob.glob('*JES_up*'))>0 and len(glob.glob('*JES_down*'))>0 and len(glob.glob('*JER_up*'))>0 and len(glob.glob('*JER_down*'))>0  
	listsOfFiles = [glob.glob(name+'_*_tree.root')]
	names = [name]
	if includeJEC :
		i=0
		while i<len(listsOfFiles[0]) :
			thisfile = listsOfFiles[i]
			if thisfile.find('JES')!= -1 or thisfile.find('JER')!=-1 :
				listsOfFiles.pop(i)
			else :
				i+=1
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
			if size_counter==0. or size_counter+nextsize>1000000000. or len(hadd_jobs[len(hadd_jobs)-1][1])>=5000 : #1GB or 5,000 file limit
				hadd_jobs.append(['aggregated_'+name+'_'+str(len(hadd_jobs))+'.root',[]])
				size_counter=0.
			size_counter+=nextsize
			hadd_jobs[len(hadd_jobs)-1][1].append(thisfile)
		#execute the hadd jobs to aggregate files in this directory
		for haddjob in hadd_jobs :
			print haddjob[0]
			#the hadd command
			cmd = 'hadd -f '+haddjob[0]
			for f in haddjob[1] :
				cmd+=' '+f
			os.system(cmd)
		#make sure it ran correctly and if so delete the non-hadded files
		isGood = True
		for haddjob in hadd_jobs :
			if not os.path.isfile(haddjob[0]) :
				isGood = False
				break
		if isGood :
			#print 'yeah I would be deleting files but I am too afraid to leave this in defacto'
			for thisfile in listOfFiles :
				os.system('rm -rf '+thisfile)

def skimRecoFiles(sample) :
	name = sample.getShortName()
	print 'skimming reconstructor files for sample '+name
	#first find out where we are right now so we can return here
	cwd = os.getcwd()
	#next move to the reconstructor run directory 
	os.chdir(subprocess.check_output('echo $CMSSW_BASE',shell=True).rstrip('\n')+'/src/Analysis/Reconstructor/test/'+name)
	#skim files
	os.system('rm -rf *_skim_tree.root')
	includeJEC = len(glob.glob('*JES_up*'))>0 and len(glob.glob('*JES_down*'))>0 and len(glob.glob('*JER_up*'))>0 and len(glob.glob('*JER_down*'))>0  
	filelist = glob.glob('aggregated_'+name+'_*.root')
	if len(filelist)==0 :
		filelist = glob.glob('*_tree.root')
	for i in range(len(filelist)) :
		print ' '+str(i)+': '+str(filelist[i])
		f = TFile(filelist[i]); t = f.Get('tree')
		newname = filelist[i].replace('_tree.root','')+'_skim_tree.root'
		newFile = TFile(newname,'recreate')
		#newTree = t.CopyTree('weight!=0.')
		newTree = t.CopyTree('fullselection==1')
		#newTree = t.CopyTree('eventTopology==1')
		newTree.Write()
		newFile.Close()

def skimHaddRecoFiles(sample) :
	name = sample.getShortName()
	print 'skimming and hadd-ing reconstructor files for sample '+name
	#first find out where we are right now so we can return here
	cwd = os.getcwd()
	#next move to the reconstructor run directory 
	os.chdir(subprocess.check_output('echo $CMSSW_BASE',shell=True).rstrip('\n')+'/src/Analysis/Reconstructor/test/'+name)
	#skim files
	os.system('rm -rf *_skim_tree.root')
	includeJEC = len(glob.glob('*JES_up*'))>0 and len(glob.glob('*JES_down*'))>0 and len(glob.glob('*JER_up*'))>0 and len(glob.glob('*JER_down*'))>0  
	filelist = glob.glob('aggregated_'+name+'_*.root')
	if len(filelist)==0 :
		filelist = glob.glob('*_tree.root')
	for i in range(len(filelist)) :
		print ' '+str(i)+': '+str(filelist[i])
		f = TFile(filelist[i]); t = f.Get('tree')
		newname = filelist[i].replace('_tree.root','')+'_skim_tree.root'
		newFile = TFile(newname,'recreate')
		#newTree = t.CopyTree('weight!=0.')
		newTree = t.CopyTree('fullselection==1')
		#newTree = t.CopyTree('eventTopology==1')
		newTree.Write()
		newFile.Close()
	if includeJEC :
		i = 0
		while i<len(filelist) :
			if filelist[i].find('JES')!=-1 or filelist[i].find('JER')!=-1 or filelist[i].find('skim')!=-1 :
				filelist.pop(i)
			else :
				i+=1
	ifiles = []; ifiles_JES_up = []; ifiles_JES_down = []; ifiles_JER_up = []; ifiles_JER_down = []
	for i in range(len(filelist)) :
		thisfile = filelist[i]
		if i%100==0 :
			cmd = 'hadd -f '+name+'_skim_all_'+str(len(ifiles))+'.root '+thisfile; ifiles.append(name+'_skim_all_'+str(len(ifiles))+'.root')
			if includeJEC :
				tfs = thisfile.split('_')
				cmd_JES_up   = 'hadd -f '+name+'_JES_up_skim_all_'+str(len(ifiles_JES_up))+'.root '+tfs[0]+'_'+tfs[1]+'_JES_up_'+tfs[2]+'_'+tfs[3]
				ifiles_JES_up.append(name+'_JES_up_skim_all_'+str(len(ifiles_JES_up))+'.root')
				cmd_JES_down = 'hadd -f '+name+'_JES_down_skim_all_'+str(len(ifiles_JES_down))+'.root '+tfs[0]+'_'+tfs[1]+'_JES_down_'+tfs[2]+'_'+tfs[3]
				ifiles_JES_down.append(name+'_JES_down_skim_all_'+str(len(ifiles_JES_down))+'.root')
				cmd_JER_up   = 'hadd -f '+name+'_JER_up_skim_all_'+str(len(ifiles_JER_up))+'.root '+tfs[0]+'_'+tfs[1]+'_JER_up_'+tfs[2]+'_'+tfs[3]
				ifiles_JER_up.append(name+'_JER_up_skim_all_'+str(len(ifiles_JER_up))+'.root')
				cmd_JER_down = 'hadd -f '+name+'_JER_down_skim_all_'+str(len(ifiles_JER_down))+'.root '+tfs[0]+'_'+tfs[1]+'_JER_down_'+tfs[2]+'_'+tfs[3]
				ifiles_JER_down.append(name+'_JER_down_skim_all_'+str(len(ifiles_JER_down))+'.root')
		elif (i+1)%100==0 or i==len(filelist)-1 :
			os.system(cmd)
			if includeJEC :
				os.system(cmd_JES_up); os.system(cmd_JES_down); os.system(cmd_JER_up); os.system(cmd_JER_down)
		else :
			cmd+=' '+thisfile
			if includeJEC :
				tfs = thisfile.split('_')
				cmd+=' '+tfs[0]+'_'+tfs[1]+'_JES_up_'+tfs[2]+'_'+tfs[3]
				cmd+=' '+tfs[0]+'_'+tfs[1]+'_JES_down_'+tfs[2]+'_'+tfs[3]
				cmd+=' '+tfs[0]+'_'+tfs[1]+'_JER_up_'+tfs[2]+'_'+tfs[3]
				cmd+=' '++tfs[0]+'_'+tfs[1]+'_JER_down_'+tfs[2]+'_'+tfs[3]
	cmd = 'hadd -f '+name+'_skim_all.root'
	for ifile in ifiles :
		cmd+=' '+ifile
	os.system(cmd)
	if includeJEC :
		cmd_JES_up = 'hadd -f '+name+'_JES_up_skim_all.root'
		for ifile in ifiles_JES_up :
			cmd+=' '+ifile
		os.system(cmd_JES_up)
		cmd_JES_down = 'hadd -f '+name+'_JES_down_skim_all.root'
		for ifile in ifiles_JES_down :
			cmd+=' '+ifile
		os.system(cmd_JES_down)
		cmd_JER_up = 'hadd -f '+name+'_JER_up_skim_all.root'
		for ifile in ifiles_JER_up :
			cmd+=' '+ifile
		os.system(cmd_JER_up)
		cmd_JER_down = 'hadd -f '+name+'_JER_down_skim_all.root'
		for ifile in ifiles_JER_down :
			cmd+=' '+ifile
		os.system(cmd_JER_down)
	if not os.path.isdir('../total_ttree_files') : os.mkdir('../total_ttree_files')
	os.system('mv *_all.root ../total_ttree_files')
	os.system('rm -rf *_skim_*.root')
	#return to the previous working directory
	os.chdir(cwd)
	print 'Done.'
