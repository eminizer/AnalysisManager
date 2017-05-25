#printHelp prints the help text for the code I guess. Mostly list of possible actions.
def printHelp() :
	print '-------------------------------------------------------------------------------------------------------------'
	print 'This code is used to keep track of a bunch of analysis details and perform common tasks on many files at once'
	print 'Here are the available actions, run this with each one separated by an underscore: '
	print '	haddNTuples: for each sample, hadd files in the nTuple subdirectories into larger files of about 5GB each,'
	print '	             transferring the results back to the sample top directory on EOS'
	print '	setupRecoRuns: for each sample, setup up a new Reconstructor directory with scripts, input file, and list of'
	print '	               jobs based on the aggregated files. DOES NOT submit jobs (and JEC wiggles are hardcoded).'
	print '	runReco: submit the Reconstructor jobs in already-created Reconstructor run directories to Condor'
	print '	findfailedjobs: make a new ana.listOfJobs for each sample that includes the jobs that failed'
	print '	haddrecofiles: aggregate the individual reconstructor run output files to larger ones'
	print '	skimrecofiles: skim the aggregated reconstructor output files individually; skim cut is hardcoded sorry'
	print '	skimhaddrecofiles: skim the aggregated reconstructor output files, hadd them, move them to the total files'
	print '	                   directory.'
	print '-------------------------------------------------------------------------------------------------------------'

import os
import subprocess
import csv
from analysis import Analysis

#buildAnalysis builds the initial analysis from the sample csv file
def buildAnalysis(uname,sfname) :
	print 'Building analysis object from sample filename %s...'%(sfname)
	if not sfname.endswith('.csv') :
		sfname+='.csv'
	if sfname.startswith('$CMSSW_BASE') :
		sfname = subprocess.check_output('echo $CMSSW_BASE',shell=True).rstrip('\n')+sfname[len('$CMSSW_BASE'):]
	#initialize the new analysis
	newAnalysis = Analysis(uname)
	#check for the sample file
	if not os.path.isfile(sfname) :
		print 'ERROR! cannot load sample file %s (file not found)'%(sfname)
		return None
	#open it
	sfile = open(sfname,'rU')
	#declare the reader and get the first line's list of categories
	reader = csv.reader(sfile)
	categories = next(reader)
	#read the rest of the lines
	for row in reader :
		if (not len(row)>0) or row[0]=='' : continue
		newAnalysis.addSampleFromFileInfo(categories,row)
	print 'Done building analysis object.'
	return newAnalysis
