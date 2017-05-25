#imports
from optparse import OptionParser
from run_helper import printHelp

#run options
parser = OptionParser()
parser.add_option('--samples', type='string', action='store', dest='samples', help='String of sample groups to act on, separated by double underscores (run with just "help" to get list of options)')
parser.add_option('--actions', type='string', action='store', dest='actions', help='String of actions to perform, separated by double underscores (run with just "help" to get list of options)')
parser.add_option('--username', type='string', action='store', default='eminizer', dest='username', help='Forces direct rebuild of analysis object instead of loading from pickle file')
parser.add_option('--sample_filename', type='string', action='store', default='$CMSSW_BASE/src/Analysis/AnalysisManager/test/samples.csv', dest='sample_filename', help='Name of sample details file to read when building analysis')
parser.add_option('--interactive', type='string', action='store', default='no', dest='interactive', help='"yes"=use prompts to tailor run')
(options, args) = parser.parse_args()

interactive = options.interactive.lower()=='yes'

#first check if we just want help
actionlist = options.actions.split('__')
acitonlist = [x.lower() for x in actionlist]
if interactive or 'help' in actionlist :
	printHelp()
	if len(actionlist)==1 :
		exit()

from run_helper import *

#get the analysis object
this_analysis = buildAnalysis(options.username,options.sample_filename)
if this_analysis==None : 
	print 'ERROR! Could not build analysis.'
	exit()

#for each of the sample groups we want to run
sample_groups = options.samples.split('__')
sample_groups = [x.lower() for x in sample_groups]
for i in range(len(this_analysis.getSampleList())) :
	sample = this_analysis.getSampleList()[i]
	thisGroup = sample.getGroup()
	if 'all' in sample_groups or thisGroup in sample_groups or thisGroup.lower() in sample_groups or str(i) in sample_groups or sample.getShortName().lower() in sample_groups :
		#make sure the user intends to actually operate on this sample
		check = 'yes'
		if interactive :
			check = raw_input('Type "yes" to perform actions on sample %s ---> '%(sample.getShortName()))
		if check.lower()=='yes' :
			#perform the actions
			for action in actionlist :
				if interactive :
					check = raw_input('	Type "yes" to do %s for sample %s ---> '%(action,sample.getShortName()))
				if check.lower()=='yes' :
					this_analysis.doAction(sample,action)

#delete the analysis and quit
del this_analysis