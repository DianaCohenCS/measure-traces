######################################################################
# Author: Diana Cohen (sch.diana@gmail.com)
######################################################################
# Generate the plots, according to the csv metadata files
# metadata files were created by the golang scripts in prior step
######################################################################

# importing the module
import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt 
import seaborn as sns
import scipy
from matplotlib.lines import Line2D

# update the overall font size for plot
plt.rcParams.update({'font.size':16})

# define the traces and different batch sizes as in prior step in trace_shell.sh
#traces = ['Chicago16Small', 'Chicago1610Mil', 'ny19A', 'ny19B', 'SJ14.small']
#batches = [50, 100, 250, 500, 1000, 2000, 4000]
# just use some subset, according to the prior stage of processing
traces = ['Chicago1610Mil', 'ny19A', 'SJ14.small']
batches = [100, 500, 2000, 4000]

# the metadata files were created there
data_dir = "outfiles/"

# main columns of our interest
col_ni = "Ni"
col_cms_true = "cms_true"
col_rec_cms = "rec_cms"
col_rec_true = "rec_true"
col_hist_true = "hist_true"

# main labels
lbl_cms = r'$\hat{c_x}$ vs $c_x$'
lbl_hist = r'$\hat{c_x}^{(t)}$ vs $c_x$'
lbl_diff = r'$(\hat{c_x}^{(t)} + B)$ vs $\hat{c_x}$'
lbl_rec = r'$(\hat{c_x}^{(t)} + B)$ vs ${c_x}$'

# deal with different batches of a single trace at a time
for trace in traces:
	for batch in batches:
		title = trace + ", B=" + str(batch)
		# read specific columns of csv file using Pandas
		data_batch = pd.read_csv(data_dir + trace + "/" + trace + "_" + str(batch) + "_error.csv", usecols = [col_ni, col_cms_true, col_rec_cms, col_rec_true, col_hist_true])
		# the output file to be created for a plot
		figpath = 'plots/MRE_backup_' + trace + '_' + str(batch) + '.png'
		figpath2 = 'plots/MRE_recovery_' + trace + '_' + str(batch) + '.png'

		# Defining categories and values for the groups
		# converting column data to list
		categories = data_batch[col_ni].tolist() #read Ni from file
		# plot MRE backup - cms vs true, backup cms vs true
		values_cms = data_batch[col_cms_true].tolist() #bottom - read cms_true
		values_hist = data_batch[col_hist_true].tolist() #next - read hist_true
		# plot MRE recovery - impact of adding B upon query
		values_diff = data_batch[col_rec_cms].tolist() #up - read rec_cms
		values_rec = data_batch[col_rec_true].tolist() #next - read rec_true = hist+B

		# Setting the width of the bars 
		bar_width = 0.4
		# Calculating bar positions for both groups
		bar_cms = np.arange(len(categories))
		bar_hist = bar_cms + bar_width
		bar_diff = bar_cms
		bar_rec = bar_diff + bar_width
		# format xlabels
		xlabels = [f'{label:,}' for label in categories]
		
		fig, ax = plt.subplots()
		
		# Adding labels to the axes
		plt.xlabel('Crash after Item#', fontsize=20)
		plt.ylabel('Mean Relative Error', fontsize=22)

		# Adding a title to the graph
		plt.title(title, fontsize=24)

		plt.xticks(np.arange(0, 9, step=1), labels=xlabels, rotation=75)

		# Group 1 - MRE backup
		# bCMS = plt.bar(bar_cms, values_cms, width=bar_width, label='CMS vs true', align='edge')
		# bHist = plt.bar(bar_hist, values_hist, width=bar_width, label='Backup vs true', align='edge')
		bCMS = plt.bar(bar_cms, values_cms, width=bar_width, label=lbl_cms, align='edge')
		bHist = plt.bar(bar_hist, values_hist, width=bar_width, label=lbl_hist, align='edge')

		# Displaying a legend to identify the groups
		plt.legend()
		plt.grid()

		#plt.savefig(figpath, bbox_inches="tight", pad_inches=0, dpi=600)
		plt.savefig(figpath, bbox_inches="tight", pad_inches=0, dpi=200) # reduce upon creation
		# Showing the plot
		#plt.show()

		# Group 2 - MRE recovery
		# empty unnecessary graphics
		bCMS.remove()
		bHist.remove()
		# bDiff = plt.bar(bar_diff, values_diff, width=bar_width, label='Recovery vs CMS', align='edge')
		# bRec = plt.bar(bar_rec, values_rec, width=bar_width, label='Recovery vs true', align='edge')
		bDiff = plt.bar(bar_diff, values_diff, width=bar_width, label=lbl_diff, align='edge')
		bRec = plt.bar(bar_rec, values_rec, width=bar_width, label=lbl_rec, align='edge')
		plt.legend(loc='lower right')
		plt.savefig(figpath2, bbox_inches="tight", pad_inches=0, dpi=200)
