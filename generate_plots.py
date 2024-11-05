######################################################################
# Author: Diana Cohen (sch.diana@gmail.com)
######################################################################
# Generate the plots, according to the csv metadata files
# metadata files were created by the golang scripts in prior step
# Each plot reflects beta measurements of a specific trace, along with the pre-defined batch sizes
# 3 important baselines:
# - traffic = theta:		desired values are above (>=)
# - space = theta/alpha:	desired values are above (>=)
# - beta avg:				desired values are below (<=)
######################################################################

# importing the module
import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt 
import seaborn as sns
import scipy
from matplotlib.lines import Line2D

#get from command line
args = sys.argv[1:]
if (len(args) < 1):
	sys.exit("Usage: [prog] [alpha]")

# get alpha - the load threshold of a data structure
alpha = float(args[0])

# update the overall font size for plot
plt.rcParams.update({'font.size':16})

# define the traces and different batch sizes as in prior step in trace_shell.sh
traces = ['Chicago16Small', 'Chicago1610Mil', 'ny19A', 'ny19B', 'SJ14.small']
batches = [50, 100, 250, 500, 1000, 2000, 4000]

# define the desired percentiles to peocessing
percs_limit = [5, 25, 50, 75, 95]

# the metadata files were created there
data_dir = "outfiles/"

# main columns of our interest
col_theta = "theta (1+cnt_len/id_len)"
col_beta = "beta (B/b)"

rows, cols = (9, 8)

# deal with different batches of a single trace at a time
for trace in traces:
	arr = [[0 for i in range(cols)] for j in range(rows)]

	# fixed headers for each sample-set associated with a particular batch-size
	arr[0][0] = "batch-size"
	arr[1][0] = "traffic"
	arr[2][0] = "space"
	arr[3][0] = "avg"
	arr[4][0] = "5%"
	arr[5][0] = "25%"
	arr[6][0] = "50%"
	arr[7][0] = "75%"
	arr[8][0] = "95%"

	j = 1
	for batch in batches:
		arr[0][j] = str(batch)
		# read specific columns of csv file using Pandas
		data_batch = pd.read_csv(data_dir + trace + "/" + trace + "_" + str(batch) + ".csv", usecols = [col_beta, col_theta])

		# converting column data to list
		betas = data_batch[col_beta].tolist()
		
		# extract the values of the 3 baselines
		betas_avg = round(np.average(betas), 4)
		theta = data_batch[col_theta].tolist()[0]
		space = round(theta / alpha, 4)

		arr[1][j] = theta
		arr[2][j] = space
		arr[3][j] = betas_avg

		# generate the percentiles values from betas
		beta_perc_limit = [np.percentile(betas, p) for p in percs_limit]
		for i in range(4,9):
			arr[i][j] = beta_perc_limit[i-4]
		
		# next batch-size, new column
		j += 1

	# print("trace: " + trace)
	# for row in arr:
	# 	print(row)

	df = pd.DataFrame({arr[i][0]: arr[i][1:] for i in range(rows)})
	# set first column as index
	df = df.set_index(arr[0][0])
	#print(df)

	# print("theta = " + str(theta) + "; theta/alpha = " + str(space) + "; beta_avg = " + str(betas_avg))
	# print(percs_limit)
	# print(beta_perc_limit)

	#df.plot.line()
	fig, ax = plt.subplots()
	df.plot.line(ax=ax, y='traffic', color='k', ls=':', lw=2)
	df.plot.line(ax=ax, y='space', color='k', ls='-.', lw=2)
	df.plot.line(ax=ax, y='avg', color='k', ls='--', lw=2)
	df.plot.line(ax=ax, y=[arr[i][0] for i in range(8,3,-1)], marker='.')

	xx=arr[0][1:] # batch-size
	y_traffic=arr[1][1:]
	y_space=arr[2][1:]
	y_avg=arr[3][1:]
	y_05=arr[4][1:]
	
	# the widest desired area for efficient traffic: beta >= traffic baseline
	where_avg=[y_avg[i] >= y_traffic[i] for i in range(len(xx))]
	# the narrow area for "overall" space efficiency: beta_5% >= space baseline
	where_05=[y_05[i] >= y_space[i] for i in range(len(xx))]
	# fill the desired areas with color
	plt.fill_between(x=xx, y1=y_traffic, y2=y_avg, where=where_avg, interpolate=True, color='y', alpha=.1)
	plt.fill_between(x=xx, y1=y_space, y2=y_05, where=where_05, interpolate=True, color='m', alpha=.1)

	plt.title(trace, fontsize=24)
	plt.xlabel('batch size', fontsize=20)
	plt.ylabel(r'$\beta=\frac{B}{b}$', fontsize=22)#, rotation=0)

	# the output file to be created for a plot
	figpath = 'plots/beta_' + trace + '_' + str(alpha) + '.png'

	#plt.legend(loc='lower right')
	plt.legend().set_visible(False) # we shall create a separate file for legend
	plt.grid()

	plt.savefig(figpath, bbox_inches="tight", pad_inches=0, dpi=600)
	#plt.show()

# now save the legend in separate file
# need to empty everything but the legend
plt.axis('off')
plt.title('')
plt.fill_between(x=xx, y1=y_traffic, y2=y_avg, where=where_avg, interpolate=True, color='w')
plt.fill_between(x=xx, y1=y_space, y2=y_05, where=where_05, interpolate=True, color='w')

# prepare the nice centered legend
plt.legend().set_visible(True)
plt.legend(loc='center', title='Legend')

# continue to empty unnecessary graphics
for line in ax.get_lines(): # ax.lines:
    line.remove()

# finally, save the legent in file
plt.savefig('plots/beta_legend.png', bbox_inches="tight", pad_inches=0, dpi=600)
