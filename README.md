# measure-traces by Diana Cohen

## Project definition
Measure the Internet traces in several steps:
* Organize traces' files in data folder (out of the scope).
* Process traces using go, creating metadata files in csv format (outfiles folder is out of the scope as well).
* Generate plots using python from the metadata files that were created in prior step.

## Content desciption
Measure beta - the average frequency per flow within a batch:
* trace_shell.sh - define traces' names along with the corresponding id-length, a set of batch sizes and run the golang scripts.
** run shell file: bash trace_shell.sh
* trace_all.go - generate the basic metadata regarding a given trace, i.e., track the number of flows (distinct items), and the stream's length.
* trace_batch.go - handle a given trace using batches, according to a given batch-size; foreach batch, track the number of flows and compute beta - the average frequency.
* generate_plots.py - each plot reflects beta measurements of a given trace, along with the pre-defined batch sizes; the outputs are provided in our paper.
* use imagemagick command line tool to resize an image file: $ convert <SRC> -resize 20% <DST>

Measure mean relative error:
* error_shell.sh - define traces' names and batch sizes for golang processing
* est_err_batch.go - process the given trace and batch size:
** emulate the crash in different points of trace's timeline
** measure MRE in two aspects: the impact of batch size on diff in estimation error; the impact of +B upon a query after recovery
* generate_bars.py - generate the plots to reflect MRE measurements in both aspects on each round
* the plots are saved as figures in 200 dpi, hence no need in resizing
