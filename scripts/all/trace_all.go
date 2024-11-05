/***************************************************
* Author: Diana Cohen (sch.diana@gmail.com)
* **************************************************
* handle the entire trace as a single batch (all), counting:
* n - the number of flows (distinct items),
* N - stream length
* also, compute for further analysis:
* beta = (N/n) - the average frequency
*/

package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"os"
)

func main() {
	args := os.Args[1:]
	if len(args) < 1 {
		fmt.Println("Trace parameter is required.")
		return
	}
	// get the trace-name and configure input/output files
	trace := args[0]
	data_dir := "data/"
	out_dir := "outfiles/" + trace + "/"
	batch_size := "all"

	// define headers for detailed and metadata files
	headers := []string{"trace", "N", "n", "beta (N/n)", "idx", "val", "key"}
	headers_meta := []string{"trace", "N", "n", "beta (N/n)"}

	// open the trace (input) file
	infile, err := os.Open(data_dir + trace + ".txt")
	if err != nil {
		fmt.Println("Error opening in-file:", err)
		return
	}
	defer infile.Close()

	// create the detailed (output) file, listing the flows
	outfile, errout := os.Create(fmt.Sprintf("%s%s_%s_flows.csv", out_dir, trace, batch_size))
	if errout != nil {
		fmt.Println("Error opening out-file-flows:", errout)
		return
	}
	defer outfile.Close()

	// create the metadata (output) file, aggregating the data per batch
	outfile_meta, errout_meta := os.Create(fmt.Sprintf("%s%s_%s.csv", out_dir, trace, batch_size))
	if errout_meta != nil {
		fmt.Println("Error opening out-file:", errout_meta)
		return
	}
	defer outfile_meta.Close()

	// write the CSV data, first put a header-row
	writer := csv.NewWriter(outfile)
	defer writer.Flush()
	writer.Write(headers)

	writer_meta := csv.NewWriter(outfile_meta)
	defer writer_meta.Flush()
	writer_meta.Write(headers_meta)

	// Creating a map using make() function.
	// key-value pairs for flow-id (string) and frequency (integer)
	// the batch counter is a non-negative integer, no longer than 16 bits
	var flow_map = make(map[string]uint64)
	flow_index := 1 // 1-based index of a current flow within a batch
	B := 0          // number of currently delayed items within a given batch
	b := 0          // number of currently delayed flows within a given batch

	scanner := bufio.NewScanner(infile) //scan the contents of a file and print line by line
	for scanner.Scan() {
		// read the item-id
		id := scanner.Text()
		// update the frequency
		value, found := flow_map[id]
		if found {
			flow_map[id] = value + 1
		} else { // new flow
			b++
			flow_map[id] = 1
		}
		B++
	}
	if B != 0 { // send/print the partial batch
		// write to metadata file
		data_csv_meta := []string{trace,
			fmt.Sprintf("%d", B),
			fmt.Sprintf("%d", b),
			fmt.Sprintf("%.4f", float64(B)/float64(b))}
		writer_meta.Write(data_csv_meta)

		// write to detailed file
		flow_index = 1 // reset
		// iterate map using for range loop
		for flow_id, frequency := range flow_map {
			flow_csv := []string{fmt.Sprintf("%d", flow_index),
				fmt.Sprintf("%d", frequency),
				fmt.Sprintf(flow_id)}
			data_csv := concatMultipleSlices([][]string{data_csv_meta, flow_csv})
			writer.Write(data_csv)
			
			// next flow
			flow_index++
		}

		// clear leftovers
		for k := range flow_map {
			delete(flow_map, k)
		}
		B = 0
		b = 0
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading from in-file:", err) //print error if scanning is not done properly
	}
}

func concatMultipleSlices[T any](slices [][]T) []T {
	var totalLen int

	for _, s := range slices {
		totalLen += len(s)
	}

	result := make([]T, totalLen)

	var i int

	for _, s := range slices {
		i += copy(result[i:], s)
	}

	return result
}
