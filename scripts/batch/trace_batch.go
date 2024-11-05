/***************************************************
* Author: Diana Cohen (sch.diana@gmail.com)
* **************************************************
* handle the trace using batches, counting:
* b - the number of flows (distinct items) within a batch,
* B - the number of items
* also, compute for further analysis:
* theta = (1 + len(counter)/len(flow-id))
* beta = (B/b) - the average frequency
*/

package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"math"
	"os"
	"strconv"
)

func main() {
	args := os.Args[1:]
	if len(args) < 3 {
		fmt.Println("Usage: [prog] [trace-name] [batch-size] [id-len]")
		return
	}
	// get the trace-name, batch-size and bit-length of identifier
	trace := args[0]
	batch_size, err := strconv.Atoi(args[1])
	if err != nil {
		fmt.Println("Error converting batch size")
		return
	}
	id_length, err := strconv.Atoi(args[2])
	if err != nil {
		fmt.Println("Error converting id length")
		return
	}

	// configure input/output files
	data_dir := "data/"
	out_dir := "outfiles/" + trace + "/"

	// compute the counter bit-length and corresponding theta value as a threshold
	cnt_length := math.Ceil(math.Log2(float64(batch_size)))
	theta := 1 + cnt_length/float64(id_length)

	// define headers for detailed and metadata files
	headers := []string{"trace", "batch size", "counter len", "id len", "theta (1+cnt_len/id_len)",
		"batch#", "B", "b", "beta (B/b)", "idx", "val", "key"}
	headers_meta := []string{"trace", "batch size", "counter len", "id len", "theta (1+cnt_len/id_len)",
		"batch#", "B", "b", "beta (B/b)"}

	// open the trace (input) file
	infile, err := os.Open(data_dir + trace + ".txt")
	if err != nil {
		fmt.Println("Error opening in-file:", err)
		return
	}
	defer infile.Close()

	// create the detailed (output) file, listing the batches and the associated flows
	outfile, errout := os.Create(fmt.Sprintf("%s%s_%d_flows.csv", out_dir, trace, batch_size))
	if errout != nil {
		fmt.Println("Error opening out-file-flows:", errout)
		return
	}
	defer outfile.Close()

	// create the metadata (output) file, aggregating the data per batch
	outfile_meta, errout_meta := os.Create(fmt.Sprintf("%s%s_%d.csv", out_dir, trace, batch_size))
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

	// generate trace + batch-size specific header
	trace_csv := []string{trace,
		fmt.Sprintf("%d", batch_size),
		fmt.Sprintf("%d", int(cnt_length)),
		fmt.Sprintf("%d", int(id_length)),
		fmt.Sprintf("%.4f", theta)}
	// trace_row := fmt.Sprintf("trace: %s, batch size: %d, counter len: %d, id len: %d, theta (1+cnt_len/id_len): %.4f", trace, batch_size, int(cnt_length), int(id_length), theta)
	// fmt.Println(trace_row)
	// fmt.Println("==========")

	// Creating a map using make() function.
	// key-value pairs for flow-id (string) and frequency (integer)
	// the batch counter is a non-negative integer, no longer than 16 bits
	var flow_map = make(map[string]uint16)
	batch_index := 1 // 1-based index of a current batch
	flow_index := 1  // 1-based index of a current flow within a batch
	B := 0           // number of currently delayed items within a given batch
	b := 0           // number of currently delayed flows within a given batch

	scanner := bufio.NewScanner(infile) //scan the contents of a file and print line by line
	for scanner.Scan() {
		if B >= batch_size { // send/print the full batch
			// write to metadata file
			batch_csv := []string{fmt.Sprintf("%d", batch_index),
				fmt.Sprintf("%d", B),
				fmt.Sprintf("%d", b),
				fmt.Sprintf("%.4f", float64(B)/float64(b))}
			// batch_row := fmt.Sprintf("batch#: %d, B: %d, b: %d, beta (B/b): %.4f", batch_index, B, b, float64(B)/float64(b))
			// fmt.Println(batch_row)
			// fmt.Println("----------")
			data_csv_meta := concatMultipleSlices([][]string{trace_csv, batch_csv})
			writer_meta.Write(data_csv_meta)

			// write to detailed file
			flow_index = 1 // reset
			// iterate map using for range loop
			for flow_id, frequency := range flow_map {
				flow_csv := []string{fmt.Sprintf("%d", flow_index),
					fmt.Sprintf("%d", frequency),
					fmt.Sprintf(flow_id)}
				// flow_row := fmt.Sprintf("idx: %d, val: %d, key: %s", flow_index, frequency, flow_id)
				// fmt.Println(flow_row)
				data_csv := concatMultipleSlices([][]string{trace_csv, batch_csv, flow_csv})
				writer.Write(data_csv)

				// next flow
				flow_index++
			}
			// fmt.Println("----------")

			// clear all elements from map
			for k := range flow_map {
				delete(flow_map, k)
			}
			B = 0
			b = 0

			// next batch
			batch_index++
		}
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

	// handle the remainder
	if B != 0 { // send/print the partial batch
		// TODO: need to decide whether to include the partial (last) batch

		// write to metadata file
		batch_csv := []string{fmt.Sprintf("%d", batch_index),
			fmt.Sprintf("%d", B),
			fmt.Sprintf("%d", b),
			fmt.Sprintf("%.4f", float64(B)/float64(b))}
		// batch_row := fmt.Sprintf("batch#: %d, B: %d, b: %d, beta (B/b): %.4f", batch_index, B, b, float64(B)/float64(b))
		// fmt.Println(batch_row)
		// fmt.Println("----------")
		data_csv_meta := concatMultipleSlices([][]string{trace_csv, batch_csv})
		writer_meta.Write(data_csv_meta)
		
		// write to detailed file
		flow_index = 1 // reset
		// iterate map using for range loop
		for flow_id, frequency := range flow_map {
			flow_csv := []string{fmt.Sprintf("%d", flow_index),
				fmt.Sprintf("%d", frequency),
				fmt.Sprintf(flow_id)}
			// flow_row := fmt.Sprintf("idx: %d, val: %d, key: %s", flow_index, frequency, flow_id)
			// fmt.Println(flow_row)
			data_csv := concatMultipleSlices([][]string{trace_csv, batch_csv, flow_csv})
			writer.Write(data_csv)
			
			// next flow
			flow_index++
		}

		// DON'T FORGET TO: 
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
