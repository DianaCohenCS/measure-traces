/***************************************************
* Author: Diana Cohen (sch.diana@gmail.com)
* **************************************************
* handle the trace using batches, measuring estimation error after recovery
* ability to recover the latest backup, where only the last batch is lost
* latest backup is at time (batch) t, at time t+1 we discover a crash (up to B items have been lost)
* N - number of overall items within a trace: data dependent, discover N on first pass
* B - batch size: 100, 500, 1000, 4000
* failed batch of a trace, as a percentile: 1/3, 1/2 or 2/3
* failed item of a failed batch, as a percentile: 0.1, 0.5 or 0.9

* true values are handled by maps:
* - flow_map from the beginning of a trace up until the crash
* - curr_map of the current batch (can find out exctly how many losses)
* estimations are handled by Count-Min Sketches:
* - cms_curr of the current batch, and
* - cms_hist from the beginning of a trace up until the current batch

* for each flow x (from the beginning):
* - true frequency at time of crash: flow_map[x]
* - estimation after recovery: cms_hist.Estimate(x) + B
* - estimation at time of crash: cms_hist.Estimate(x) + cms_curr.Estimate(x)
 */

package main

import (
	"bufio"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	//"hash/fnv"
	//"github.com/cespare/xxhash/v2"
	"hash/maphash"
	"math"
	"os"
	"strconv"
	"strings"
)

func main() {
	/* ****************************************
	** handle arguments
	**************************************** */
	args := os.Args[1:]
	if len(args) < 2 {
		fmt.Println("Usage: [prog] [trace-name] [batch-size]")
		return
	}
	// get the trace-name and batch-size
	trace := args[0]
	B, err := strconv.Atoi(args[1])
	if err != nil {
		fmt.Println("Error converting batch size")
		return
	}

	/* ****************************************
	** define constants
	**************************************** */
	// emulate crash after
	failed_batches := []float32{1 / 3.0, 0.5, 2 / 3.0}
	failed_items := []float32{0.1, 0.5, 0.9}
	// CMS user-params
	epsilon := math.Pow10(-6)
	delta := math.Pow10(-2)
	// configure input/output files
	data_dir := "data/"
	out_dir := "outfiles/" + trace + "/"
	// define header for file: Nt - latest backup item, Ni - latest non-failed item
	headers_meta := []string{"N", "n", "Nt", "Ni", "rec_cms", "rec_true", "cms_true", "hist_true"}

	/* ****************************************
	** get stream size N by counting non-empty lines
	**************************************** */
	// open the trace (input) file
	infile, err := os.Open(data_dir + trace + ".txt")
	if err != nil {
		fmt.Println("Error opening in-file:", err)
		return
	}
	defer infile.Close()

	// get N by counting non-empty lines
	N := 0                              // number of items within a stream
	scanner := bufio.NewScanner(infile) //scan the contents of a file and print line by line
	for scanner.Scan() {
		// read the item-id
		id := scanner.Text()
		if len(strings.TrimSpace(id)) > 0 {
			N++
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading from in-file:", err) //print error if scanning is not done properly
	}
	if N == 0 {
		fmt.Println("in-file contains no data")
		return
	}

	/* ****************************************
	** prepare out-file
	**************************************** */
	// create the metadata (output) file, aggregating the data per failing item
	outfile_meta, errout_meta := os.Create(fmt.Sprintf("%s%s_%d_error.csv", out_dir, trace, B))
	if errout_meta != nil {
		fmt.Println("Error opening out-file:", errout_meta)
		return
	}
	defer outfile_meta.Close()
	// write the CSV data, first put a header-row
	writer_meta := csv.NewWriter(outfile_meta)
	defer writer_meta.Flush()
	writer_meta.Write(headers_meta)

	/* ****************************************
	** create data structures to track the trace
	**************************************** */
	// Creating a map using make() function.
	// key-value pairs for flow-id (string) and frequency (integer)
	flow_map := make(map[string]int)                  // overall
	curr_map := make(map[string]int)                  // within a failed batch
	cms_hist, err := NewWithEstimates(epsilon, delta) // accumulative CMS
	checkerr(err)
	depth := cms_hist.getDepth() // matrix dimensions based on (epsilon, delta)
	width := cms_hist.getWidth()
	fmt.Printf("ε: %f, δ: %f -> d: %d, w: %d\n", epsilon, delta, depth, width)

	q := (N / B)                         // calculate the number of whole batches (floor)
	item_idx := 0                        // latest item# before crash
	// second round
	// back to the beginning of the file
	infile.Seek(0, io.SeekStart)
	scanner = bufio.NewScanner(infile) //scan the contents of a file and print line by line

	for _, fb := range failed_batches { // don't care about index
		t := int(float32(q) * fb) // latest backup batch#
		Nt := t * B               // latest backup item#

		// fill the CMS up to the latest backup
		for item_idx < Nt {
			// readline from file into id
			if scanner.Scan() {
				id := scanner.Text()
				if len(strings.TrimSpace(id)) > 0 {
					// update the frequency
					flow_map[id]++         // true frequency
					cms_hist.Update(id, 1) // CMS after recovery = latest backup
					item_idx++
				}
			}
		}

		// handle failed batch using cms_curr
		cms_curr, _ := New(depth, width) // tmp CMS for current batch
		cms_curr.CopySeeds(cms_hist)     // use the same seeds for all
		for _, fi := range failed_items {
			Ni := Nt + int(float32(B)*fi) // latest item# before crash

			// this is a failed batch
			for item_idx < Ni {
				// readline from file into id
				if scanner.Scan() {
					id := scanner.Text()
					if len(strings.TrimSpace(id)) > 0 {
						// update the frequency
						flow_map[id]++         // true frequency until crash
						curr_map[id]++         // true frequency within the batch
						cms_curr.Update(id, 1) // diff matrix of lost batch
						item_idx++

						//fmt.Printf("id: %s, true: %d, extimation: %d\n", id, flow_map[id], cms_hist.Estimate(id)+cms_curr.Estimate(id))
					}
				}
			}

			// EVALUATE ERROR
			rec_cms := 0.0
			rec_true := 0.0
			cms_true := 0.0
			hist_true := 0.0
			n := 0 // number of keys in map
			// iterate map using for range loop
			//for x, c_x := range curr_map {
			for x, c_x := range flow_map {
				history_c_x := cms_hist.Estimate(x)                         // estimation of latest backup
				hat_c_x := history_c_x + cms_curr.Estimate(x)               // estimation up until the crash
				recovery_c_x := history_c_x + B                             // ensure one sided error
				rec_cms += float64(recovery_c_x-hat_c_x) / float64(hat_c_x) // what is the impact of +B?
				rec_true += float64(recovery_c_x-c_x) / float64(c_x)        // how far from ground truth?
				cms_true += float64(hat_c_x-c_x) / float64(c_x)
				hist_true += float64(history_c_x-c_x) / float64(c_x)
				n++
			}
			flows := float64(n)
			// write to file: trace_batch [N, n, Nt, Ni, rec_cms, rec_true, cms_true, hist_true]
			batch_csv := []string{
				fmt.Sprintf("%d", N),
				fmt.Sprintf("%d", n),
				fmt.Sprintf("%d", Nt),
				fmt.Sprintf("%d", Ni),
				fmt.Sprintf("%.8f", (rec_cms / flows)),
				fmt.Sprintf("%.8f", (rec_true / flows)),
				fmt.Sprintf("%.8f", (cms_true / flows)),
				fmt.Sprintf("%.8f", (hist_true / flows))}
			writer_meta.Write(batch_csv)
		}
		// catchup the failed batch into history
		cms_hist.Merge(cms_curr)
		cms_curr.Clear()
		// clear leftovers
		for k := range curr_map {
			delete(curr_map, k)
		}
	}
	// by the end of run we have a file with 3*3 rows
	// this will be used for bar plot
	// clear leftovers
	for k := range flow_map {
		delete(flow_map, k)
	}
}

func checkerr(err error) {
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

/*
* implement the Count-Min Sketch in go:
* epsilon, delta - input parameters for CMS
* d - number of rows in CMS - ceil(ln(1/delta)), for each row there is a hash function
* w - number of counters per each row - ceil(e/epsilon)
 */

// Count-Min Sketch struct.
type CMS struct {
	d     int
	w     int
	count [][]int
	seeds []maphash.Seed
}

// New is a constructor that creates a new Count-Min Sketch with d X w matrix of counters
func New(d, w int) (cms *CMS, err error) {
	if d <= 0 || w <= 0 {
		return nil, errors.New("CMS: d and w must be greater than 0")
	}

	cms = &CMS{
		d:     d,
		w:     w,
		count: make([][]int, d),
		seeds: make([]maphash.Seed, d),
	}
	for i := 0; i < d; i++ {
		cms.count[i] = make([]int, w)
		cms.seeds[i] = maphash.MakeSeed()
	}

	return cms, nil
}

// NewWithEstimates creates a new Count-Min Sketch with given error rate and confidence.
// Accuracy guarantees will be made in terms of a pair of user specified parameters,
// ε and δ, meaning that the error in answering a query is within a factor of ε with
// probability at least (1-δ)
func NewWithEstimates(epsilon, delta float64) (*CMS, error) {
	if epsilon <= 0 || epsilon >= 1 {
		return nil, errors.New("CMS: epsilon must be in range of (0, 1)")
	}
	if delta <= 0 || delta >= 1 {
		return nil, errors.New("CMS: delta must be in range of (0, 1)")
	}

	d, w := dimensions(epsilon, delta)
	// fmt.Printf("ε: %f, δ: %f -> d: %d, w: %d\n", epsilon, delta, d, w)

	return New(d, w)
}

// Update the frequency of a given key
func (cms *CMS) Update(key string, cnt int) {
	for i := 0; i < cms.d; i++ {
		j := cms.hash(key, i)
		cms.count[i][j] += cnt
	}
}

// Estimate the frequency of a key. This is a point query.
func (cms *CMS) Estimate(key string) int {
	min := math.MaxInt
	for i := 0; i < cms.d; i++ {
		j := cms.hash(key, i)
		value := cms.count[i][j]
		if value < min {
			min = value
		}
	}
	return min
}

// Merge other CMS into a current CMS by adding the corresponding counts
func (curr *CMS) Merge(other *CMS) error {
	if curr.d != other.d || curr.w != other.w {
		return errors.New("CMS: matrix dimensions must match")
	}

	for i := 0; i < curr.d; i++ {
		for j := 0; j < curr.w; j++ {
			curr.count[i][j] += other.count[i][j]
		}
	}
	return nil
}

// Copy seeds from other CMS
func (curr *CMS) CopySeeds(other *CMS) {
	for i := 0; i < curr.d; i++ {
		curr.seeds[i] = other.seeds[i]
	}
}

func (cms *CMS) Clear() {
	for i := 0; i < cms.d; i++ {
		for j := 0; j < cms.w; j++ {
			cms.count[i][j] = 0
		}
	}
}

// func (cms *CMS) hash(key string, seed uint) uint {
// 	seed += 1
// 	// use xxhash64 to hash key using row index as a seed
// 	h := xxhash.New()
// 	h.Write([]byte(key))
// 	h.Write([]byte{byte(seed)})
// 	return uint(h.Sum64() % uint64(cms.w))

//		// h := fnv.New64a()
//		// h.Write([]byte(key))
//		// h.Write([]byte{byte(seed), byte(seed >> 8), byte(seed >> 16), byte(seed >> 24)})
//		// return uint(h.Sum64() % uint64(cms.w))
//	}
func (cms *CMS) hash(key string, row int) int {
	h := maphash.Hash{}
	h.SetSeed(cms.seeds[row])
	h.WriteString(key)
	return int(h.Sum64() % uint64(cms.w))
}

// calculate the matrix dimensions based on user params (epsilon, delta)
func dimensions(epsilon, delta float64) (d int, w int) {
	// math.Log is actually a ln (natural log)
	d = int(math.Ceil(math.Log(1.0 / delta)))
	w = int(math.Ceil(math.E / epsilon))
	return
}

// D returns the number of hashing functions
func (cms *CMS) getDepth() int {
	return cms.d
}

// W returns the size of hashing functions
func (cms *CMS) getWidth() int {
	return cms.w
}
