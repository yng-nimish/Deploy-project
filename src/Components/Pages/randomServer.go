package rng

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/maphash"
	"io"
	"log"
	"math"
	"math/rand"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Random server that serves multiple RNG handlers
type RandomServer struct {
	// File readers corresponding to each HWRNG
	rngs            []RandomBufferedReader
	readers_per_rng int

	// Correctness score for the HWRNGs, taking a running average of the success rates of RNG tests
	rng_correctness []float64
	rng_test_log    []BitRing
	rng_names       []string
	rng_last_failed []time.Time
}

func (server *RandomServer) Close() {
	for i := range server.rngs {
		server.rngs[i].Close()
	}
}

// Maintain a history of past tests
const HISTORY_LENGTH = 20
const CORRECTNESS_THRESHOLD = 0.90

func (server *RandomServer) Setup(names []string, threads_per_file int) error {
	server.rngs = make([]RandomBufferedReader, len(names)*threads_per_file)
	server.rng_names = make([]string, len(names))

	// Set up testing logs for each RNG
	server.rng_correctness = make([]float64, len(names))
	server.rng_test_log = make([]BitRing, len(names))
	server.rng_last_failed = make([]time.Time, len(names))
	server.readers_per_rng = threads_per_file

	for i, name := range names {
		for j := 0; j < threads_per_file; j++ {
			err := server.rngs[i*threads_per_file+j].Setup(name)
			if err != nil {
				return err
			}
		}
		log.Printf("Set up for %d threads reading %s", threads_per_file, name)

		server.rng_test_log[i].init(HISTORY_LENGTH)
		server.rng_names[i] = name
	}

	return nil
}

func (server *RandomServer) PickRng() (int, error) {
	var backend = int(new(maphash.Hash).Sum64() % uint64(len(server.rngs)))

	var found_rng = false
	for i := 0; i < len(server.rng_correctness); i++ {
		if server.rng_correctness[backend/server.readers_per_rng] >= CORRECTNESS_THRESHOLD {
			found_rng = true
			break
		}

		// Advance to the next RNG
		backend = (backend + server.readers_per_rng) % len(server.rngs)
	}

	if found_rng == false {
		return 0, errors.New("No good RNGs")
	}

	return backend, nil
}

func (server *RandomServer) ServeData(w http.ResponseWriter, r *http.Request) {
	if len(r.Method) > 0 && r.Method != "GET" {
		http.Error(w, "Non-GET requests are not supported at this endpoint", 405)
		return
	}

	query := r.URL.Query()

	// Length of the data requested
	var length = 64
	if query.Has("len") || query.Has("l") {
		var requested_len_str = query.Get("len")
		if len(requested_len_str) == 0 {
			requested_len_str = query.Get("l")
		}

		requested_len, err := strconv.Atoi(requested_len_str)
		if err != nil {
			log.Printf("Error parsing length [%s] in query for data: %s", requested_len_str, err)
			http.Error(w, fmt.Sprintf("Bad length: %s", requested_len_str), 400)
			return
		}
		length = requested_len
	}

	backend, err := server.PickRng()
	if err != nil {
		log.Printf("Internal RNG server error: %s", err)
		http.Error(w, fmt.Sprintf("Internal error: %s", err), 500)
		return
	}

	// Get a buffer of bytes
	var buffer = make([]byte, length)
	if err := server.rngs[backend].ReadBytes(buffer); err != nil {
		log.Printf("Internal RNG server error: %s", err)
		http.Error(w, fmt.Sprintf("Internal error: %s", err), 500)
		return
	}

	// Get the user-requested encoding for the return data
	var req_encoding string
	if query.Has("encoding") {
		req_encoding = strings.ToLower(query.Get("encoding"))
	} else if query.Has("enc") {
		req_encoding = strings.ToLower(query.Get("enc"))
	} else if query.Has("e") {
		req_encoding = strings.ToLower(query.Get("e"))
	} else {
		req_encoding = "base64"
	}

	// Encode the data
	var encoded_buf []byte
	switch req_encoding {
	case "base64", "b64":
		encoded_buf = make([]byte, base64.StdEncoding.EncodedLen(len(buffer)))
		base64.StdEncoding.Encode(encoded_buf, buffer)
	case "h", "hex", "hexadecimal":
		encoded_buf = make([]byte, hex.EncodedLen(len(buffer)))
		hex.Encode(encoded_buf, buffer)
	case "raw":
		encoded_buf = buffer
	default:
		log.Printf("Error parsing encoding [%s] in query for data: %s", req_encoding, err)
		http.Error(w, fmt.Sprintf("Bad encoding: %s", req_encoding), 400)
		return
	}

	w.Write(encoded_buf)
}

func (server *RandomServer) ServeNumber(w http.ResponseWriter, r *http.Request) {
	if len(r.Method) > 0 && r.Method != "GET" {
		http.Error(w, "Non-GET requests are not supported at this endpoint", 405)
		return
	}

	query := r.URL.Query()

	// Parse the base if it is given
	var base = 10
	if query.Has("base") {
		requested_base := query.Get("base")

		user_base, err := strconv.Atoi(requested_base)
		if err != nil {
			log.Printf("Error parsing base [%s] in query: %s", requested_base, err)
			http.Error(w, fmt.Sprintf("Bad base: %s", requested_base), 400)
			return
		}
		base = user_base
	}

	// Range check the base
	if base < 2 || base > 36 {
		http.Error(w, fmt.Sprintf("Invalid base (can be between 2 and 36): %d", base), 400)
		return
	}

	// Parse the floor
	var floor int64 = 0
	if query.Has("floor") || query.Has("f") {
		var requested_floor = query.Get("floor")
		if len(requested_floor) == 0 {
			requested_floor = query.Get("f")
		}

		user_floor, err := strconv.ParseInt(requested_floor, base, 64)
		if err != nil {
			log.Printf("Error parsing floor [%s] in query: %s", requested_floor, err)
			http.Error(w, fmt.Sprintf("Bad floor: %s", requested_floor), 400)
			return
		}
		floor = user_floor
	}

	// Parse the limit
	var limit int64 = math.MaxInt64
	if query.Has("limit") || query.Has("lim") || query.Has("l") {
		var requested_lim = query.Get("limit")
		if len(requested_lim) == 0 {
			requested_lim = query.Get("lim")
		}
		if len(requested_lim) == 0 {
			requested_lim = query.Get("l")
		}

		user_lim, err := strconv.ParseInt(requested_lim, base, 64)
		if err != nil {
			log.Printf("Error parsing limit [%s] in query: %s", requested_lim, err)
			http.Error(w, fmt.Sprintf("Bad limit: %s", requested_lim), 400)
			return
		}
		limit = user_lim
	}

	// Parse the count of numbers
	var count int64 = 1
	if query.Has("count") || query.Has("draws") {
		var requested_count = query.Get("count")
		if len(requested_count) == 0 {
			requested_count = query.Get("draws")
		}

		user_count, err := strconv.ParseInt(requested_count, 10, 64)
		if err != nil {
			log.Printf("Error parsing count [%s] in query: %s", requested_count, err)
			http.Error(w, fmt.Sprintf("Bad count: %s", requested_count), 400)
			return
		}
		if user_count < 0 {
			http.Error(w, "Cannot draw fewer than 0 numbers", 400)
		}
		count = user_count
	}

	// Parse whether to redraw or not
	var redraw bool = true
	if query.Has("redraw") {
		var requested_redraw = query.Get("redraw")
		if strings.ToLower(requested_redraw) == "false" {
			redraw = false
		} else if strings.ToLower(requested_redraw) == "true" {
			redraw = true
		} else {
			log.Printf("Error parsing redraw [%s] in query", requested_redraw)
			http.Error(w, fmt.Sprintf("Bad redraw: %s", requested_redraw), 400)
			return
		}
	}

	// Compute the range, checking for ranges that are too big or small
	var range_size = limit - floor
	if range_size <= 0 {
		http.Error(w, fmt.Sprintf("Invalid range (overflow or range ≤ 0): %d (floor: %d, limit %d)",
			range_size, floor, limit), 400)
		return
	} else if !redraw && range_size < count {
		http.Error(w, fmt.Sprintf("Cannot draw %d numbers from range of size %d without redraws",
			count, range_size), 400)
		return
	}

	// Pick a pseudorandom backend file - we don't care that maphash.Hash uses a suspect PRNG
	backend, err := server.PickRng()
	if err != nil {
		log.Printf("Internal RNG server error: %s", err)
		http.Error(w, fmt.Sprintf("Internal error: %s", err), 500)
		return
	}

	rng_src := rand.New(&server.rngs[backend])

	// If we are doing randomness without redraw, use a hash table to implement Fisher-Yates
	var draw_map = make(map[int64]int64)

	// Prepare an array to collect the results
	var numbers []int64

	for i := int64(0); i < count; i++ {
		// Source the roll from the good RNG
		roll := floor + rng_src.Int63n(range_size)

		// When we don't allow redraws, run Fisher-Yates
		if !redraw {
			// Check if we have already rolled the number and whether we need to replace it
			replacement_roll, drawn := draw_map[roll]
			// Set up the replacement for the current number
			replacement, replacement_found := draw_map[floor+range_size-1]
			if !replacement_found {
				draw_map[roll] = floor + range_size - 1
			} else {
				draw_map[roll] = replacement
			}
			// Replace if needed and then shrink the range
			if drawn {
				roll = replacement_roll
			}
			range_size -= 1
		}

		// Collect the number into the array
		numbers = append(numbers, roll)
	}

	// Return the collected numbers as a JSON response
	w.Header().Set("Content-Type", "application/json")
	jsonResponse, err := json.Marshal(numbers)
	if err != nil {
		log.Printf("Error marshalling numbers to JSON: %s", err)
		http.Error(w, "Internal error", 500)
		return
	}

	// Write the JSON response
	w.Write(jsonResponse)
}

// Handle a TCP stream
const tcpBufferSize = 1 << 21

func (server *RandomServer) HandleStream(c net.Conn) {
	log.Printf("Client [%s] opened TCP connection", c.RemoteAddr().String())
	defer c.Close()

	// Send data as requested
	for {
		var raw_bytes [4]byte

		// 15 minute deadline to avoid TCP connection closure
		c.SetReadDeadline(time.Now().Add(15 * time.Minute))
		if bytes_read, err := c.Read(raw_bytes[:]); err != nil {
			if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
				log.Printf("Client [%s] timed out", c.RemoteAddr().String())
				return
			}
			if err == io.EOF {
				log.Printf("Client [%s] closed the connection on their end", c.RemoteAddr().String())
				return
			}

			log.Printf("Unexpected error when reading from client [%s]: %s", c.RemoteAddr().String(), err)
			return
		} else if bytes_read != len(raw_bytes) {
			log.Printf("Unexpected number of bytes from client [%s]: should be %d", c.RemoteAddr().String(), len(raw_bytes))
			return
		}
		var bytes_requested = int(binary.LittleEndian.Uint32(raw_bytes[:]))

		// Allow a user-requested channel closure on 0 bytes requested
		if bytes_requested == 0 {
			log.Printf("Client [%s] requested connection closure", c.RemoteAddr().String())
			return
		}

		backend, err := server.PickRng()
		if err != nil {
			log.Printf("Internal RNG server error on TCP streaming: %s", err)
			return
		}

		// Send data, rounding up the data rate we send, but use chunks of size tcpBufferSize if a large number is requested
		if bytes_requested > tcpBufferSize {
			buffer := make([]byte, tcpBufferSize)
			for bytes_requested > 0 {
				var byte_demand = bytes_requested
				if byte_demand > len(buffer) {
					byte_demand = len(buffer)
				}

				if err := server.rngs[backend].ReadBytes(buffer[:byte_demand]); err != nil {
					log.Printf("Internal RNG server error: %s", err)
					return
				}
				c.Write(buffer[:byte_demand])

				bytes_requested -= byte_demand
			}
		} else {
			buffer := make([]byte, bytes_requested)
			if err := server.rngs[backend].ReadBytes(buffer[:]); err != nil {
				log.Printf("Internal RNG server error: %s", err)
				return
			}
			c.Write(buffer)
		}
	}
}

// Pull 1 MB of output from each RNG and run it through our basic monitoring test suite
// 1 MB is small enough that some tests may fail spuriously, but we keep a log to make sure it's not too often
func (server *RandomServer) TestRngs() {
	test_buffer := make([]byte, 1<<24)

	offset := int(new(maphash.Hash).Sum64() % uint64(len(server.rng_correctness)))
	for i := 0; i < len(server.rng_correctness); i += 1 {
		backend := i*server.readers_per_rng + offset

		if err := server.rngs[backend].ReadBytes(test_buffer); err != nil {
			log.Printf("Error reading RNG file during testing: %s", err)

			// Treat a failure to read as a test failure
			server.rng_test_log[i].push(false)
			server.rng_correctness[i] = float64(server.rng_test_log[i].popcount()) / float64(server.rng_test_log[i].size)
			server.rng_last_failed[i] = time.Now()
			continue
		}

		// Run statistical tests
		var correct = freqTest(test_buffer)
		if correct {
			// Runs test needs frequency test to be valid
			correct = runsTest(test_buffer)
		}

		if !correct {
			log.Printf("RNG device %s failed a statistical correctness test", server.rng_names[i])
		}

		// Log the test and update our correctness score
		server.rng_test_log[i].push(correct)
		server.rng_correctness[i] = float64(server.rng_test_log[i].popcount()) / float64(server.rng_test_log[i].size)
		if !correct {
			server.rng_last_failed[i] = time.Now()
		}
	}
}

// Report RNG status by JSON
type StatusReport struct {
	Names       []string
	Correctness []float64
	LastFailed  []time.Time
}

func (server *RandomServer) ServeStatus(w http.ResponseWriter, r *http.Request) {
	report := StatusReport{
		Names:       server.rng_names,
		Correctness: server.rng_correctness,
		LastFailed:  server.rng_last_failed,
	}

	report_serialized, err := json.Marshal(report)
	if err != nil {
		log.Printf("Internal error getting status: %s", err)
		http.Error(w, fmt.Sprintf("Internal error getting status"), 500)
		return
	}

	w.Write(report_serialized)
	w.Header().Set("Content-Type", "application/json")
}
