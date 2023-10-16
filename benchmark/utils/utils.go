package utils

import (
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	dbm "github.com/tendermint/tm-db"

	"github.com/cosmos/iavl"
)

const (
	DefaultCacheSize int = 10000
)

type KeyValuePair struct {
	Key   []byte `json:"key"`
	Value []byte `json:"value"`
}

// Opens application db
func OpenDB(dir string) (dbm.DB, error) {
	switch {
	case strings.HasSuffix(dir, ".db"):
		dir = dir[:len(dir)-3]
	case strings.HasSuffix(dir, ".db/"):
		dir = dir[:len(dir)-4]
	default:
		return nil, fmt.Errorf("database directory must end with .db")
	}
	// TODO: doesn't work on windows!
	cut := strings.LastIndex(dir, "/")
	if cut == -1 {
		return nil, fmt.Errorf("cannot cut paths on %s", dir)
	}
	name := dir[cut+1:]
	db, err := dbm.NewGoLevelDB(name, dir[:cut])
	if err != nil {
		return nil, err
	}
	return db, nil
}

// ReadTree loads an iavl tree from the directory
// If version is 0, load latest, otherwise, load named version
// The prefix represents which iavl tree you want to read. The iaviwer will always set a prefix.
func ReadTree(db dbm.DB, version int, prefix []byte) (*iavl.MutableTree, error) {
	if len(prefix) != 0 {
		db = dbm.NewPrefixDB(db, prefix)
	}

	tree, err := iavl.NewMutableTree(db, DefaultCacheSize, true)
	if err != nil {
		return nil, err
	}
	_, err = tree.LoadVersion(int64(version))
	return tree, err
}

// Writes raw key / values from a tree to a file
// Writes a chunkSize number of keys/values to separate files per module
func WriteTreeDataToFile(tree *iavl.MutableTree, filenamePattern string, chunkSize int) {
	var currentChunk, currentCount int
	var currentFile *os.File

	createNewFile := func() {
		if currentFile != nil {
			currentFile.Close()
		}

		filename := fmt.Sprintf("%s_chunk_%d.kv", filenamePattern, currentChunk)
		var err error
		currentFile, err = os.Create(filename)
		if err != nil {
			panic(err)
		}

		currentChunk++
	}

	// Open first chunk file
	createNewFile()

	tree.Iterate(func(key []byte, value []byte) bool {
		// If we've reached chunkSize, close current file and open a new one
		if currentCount >= chunkSize {
			createNewFile()
			currentCount = 0
		}

		if err := writeByteSlice(currentFile, key); err != nil {
			currentFile.Close()
			panic(err)
		}
		if err := writeByteSlice(currentFile, value); err != nil {
			currentFile.Close()
			panic(err)
		}

		currentCount++
		return false
	})

	if currentFile != nil {
		currentFile.Close()
	}
}

// Writes raw bytes to file
func writeByteSlice(w io.Writer, data []byte) error {
	length := uint32(len(data))
	if err := binary.Write(w, binary.LittleEndian, length); err != nil {
		return err
	}
	_, err := w.Write(data)
	return err
}

// Reads raw keys / values from a file
// TODO: Adding in ability to chunk larger exported file (like for wasm dir)
func ReadKVEntriesFromFile(filename string) ([]KeyValuePair, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var kvPairs []KeyValuePair
	for {
		key, err := readByteSlice(f)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		value, err := readByteSlice(f)
		if err != nil {
			return nil, err
		}

		kvPairs = append(kvPairs, KeyValuePair{Key: key, Value: value})
	}

	return kvPairs, nil
}

func readByteSlice(r io.Reader) ([]byte, error) {
	var length uint32
	if err := binary.Read(r, binary.LittleEndian, &length); err != nil {
		return nil, err
	}

	data := make([]byte, length)
	_, err := io.ReadFull(r, data)
	return data, err
}

// Randomly Shuffle kv pairs once read
func RandomShuffle(kvPairs []KeyValuePair) {
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(kvPairs), func(i, j int) {
		kvPairs[i], kvPairs[j] = kvPairs[j], kvPairs[i]
	})
}

// Add Random Bytes to keys / values
func AddRandomBytes(data []byte, numBytes int) []byte {
	randomBytes := make([]byte, numBytes)
	if _, err := rand.Read(randomBytes); err != nil {
		panic(fmt.Sprintf("Failed to generate random bytes: %v", err))
	}
	return append(data, randomBytes...)
}

// NOTE: Assumes latencies is sorted
func CalculatePercentile(latencies []time.Duration, percentile float64) time.Duration {
	if percentile < 0 || percentile > 100 {
		panic(fmt.Sprintf("Invalid percentile: %f", percentile))
	}
	index := int(float64(len(latencies)-1) * percentile / 100.0)
	return latencies[index]
}

// Picks random item from a list and updates sync map with it
func PickRandomItem(items []string, processedItems *sync.Map) string {
	var availableItems []string

	for _, item := range items {
		if _, processed := processedItems.Load(item); !processed {
			availableItems = append(availableItems, item)
		}
	}

	if len(availableItems) == 0 {
		return ""
	}

	selected := availableItems[rand.Intn(len(availableItems))]
	processedItems.Store(selected, true)
	return selected
}

// CreateVersions creates symlink versions of the base directory.
func CreateVersions(baseOutputDir, outputDir string, numOutputVersions int) error {
	// Ensure the base directory exists
	if _, err := os.Stat(baseOutputDir); os.IsNotExist(err) {
		return fmt.Errorf("base output directory does not exist: %s", baseOutputDir)
	}

	if _, err := os.Stat(outputDir); os.IsNotExist(err) {
		if err := os.MkdirAll(outputDir, os.ModePerm); err != nil {
			return fmt.Errorf("cannot create output directory: %s", err)
		}
	}

	// Create symlinks for the specified number of versions
	for i := 1; i < numOutputVersions; i++ {
		destDir := filepath.Join(outputDir, fmt.Sprintf("version_%d", i))
		if err := os.Symlink(baseOutputDir, destDir); err != nil {
			return fmt.Errorf("error creating symlink: %s", err)
		}
	}
	return nil
}

func ListAllFiles(dir string) ([]string, error) {
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return []string{}, err
	}
	// Extract file nams from input KV dir
	var fileNames []string
	for _, file := range files {
		fileNames = append(fileNames, file.Name())
	}

	return fileNames, nil
}

func LoadAndShuffleKV(inputDir string) ([]KeyValuePair, error) {
	var allKVs []KeyValuePair
	mu := &sync.Mutex{}
	wg := &sync.WaitGroup{}

	allFiles, err := ListAllFiles(inputDir)
	if err != nil {
		log.Fatalf("Failed to list all files: %v", err)
	}

	for _, file := range allFiles {
		wg.Add(1)
		go func(id string, selectedFile string) {
			defer wg.Done()

			kvEntries, err := ReadKVEntriesFromFile(filepath.Join(id, selectedFile))
			if err != nil {
				panic(err)
			}

			// Safely append the kvEntries to allKVs
			mu.Lock()
			allKVs = append(allKVs, kvEntries...)
			fmt.Printf("Done processing file %+v\n", filepath.Join(id, selectedFile))
			mu.Unlock()
		}(inputDir, file)
	}
	wg.Wait()

	rand.Shuffle(len(allKVs), func(i, j int) {
		allKVs[i], allKVs[j] = allKVs[j], allKVs[i]
	})

	return allKVs, nil
}

func GenerateVersionNames(numVersions int) []string {
	if numVersions <= 0 {
		panic("Number of versions must be larger than 0")
	}

	names := []string{}
	for i := 0; i < numVersions; i++ {
		names = append(names, fmt.Sprintf("version_%d", i))
	}

	return names
}
