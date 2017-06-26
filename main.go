package main

import (
	"github.com/go-redis/redis"
	"fmt"
	"sort"
	"strings"
	"os"
	"os/user"
	"io/ioutil"
	"log"
	"crypto/md5"
	"io"
)

const (
	PAGE_SIZE = 8192
)

func ExampleNewClient() (*redis.Client){
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	pong, err := client.Ping().Result()
	fmt.Println(pong, err)

	return client
}

var (
	homeDir string;
)

func main() {
	//DumpAllRelationsFromRedis();

	diff();
}

func diff() {
	redisRelationsDir := "/Users/yyao/tmp"
	//redisRelationsDir = "/Users/yyao/workspace/oss/postgresql-bakup/smgr2/base/1"
	//redisRelationsDir := "/tmp/aaa/base/1"
	pgRelationDir := "/Users/yyao/workspace/oss/postgresql-bakup/smgr/base/1"

	pgFiles := getFilesUnderDir(pgRelationDir)
	redisFiles := getFilesUnderDir(redisRelationsDir)

	inPGOnly, inRedisOnly, common := difference(pgFiles, redisFiles)

	log.Printf("In PostgreSQL Only: %v\n", inPGOnly)
	log.Printf("In Redis      Only: %v\n", inRedisOnly)

	for _, fileName := range common {
		sum1 := checksumFile(redisRelationsDir + "/" + fileName)
		sum2 := checksumFile(pgRelationDir + "/" + fileName)
		if sum1 != sum2 {
			log.Printf("File %v has different checksum\n", fileName)
		}
	}
}

func getFilesUnderDir(dir string) ([]string){
	var files []string
	fileInfos, err := ioutil.ReadDir(dir)
	if err != nil {
		panic(err)
	}

	for _, file := range fileInfos {
		if file.IsDir() {
			if file.Name() != "." || file.Name() != ".." {
				fmt.Printf("unexpected directory: %v under %v\n", file.Name(), dir)
			}
			continue
		}
		files = append(files, file.Name())
	}

	return files
}

// Return checksum for a file.
func checksumFile(file string) string {
	f, err := os.Open(file)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		log.Fatal(err)
	}

	return fmt.Sprintf("%x", h.Sum(nil))
}

// Compare two slices, and return differences and commons.
func difference(slice1 []string, slice2 []string) (
		inSlice1 []string, inSlice2 []string, common []string){
	m1 := map [string]int{}
	m2 := map [string]int{}

	for _, s1Val := range slice1 {
		m1[s1Val] = 1
	}
	for _, s2Val := range slice2 {
		m2[s2Val] = 1
	}

	for _, v := range slice1 {
		if _, ok := m2[v]; ok {
			common = append(common, v)
		} else {
			inSlice1 = append(inSlice1, v)
		}
	}

	for _, v := range slice2 {
		if _, ok := m1[v]; ! ok {
			inSlice2 = append(inSlice2, v)
		}
	}

	return inSlice1, inSlice2, common
}

func DumpAllRelationsFromRedis() {
	homeDir = getHomeDir()

	client := ExampleNewClient()
	defer client.Close()

	keys, err := client.Keys("base/1/*").Result()
	if err != nil {
		panic(err)
	}

	sort.Strings(keys)

	for _, v := range keys {
		dump(client, v)
	}
}

// dump relation stored in redis server.
// will combine files to single file if it is blocks belong to same file.
// Key format in redis:
//		base/<dbid>/<relid>[forknum]#<blockno>
func dump(client *redis.Client, key string) {
	s := client.Get(key)
	if s.Err() != nil {
		panic(s.Err())
	}

	b, err := s.Bytes()
	if err != nil {
		panic(err)
	}

	if len(b) != PAGE_SIZE {
		panic(fmt.Errorf("length is not expected: %v", key))
	}

	fields := strings.Split(key, "#")
	if len(fields) != 2 {
		panic(fmt.Errorf("key format is incorrect: %v", key))
	}

	// create a new file
	relPath := strings.Split(fields[0], "/")
	fileName := relPath[len(relPath) - 1]
	filePath := fmt.Sprintf("%v/tmp/%v", homeDir, fileName)

	var f *os.File

	if (fields[1]) == "0" {
		f, err = os.Create(filePath)
		if err != nil {
			panic(err)
		}
	} else {
		if f, err = os.OpenFile(filePath, os.O_APPEND|os.O_RDWR, 0666); err != nil {
			panic(err)
		}
	}

	defer f.Close()

	f.Write(b)
}

func getHomeDir() (string) {
	usr, err := user.Current()
	if err != nil {
		panic(err)
	}
	return usr.HomeDir
}