package main

import (
	"github.com/catlittlechen/consistent"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"
)

func handleError(err error) {
	if err != nil {
		log.Fatal(err)
		os.Exit(-1)
	}
}

func main() {
	rand.Seed(time.Now().Unix())
	c := consistent.DefaultNew()

	serverID := []string{"server_id_1", "server_id_2", "server_id_3", "server_id_4"}
	for index, serverid := range serverID {
		handleError(c.Add(serverid, index+1))
	}

	answer := make(map[string]int)
	for i := 0; i < 10000000; i++ {
		key := "key_" + strconv.Itoa(rand.Int())
		answer[c.Get(key)] += 1
	}

	for _, serverid := range serverID {
		log.Printf("%s -- %d\n", serverid, answer[serverid])
		handleError(c.Del(serverid))
	}

}
