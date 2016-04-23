package consistent

import (
	"errors"
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

var (
	ErrServerIDExist    = errors.New("server id exists")
	ErrServerIDNotExist = errors.New("server id doesn't exist")
	ErrWrongWeight      = errors.New("weight must be larger than 0")
)

var globalVirtualNumber = 10

type circle struct {
	circle []uint32
	count  int
	cap    int
}

func newCircle() circle {
	c := circle{
		circle: make([]uint32, globalVirtualNumber),
		count:  0,
		cap:    globalVirtualNumber,
	}

	return c
}

func (c circle) Len() int { return c.count }

func (c circle) Less(i, j int) bool { return c.circle[i] < c.circle[j] }

func (c circle) Swap(i, j int) { c.circle[i], c.circle[j] = c.circle[j], c.circle[i] }

func (c *circle) clear() {
	c.count = 0
	return
}

func (c *circle) addMember(key uint32) {
	if c.count >= c.cap {
		keys := make([]uint32, globalVirtualNumber)
		c.circle = append(c.circle, keys...)
		c.cap += globalVirtualNumber
	}

	c.circle[c.count] = key
	c.count++
	return
}

func (c *circle) search(key uint32) uint32 {

	f := func(i int) bool {
		return c.circle[i] > key
	}

	i := sort.Search(c.count, f)
	if i >= c.count {
		i = 0
	}

	return c.circle[i]
}

type Consistent struct {
	sync.RWMutex

	mapMembers map[string]int
	mapHashKey map[uint32]string
	circle     circle
}

func DefaultNew() (c *Consistent) {
	c = new(Consistent)
	c.mapMembers = make(map[string]int)
	c.mapHashKey = make(map[uint32]string)
	c.circle = newCircle()
	return
}

func New(virtualNumber int) (c *Consistent) {
	c = new(Consistent)
	globalVirtualNumber = virtualNumber
	c.mapMembers = make(map[string]int)
	c.mapHashKey = make(map[uint32]string)
	c.circle = newCircle()
	return
}

func (c *Consistent) hashKey(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

func (c *Consistent) generateKey(serverID string, id int) string {
	return serverID + strconv.Itoa(id)
}

func (c *Consistent) Add(serverID string, weight int) (err error) {
	if weight < 1 {
		return ErrWrongWeight
	}
	c.Lock()
	defer c.Unlock()

	if _, ok := c.mapMembers[serverID]; ok {
		return ErrServerIDExist
	}
	c.mapMembers[serverID] = weight

	var key uint32
	virtualNumber := globalVirtualNumber * weight
	for i := 0; i < virtualNumber; i++ {
		key = c.hashKey(c.generateKey(serverID, i))
		c.mapHashKey[key] = serverID
		c.circle.addMember(key)
	}

	sort.Sort(c.circle)
	return
}

func (c *Consistent) Get(key string) string {
	c.RLock()
	defer c.RUnlock()

	return c.mapHashKey[c.circle.search(c.hashKey(key))]
}

func (c *Consistent) Del(serverID string) (err error) {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.mapMembers[serverID]; !ok {
		return ErrServerIDNotExist
	}
	delete(c.mapMembers, serverID)

	virtualNumber := globalVirtualNumber * c.mapMembers[serverID]
	for i := 0; i < virtualNumber; i++ {
		delete(c.mapHashKey, c.hashKey(c.generateKey(serverID, i)))
	}

	c.circle.clear()
	for key := range c.mapHashKey {
		c.circle.addMember(key)
	}
	sort.Sort(c.circle)

	return
}
