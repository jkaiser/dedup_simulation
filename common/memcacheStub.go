package common

import "bytes"
import "strings"
import "strconv"

import log "github.com/cihub/seelog"
import "github.com/bradfitz/gomemcache/memcache"

// creates a memcache Client and checks if the connections are functional
func MemClientInit(server string) (*memcache.Client, bool) {
	var servers []string = strings.Split(server, ",")
	var serveradresses []string

	for s := range servers {
		serveradresses = append(serveradresses, string(servers[s])+":11211")
		log.Debug("Adding: " + serveradresses[s])
	}

	mc := memcache.New(serveradresses...)

	//try to set values. If it fails return false
	for i := range servers {
		value := make([]byte, 128)
		foo := strconv.Itoa(i)
		copy(value[:], foo)
		err := mc.Set(&memcache.Item{Key: foo, Value: value})
		if err != nil {
			log.Error("Error during Memcacheclient set test: ", err)
			return nil, false
		}
	}

	//try to get the values and check if they are correct. If either fails return false
	for i := range servers {
		value := make([]byte, 128)
		foo := strconv.Itoa(i)
		it, err := mc.Get(foo)

		if err != nil {
			log.Error("Error during Memcacheclient get test: ", err)
			return nil, false
		}

		bar := it.Value
		copy(value[:], foo)
		if bytes.Compare(value, bar) != 0 {
			log.Error("Error during Memcacheclient value test.")
			return nil, false
		}
		delerr := mc.Delete(foo)
		if delerr != nil {
			log.Error("Error during Memcache delete: ", delerr)
			return nil, false
		}
	}
	return mc, true

}
