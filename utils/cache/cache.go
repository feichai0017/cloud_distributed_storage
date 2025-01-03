package cache

import "sync"

type Cache struct {
	m	sync.RWMutex
	
}