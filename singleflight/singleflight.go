package singleflight

import "sync"



// call tracks one in-flight execution for a key.
type call struct {
	wg  sync.WaitGroup //fn
	val interface{}    //fn
	err error          //fn
}

// Group manages all kinds of  calls
// Group deduplicates concurrent work by key.
type Group struct {
	mu sync.Map //sync.Map msync.Map,key string(key),*call:key->*call
}

// DokeyDo(),fn
// Do runs fn once for a key and shares its result with waiters.
func (g *Group) Do(key string, fn func() (interface{}, error)) (interface{}, error) {
	//call
	if v, ok := g.mu.Load(key); ok {
		c := v.(*call)
		c.wg.Wait()
		return c.val, c.err
	}

	//callcall
	c := &call{}
	c.wg.Add(1)
	g.mu.Store(key, c) //map

	//fn,
	c.val, c.err = fn()
	c.wg.Done()

	//callmap()
	g.mu.Delete(key)

	return c.val, c.err
}
