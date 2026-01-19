package singleflight

import "sync"

//代表正在进行或已完成的请求

type call struct {
	wg  sync.WaitGroup //用来阻塞其他等待者，直到这个fn执行完毕
	val interface{}    //fn的返回值
	err error          //fn返回的错误
}

// Group manages all kinds of  calls
type Group struct {
	mu sync.Map //使用sync.Map来优化并发性能 m是sync.Map,key是 string(key),值是*call:key->*call
}

// Do针对相同的key，保证多次调用Do(),都只会执行一次fn
func (g *Group) Do(key string, fn func() (interface{}, error)) (interface{}, error) {
	//先看有没有正在进行的call
	if v, ok := g.mu.Load(key); ok {
		c := v.(*call)
		c.wg.Wait()         //等正在进行的请求执行完毕
		return c.val, c.err //复用它的结果
	}

	//没有正在进行的call，则创建一个call
	c := &call{}
	c.wg.Add(1)
	g.mu.Store(key, c) //把这次正在执行中的请求注册到map中

	//真正执行用户传进来的fn,并记录结果
	c.val, c.err = fn()
	c.wg.Done()

	//结束后把call从map中删除(避免内存泄漏)
	g.mu.Delete(key)

	return c.val, c.err
}
