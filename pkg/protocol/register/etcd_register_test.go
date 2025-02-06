package register

import (
	"context"
	"fmt"
	"testing"
	"zingthings/pkg/util/loggerfactory"
)

func TestEtcdRegister_Register(t *testing.T) {
	etcdReg := NewEtcdRegister(context.Background(), loggerfactory.GetLogger(), 8687)
	etcdReg.Register()
}
func TestChannel(t *testing.T) {
	ints := make(chan int, 1)
	go func() {
		for {
			select {
			case <-ints:
				fmt.Println("goroutine exit")
				return
			}
		}
	}()
	go func() {
		ints <- 1
	}()
	<-ints
	fmt.Println("goroutine exit2")
}
