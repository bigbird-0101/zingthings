package core

import "testing"

type (
	MyChannelUpStreamHandler struct {
		name   string
		called bool
	}
	MyChannel2UpStreamHandler struct {
		name   string
		called bool
	}
	MyChannelDownStreamHandler struct {
		name   string
		called bool
	}
)

func (down *MyChannelDownStreamHandler) OnChannelDownStream(context ChannelHandlerContext, data interface{}) {
	down.called = true
}

func (down *MyChannelUpStreamHandler) OnChannelUpStream(context ChannelHandlerContext, data interface{}) {
	context.SendUpStream(data)
	down.called = true
}

func (down *MyChannel2UpStreamHandler) OnChannelUpStream(context ChannelHandlerContext, data interface{}) {
	down.called = true
}

func TestNewDefaultChannelHandlerPipeline(t *testing.T) {
	test := NewDefaultChannelHandlerPipeline()
	if test == nil {
		t.Error("NewDefaultChannelHandlerPipeline failed")
	}
}

func TestChannelHandlerPipeline(t *testing.T) {
	test := NewDefaultChannelHandlerPipeline()
	ab := &MyChannelDownStreamHandler{
		name: "test",
	}
	ab2 := &MyChannelUpStreamHandler{
		name: "test2",
	}
	ab3 := &MyChannel2UpStreamHandler{
		name: "test2",
	}
	err := test.AddFirst("test", ab)
	err2 := test.AddLast("test2", ab2)
	err3 := test.AddLast("test3", ab3)
	if err != nil {
		t.Errorf("AddFirst test failed, err %s", err)
	}
	if err2 != nil {
		t.Errorf("AddLast test failed, err %s", err2)
	}
	if err3 != nil {
		t.Errorf("AddLast test failed, err %s", err3)
	}
	first := test.GetFirst()
	if (*first.(*ChannelHandler)).(*MyChannelDownStreamHandler) != ab {
		t.Errorf("handler not match")
	}
	last := test.GetLast()
	if (*last.(*ChannelHandler)).(*MyChannel2UpStreamHandler) != ab3 {
		t.Errorf("handler not match")
	}
	data := make(map[string]interface{})
	data["test"] = "test"
	test.SendUpStream(&data)
	test.SendDownStream(&data)
	if !ab3.called {
		t.Errorf("handler not called ")
	}
	if !ab2.called {
		t.Errorf("handler not called ")
	}
	if !ab.called {
		t.Errorf("handler not called ")
	}
}

func TestPipelineRemoveAndReplace(t *testing.T) {
	test := NewDefaultChannelHandlerPipeline()
	ab := &MyChannelDownStreamHandler{
		name: "test",
	}
	ab2 := &MyChannelUpStreamHandler{
		name: "test2",
	}
	err := test.AddFirst("test", ab)
	err2 := test.AddLast("test2", ab2)
	if err != nil {
		t.Errorf("AddFirst test failed, err %s", err)
	}
	if err2 != nil {
		t.Errorf("AddLast test failed, err %s", err2)
	}
	first := test.RemoveFirst()
	if first != ab {
		t.Errorf("handler not match")
	}
	first2 := test.RemoveFirst()
	if first2 != ab2 {
		t.Errorf("handler not match")
	}
	err3 := test.AddFirst("test", ab)
	err4 := test.AddFirst("test2", ab2)
	if err3 != nil {
		t.Errorf("AddFirst test failed, err %s", err3)
	}
	if err4 != nil {
		t.Errorf("AddLast test failed, err %s", err4)
	}
	last := test.RemoveLast()
	if last != ab {
		t.Errorf("handler not match")
	}
	last2 := test.RemoveLast()
	if last2 != ab2 {
		t.Errorf("handler not match")
	}
	err7 := test.AddFirst("test", ab)
	err8 := test.AddFirst("test2", ab2)
	if err7 != nil {
		t.Errorf("AddFirst test failed, err %s", err7)
	}
	if err8 != nil {
		t.Errorf("AddLast test failed, err %s", err8)
	}
	streamHandler := &MyChannel2UpStreamHandler{
		name: "test",
	}
	err9 := test.AddFirst("test3", streamHandler)
	if err9 != nil {
		t.Errorf("AddFirst test failed, err %s", err9)
	}
	replace, err := test.Replace("test2", "test4", streamHandler)
	if err != nil {
		t.Errorf("Replace test failed, err %s", err)
	}
	if replace != ab2 {
		t.Errorf("handler not match")
	}
}
