package core

import "context"

func (c *NodeContext) GetPort(defaultPort int) int {
	if v, ok := c.Properties["port"].(int); ok {
		return v
	}
	return defaultPort
}
func (c *NodeContext) GetContext() context.Context {
	if v, ok := c.Properties["Context"].(context.Context); ok {
		return v
	}
	return nil
}

func (c *NodeContext) SetContext(context context.Context) {
	if nil == c.Properties {
		c.Properties = make(map[string]interface{})
	}
	c.Properties["Context"] = context
}
