package rabbitmq_go

import (
	"fmt"
	"log"
)

type Logger interface {
	Errorf(string, ...interface{})
	Warnf(string, ...interface{})
	Infof(string, ...interface{})
}

type StdLog struct {
}

func (p StdLog) Errorf(s string, i ...interface{}) {
	log.Printf(fmt.Sprintf("[ERROR] %v", fmt.Sprintf(s, i...)))
}

func (p StdLog) Warnf(s string, i ...interface{}) {
	log.Printf(fmt.Sprintf("[WARN] %v", fmt.Sprintf(s, i...)))
}

func (p StdLog) Infof(s string, i ...interface{}) {
	log.Printf(fmt.Sprintf("[INFO] %v", fmt.Sprintf(s, i...)))
}
