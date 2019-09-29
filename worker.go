package redworker

import (
	"log"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis"
)

// Registry is the interface the user can implement to give custom behaviour
type Registry interface {
	RegisterFunc(topic string, f func(string) error)
	GetFunc(topic string) func(string) error
	GetTopics() []string
}

// defaultRegistry is the default registry workers are initialized with in case
// custom behaviour is not needed
type defaultRegistry struct {
	funcs map[string]func(string) error
}

func newDefaultRegistry() *defaultRegistry {
	return &defaultRegistry{funcs: make(map[string]func(string) error)}
}

func (r *defaultRegistry) RegisterFunc(topic string, f func(string) error) {
	r.funcs[topic] = f
}

func (r *defaultRegistry) GetFunc(topic string) func(string) error {
	return r.funcs[topic]
}

func (r *defaultRegistry) GetTopics() []string {
	topics := []string{}
	for k := range r.funcs {
		topics = append(topics, k)
	}
	return topics
}

// RedWorker is the struct of the worker pool
type RedWorker struct {
	redisClient  *redis.Client
	numWorker    int
	done         chan struct{}
	registry     Registry
	topics       []string
	timeout      time.Duration
	shutDownChan chan struct{}
}

// NewWorker returns a RedWorker
func NewWorker(n int, c *redis.Client) *RedWorker {
	return &RedWorker{
		redisClient:  c,
		numWorker:    n,
		registry:     newDefaultRegistry(),
		done:         make(chan struct{}),
		timeout:      10 * time.Second,
		shutDownChan: make(chan struct{}),
	}
}

// WithTopics is the setter of redis topics to listen to
func (w *RedWorker) WithTopics(topics []string) *RedWorker {
	w.topics = topics
	return w
}

// WithRegistry is the setter of registry
func (w *RedWorker) WithRegistry(r Registry) *RedWorker {
	w.registry = r
	return w
}

// WithTimeout is the setter of the timeout to block for getting value
// from the redis topic
func (w *RedWorker) WithTimeout(d time.Duration) *RedWorker {
	w.timeout = d
	return w
}

// GetTopics is used to get the topics to listen to when the workers start
func (w *RedWorker) GetTopics() []string {
	if w.topics != nil {
		return w.topics
	}
	return w.registry.GetTopics()
}

// RegisterFunc is used for registering the function to run for each topic
func (w *RedWorker) RegisterFunc(topic string, f func(string) error) {
	w.registry.RegisterFunc(topic, f)
}

// GetFunc is used to get the func for corresponding topic
func (w *RedWorker) GetFunc(topic string) func(string) error {
	return w.registry.GetFunc(topic)
}

// Work starts the worker goroutines to process the messages
func (w *RedWorker) Work() {
	if w.topics == nil {
		w.topics = w.registry.GetTopics()
	}
	wg := &sync.WaitGroup{}
	wg.Add(w.numWorker)
	for i := 0; i < w.numWorker; i++ {
		go func(id int) {
			defer wg.Done()
			log.Printf("Starting worker %d", id)
			c := make(chan *redis.StringSliceCmd)
			for {
				go func() {
					c <- w.redisClient.BRPop(w.timeout, w.topics...)
				}()
				select {
				case <-w.done:
					return
				case cmd := <-c:
					if err := cmd.Err(); err != nil {
						if !strings.Contains(err.Error(), "nil") {
							log.Printf("Worker ID: %d :: Error reading from the redis: %s", id, err)
						}
						continue
					}
					vals := cmd.Val()
					if vals == nil || len(vals) == 0 {
						log.Printf("Worker ID: %d :: Didn't get any value from redis", id)
						continue
					}
					topic, msg := vals[0], vals[1]
					f := w.GetFunc(topic)
					if f == nil {
						log.Printf("Worker ID: %d :: Did not find the function to execute the topic: %s", id, topic)
						continue
					}
					err := f(msg)
					if err != nil {
						log.Printf("Worker ID: %d :: Error executing the function: %s", id, err)
						continue
					}
				}
			}
		}(i)
	}
	wg.Wait()
	close(w.shutDownChan)
}

// Shutdown shuts down the worker and returns once all the workers are done processing
func (w *RedWorker) Shutdown() {
	close(w.done)
	<-w.shutDownChan
}
