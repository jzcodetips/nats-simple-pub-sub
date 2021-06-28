package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/pkg/errors"

	nats "github.com/nats-io/nats.go"
)

const user = "jzsaiyan"

func main() {
	var err error

	if err = subSync(); err != nil {
		log.Fatal(errors.Wrap(err, "subSync"))
	}

	if err = subASync(); err != nil {
		log.Fatal(errors.Wrap(err, "subASync"))
	}
}

func getNatsConn(natsName string) (*nats.Conn, error) {
	natsOptions := []nats.Option{
		nats.Name(natsName),
		nats.Timeout(10 * time.Second),
		nats.PingInterval(20 * time.Second),
		nats.MaxPingsOutstanding(5),
		nats.MaxReconnects(10),
		nats.ReconnectWait(10 * time.Second),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			log.Printf("Err handler event err=%v", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Println("Reconnect handler event")
			// handle reconnect event
		}),
		nats.ReconnectBufSize(5 * 1024 * 1024),
		nats.UserInfo(user, "password"),
	}

	nc, err := nats.Connect(nats.DefaultURL, natsOptions...)
	if err != nil {
		return nil, err
	}

	return nc, nil
}

func subSync() error {
	log.Println("subSync ...")

	subj := "demo.time"
	natsName := fmt.Sprintf("%s client", user)

	nc, err := getNatsConn(natsName)
	if err != nil {
		return errors.Wrap(err, "getNatsConnc")
	}

	defer nc.Close()

	// Sync Subscription
	sub, err := nc.SubscribeSync(subj)
	if err != nil {
		return errors.Wrap(err, "nc.SubscribeSync err=%v")
	}

	defer func() {
		if err := sub.Unsubscribe(); err != nil {
			log.Printf("sub.Unsubscribe err=%v", err)
		}
	}()

	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()

		if err := nc.Publish(subj, []byte("Hello world sync")); err != nil {
			log.Printf("Publish err=%v", err)
		}
	}()

	// Wait for a message
	msg, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		return errors.Wrap(err, "sub.NextMsg")
	}

	log.Printf("Reply: %s", msg.Data)

	wg.Wait()

	return nil
}

func subASync() error {
	log.Println("subASync ...")

	subj := "demo.time"
	natsName := fmt.Sprintf("%s client", user)

	nc, err := getNatsConn(natsName)
	if err != nil {
		return errors.Wrap(err, "getNatsConnc")
	}

	defer nc.Close()

	// Use a WaitGroup to wait for a message to arrive
	var wg sync.WaitGroup

	wg.Add(1)

	// Subscribe
	sub, err := nc.Subscribe(subj, func(m *nats.Msg) {
		log.Printf("Reply: %s", m.Data)

		wg.Done()
	})

	if err != nil {
		return errors.Wrap(err, "nc.Subscribe")
	}

	defer func() {
		if err := sub.Unsubscribe(); err != nil {
			log.Printf("sub.Unsubscribe err=%v", err)
		}
	}()

	if err = nc.Publish(subj, []byte("Hello world async")); err != nil {
		return errors.Wrap(err, "nc.Publish")
	}

	// Wait for a message to come in
	wg.Wait()

	return nil
}
