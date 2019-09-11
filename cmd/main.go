package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unsafe"

	"github.com/lib/pq"

	logger "utransfer/pkgs/logger"

	jsoniter "github.com/json-iterator/go"

	nats "github.com/nats-io/nats.go"
	stan "github.com/nats-io/stan.go"
)

type idOnly struct {
	ID int64 `json:"_id"`
}

var js = []byte(`[{"a":123,"b":234,"_id":1},{"a":123,"b":234,"_id":1},{"a":123,"b":234,"_id":2},{"a":123,"b":234,"_id":3}]`)
var json = jsoniter.ConfigCompatibleWithStandardLibrary

type application struct {
	log   *logger.Logger
	cfg   config
	ctx   context.Context
	toSrc chan struct{}
	toDst chan []byte
}

type dbSourse struct {
	app        application
	db         *sql.DB
	typeSourse string
}

type natsSourse struct {
	app        application
	natsConn   stan.Conn
	natsSub    stan.Subscription
	typeSourse string
	config     map[string]string
	readChan   chan *stan.Msg
}

type transfer interface {
	write([]byte) error
	read() ([]byte, error)
	ping() error
	connect() error
	closeConn()
}

// close NATS Method
func (n *natsSourse) closeConn() {
	if n.typeSourse == "src" {
		n.natsSub.Unsubscribe()
		n.natsSub.Close()
	}
	n.natsConn.Close()
}

// close NATS Method
func (d *dbSourse) closeConn() {
	d.db.Close()
}

// ping NATS Method
func (n *natsSourse) ping() error {
	if status := n.natsConn.NatsConn().Status(); status == nats.CONNECTED {
		return nil
	}
	return errors.New("NATS ping lost")
}

// ping NATS Method
func (d *dbSourse) ping() error {
	if err := d.db.Ping(); err != nil {
		return err
	}
	return nil
}

// read NATS Method
func (n *natsSourse) read() ([]byte, error) {
	for {
		select {
		case d := <-n.readChan:
			n.app.log.Debugf("read from NATS: %d bytes", len(d.Data))
			return d.Data, nil
		default:
			return nil, nil
		}
	}
}

// read DB Method
func (d *dbSourse) read() (data []byte, err error) {
	conn, err := d.db.Conn(context.Background())
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := conn.QueryRowContext(ctx, d.app.cfg.Queries.Select, d.app.cfg.Limit.Process).Scan(&data); err != nil {
		return nil, err
	}

	d.app.log.Debugf("read from db: %d bytes", len(data))
	return data, nil
}

// write DB Method
func (d *dbSourse) write(data []byte) error {

	if d.typeSourse == "src" {
		var (
			jsonIds []idOnly
			ids     []int64
		)
		if err := json.Unmarshal(data, &jsonIds); err != nil {
			d.app.log.Error(err.Error())
		}
		for i := range jsonIds {
			ids = append(ids, jsonIds[i].ID)
		}
		conn, err := d.db.Conn(context.Background())
		if err != nil {
			return err
		}
		defer conn.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := conn.ExecContext(ctx, d.app.cfg.Queries.Commit, pq.Array(ids)); err != nil {
			return err
		}
		d.app.log.Debugf("write result to db: %d elements", len(ids))
	}

	if d.typeSourse == "dst" {
		d.app.log.Debugf("write to db: %d bytes", len(data))
	}
	return nil
}

// write NATS Method
func (n *natsSourse) write(data []byte) error {
	if n.typeSourse == "src" {
		var (
			jsonIds []idOnly
		)
		if err := json.Unmarshal(data, &jsonIds); err != nil {
			n.app.log.Error(err.Error())
		}
		dataIds, err := json.Marshal(jsonIds)
		if err != nil {
			return err
		}
		err = n.natsConn.Publish(n.config["pubTopic"], dataIds)
		if err != nil {
			return err
		}

		n.app.log.Debugf("write result to nats: %v elements", jsonIds) //len(ids)
	}
	if n.typeSourse == "dst" {
		err := n.natsConn.Publish(n.config["pubTopic"], data)
		if err != nil {
			return err
		}
		n.app.log.Debugf("write to nats: %d bytes", len(data))
	}

	return nil
}

// connect DB Method
func (d *dbSourse) connect() error {
	var source string
	if d.typeSourse == "dst" {
		source = d.app.cfg.DST.Source
	}
	if d.typeSourse == "src" {
		source = d.app.cfg.SRC.Source
	}
	db, err := sql.Open("postgres", source)
	if err != nil {
		return err
	}
	if err := db.Ping(); err != nil {
		return err
	}
	db.SetMaxOpenConns(1)
	db.SetConnMaxLifetime(time.Second * 10)
	d.db = db
	d.app.log.Debug("connect db")
	return nil
}

// func (d *dbSourse) getConn() (conn *sql.Conn, err error) {
// 	// if dbm.keep {
// 	// 	if dbm.db == nil {
// 	// 		if dbm.db, err = dbm.createConn(); err != nil {
// 	// 			return nil, err
// 	// 		}
// 	// 	}
// 	// 	return dbm.db.Conn(context.Background())
// 	// }
// 	if err := d.connect(); err != nil {
// 		return nil, err
// 	}
// 	return d.db.Conn(context.Background())
// }

// connect NATS Method
func (n *natsSourse) connect() error {

	streamOpts := []stan.Option{
		stan.NatsURL(fmt.Sprintf("nats://%s:%s", n.config["host"], n.config["port"])),
		stan.Pings(10, 10),
		stan.SetConnectionLostHandler(func(c stan.Conn, err error) {
			n.app.log.Errorf("lost nats-streaming connection: %v", err)
		}),
	}
	uuid, err := newUUID()
	if err != nil {
		uuid = strconv.FormatInt(time.Now().UnixNano(), 10)
	}
	nsc, err := stan.Connect(n.config["clusterID"], uuid, streamOpts...)
	if err != nil {
		return err
	}
	n.natsConn = nsc

	if n.typeSourse == "dst" {
		//source = d.app.cfg.DST.Source
	}
	if n.typeSourse == "src" {
		n.readChan = make(chan *stan.Msg)
		sub, err := n.natsConn.QueueSubscribe(n.config["subTopic"], "123", func(m *stan.Msg) {
			n.readChan <- m
			m.Ack()
		}, stan.SetManualAckMode()) //,  , stan.DeliverAllAvailable() , stan.StartWithLastReceived(),
		if err != nil {
			return err
		}
		n.natsSub = sub
		//go n.subReader()
	}

	n.app.log.Debug("STAN connect")
	return nil

}

// func (n *natsSourse) subReader() {

// }

func main() {
	app := application{}
	if err := app.initConfig(); err != nil {
		os.Exit(1)
	}

	app.log = logger.Init(app.cfg.LogLevel, "")
	c, cancel := context.WithCancel(context.Background())
	app.ctx = c
	app.toDst = make(chan []byte, app.cfg.Limit.Process)
	app.toSrc = make(chan struct{})

	workerSrcDone := make(chan struct{})
	workerDstDone := make(chan struct{})

	srcType, cfg := checkSource(app.cfg.SRC.Source)
	if srcType == "db" {
		db := &dbSourse{app: app, typeSourse: "src"}
		go app.workerWithSRC(db, workerSrcDone)
	} else if srcType == "nats" {
		nt := &natsSourse{app: app, typeSourse: "src", config: cfg}
		go app.workerWithSRC(nt, workerSrcDone)
	} else {
		app.log.Fatal("Bad source")
	}

	dstType, cfg := checkSource(app.cfg.DST.Source)
	if dstType == "db" {
		db := &dbSourse{app: app, typeSourse: "dst"}
		go app.workerWithDST(db, workerDstDone)
	} else if dstType == "nats" {
		nt := &natsSourse{app: app, typeSourse: "dst", config: cfg}
		go app.workerWithDST(nt, workerDstDone)
	} else {
		app.log.Fatal("Bad destination")
	}

	osSignals := make(chan os.Signal, 1)
	signal.Notify(osSignals, os.Interrupt, syscall.SIGTERM)

	<-osSignals
	cancel()
	<-workerSrcDone
	<-workerDstDone
	app.log.Info("Service stoped")

}

func (a *application) workerWithSRC(src transfer, workDone chan struct{}) {
	if err := src.connect(); err != nil {
		a.log.Error("connect to SRC failed ", err.Error())
		close(a.toDst)
		workDone <- struct{}{}
		return
	}
	interval := a.cfg.Delay.Loop
	for {
		select {
		case <-a.ctx.Done():
			a.log.Info("woker withSRC stoping...")
			close(a.toDst)
			src.closeConn()
			workDone <- struct{}{}
			return
		case <-time.After(interval):
			if err := src.ping(); err != nil {
				a.log.Error("ping SRC failed")
				if err := src.connect(); err != nil {
					a.log.Error("connect SRC failed")
					interval = a.cfg.Delay.IfEmpty
					continue
				}
			}
			data, err := src.read()
			if err != nil {
				a.log.Error("can't read from SRC")
				continue
			}
			if data == nil {
				interval = a.cfg.Delay.IfEmpty
				continue
			}
			a.toDst <- data
			if _, ok := <-a.toSrc; !ok {
				a.log.Info("woker withSRC stoping...")
				close(a.toDst)
				workDone <- struct{}{}
				return
			}
			if err := src.write(data); err != nil {
				a.log.Error("write result error ", err.Error())
				continue
			}
			interval = a.cfg.Delay.Loop
		}
	}
}

func (a *application) workerWithDST(dst transfer, workDone chan struct{}) {
	if err := dst.connect(); err != nil {
		a.log.Error("connect to DST failed ", err.Error())
		close(a.toSrc)
		workDone <- struct{}{}
		return
	}
	for {
		select {
		case <-a.ctx.Done():
			a.log.Info("woker withDST stoping...")
			close(a.toSrc)
			dst.closeConn()
			workDone <- struct{}{}
			return
		case data, ok := <-a.toDst:
			if ok {
				if err := dst.ping(); err != nil {
					a.log.Error("ping DST failed")
					connect := false
					for !connect {
						if err := dst.connect(); err != nil {
							a.log.Error("connect DST failed, try reconnect")
							time.Sleep(a.cfg.Delay.IfError)
						} else {
							connect = true
						}
					}
				}
				if err := dst.write(data); err != nil {
					a.log.Error("write to DST error ", err.Error())
					continue
				}
				a.toSrc <- struct{}{}
			}
		}
	}
}

func checkSource(input string) (string, map[string]string) {
	params := strings.Split(input, " ")
	cfg := make(map[string]string)
	for _, v := range params {
		par := strings.Split(v, "=")
		cfg[par[0]] = par[1]
		if par[0] == "dbname" {
			return "db", nil
		}
	}
	if _, ok := cfg["clusterID"]; ok {
		return "nats", cfg
	}
	return "", nil
}

func newUUID() (string, error) {
	uuid := make([]byte, 16)
	n, err := io.ReadFull(rand.Reader, uuid)
	if n != len(uuid) || err != nil {
		return "", err
	}
	// variant bits; see section 4.1.1
	uuid[8] = uuid[8]&^0xc0 | 0x80
	// version 4 (pseudo-random); see section 4.1.3
	uuid[6] = uuid[6]&^0xf0 | 0x40
	// uuid[8] = (uuid[8] | 0x80) & 0xBF
	// uuid[6] = (uuid[6] | 0x40) & 0x4F

	return strings.ToUpper(fmt.Sprintf("%x-%x-%x-%x-%x", uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:])), nil
}

func b2s(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}
