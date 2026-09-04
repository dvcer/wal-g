package mysql

import (
	"context"
	"errors"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/server"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"golang.org/x/sync/errgroup"
)

type binlogSourceParams struct {
	rootFolder        storage.Folder
	dstDir            string
	startTS           time.Time
	untilTS           time.Time
	endBinlogTS       time.Time
	serverID          int
	heartbeatDisabled bool
}

// Handler is the go-mysql replication handler for one replica connection.
// It implements server.ReplicationHandler (go-mysql interface) and delegates
// the actual fetch/parse/stream pipeline to a BinlogDumpRequestProcessor
type Handler struct {
	server.EmptyReplicationHandler
	ctx                  context.Context //nolint:containedctx // detached binlog replication server outlives any request
	cancel               context.CancelFunc
	replicaSource        string
	replicaStreamer      *replication.BinlogStreamer
	dumpCommandProcessor *BinlogDumpProcessor
}

var errReplicaCaughtUp = errors.New("replica caught up")

var heartbeatPeriodAssignmentPattern = regexp.MustCompile(
	`(?i)@(?:master|source)_heartbeat_period\s*=\s*([0-9]+)`,
)

func newHandler(ctx context.Context, replicaSource string, params binlogSourceParams) *Handler {
	ctx, cancel := context.WithCancel(ctx)
	replicaStreamer := replication.NewBinlogStreamer()
	return &Handler{
		ctx:                  ctx,
		cancel:               cancel,
		replicaSource:        replicaSource,
		replicaStreamer:      replicaStreamer,
		dumpCommandProcessor: newBinlogDumpRequestProcessor(ctx, params, params.serverID, &replicaStreamerSink{replicaStreamer: replicaStreamer}),
	}
}

// streamToReplica runs the streaming pipeline to completion and then waits
// for the replica to catch up before shutting the process down.
func (h *Handler) streamToReplica() {
	tracelog.InfoLogger.Printf("Start event streaming")

	if err := h.dumpCommandProcessor.process(); err != nil {
		tracelog.ErrorLogger.Printf("Error during logs streaming: %v", err)
		h.replicaStreamer.AddErrorToStreamer(err)
		return
	}

	tracelog.InfoLogger.Printf("Event streaming finished")
	h.replicaStreamer.AddErrorToStreamer(h.waitForReplicaW())
}

func (h *Handler) waitForReplicaW() error {
	g, ctx := errgroup.WithContext(h.ctx)
	g.Go(func() error {
		return h.dumpCommandProcessor.runIdleHeartbeats(ctx)
	})
	g.Go(func() error {
		if err := h.waitForReplica(ctx); err != nil {
			return err
		}
		return errReplicaCaughtUp
	})
	return g.Wait()
}

// waitForReplica blocks until the replica's executed GTID set covers every
// GTID that was streamed. If nothing was streamed, it returns immediately.
func (h *Handler) waitForReplica(ctx context.Context) error {
	sentGTIDs := h.dumpCommandProcessor.sentGTIDs
	if sentGTIDs.IsEmpty() {
		tracelog.InfoLogger.Println("S3 objects finished. No GTIDs were sent. Finishing immediately.")
		return nil
	}

	tracelog.InfoLogger.Printf("All S3 binlogs processed. Waiting for replica to catch up to GTID: %s", sentGTIDs.String())

	dsn, err := parseMySQLDatasource(h.replicaSource)
	if err != nil {
		return fmt.Errorf("failed to parse replica datasource: %w", err)
	}
	var conn *client.Conn
	connCount := 0
	defer func() {
		if conn != nil {
			conn.Close()
		}
	}()

	for {
		if ctx.Err() != nil {
			tracelog.WarningLogger.Println("Client disconnected while waiting for completion. Handler shutting down, awaiting reconnect...")
			return ctx.Err()
		}

		if conn == nil {
			if conn, err = connectMySQL(ctx, dsn, ""); err != nil {
				connCount++
				if connCount >= 10 {
					return fmt.Errorf("failed to connect to replica SQL 10 times, giving up: %w", err)
				} else if connCount > 1 {
					tracelog.WarningLogger.Printf("Failed to connect to replica SQL (times: %d): %v", connCount, err)
				} else {
					tracelog.WarningLogger.Printf("Failed to connect to replica SQL: %v", err)
				}
				time.Sleep(time.Second)
				continue
			}
			connCount = 0
		}

		r, err := conn.Execute("SELECT @@global.gtid_executed")
		if err != nil {
			tracelog.WarningLogger.Printf("Failed to query replica GTID state: %v", err)
			conn.Close()
			conn = nil
			time.Sleep(time.Second)
			continue
		}
		executedStr, _ := r.GetString(0, 0)
		r.Close()

		replicaSet, _ := mysql.ParseGTIDSet("mysql", executedStr)
		tracelog.DebugLogger.Printf("waitForReplica: replica gtid_executed=%q, waiting for=%q",
			executedStr, sentGTIDs.String())
		if replicaSet != nil && replicaSet.Contain(sentGTIDs) {
			tracelog.InfoLogger.Println("Replica has successfully caught up! We are safely done.")
			return nil
		}

		time.Sleep(time.Second)
	}
}

func (h *Handler) HandleRegisterSlave(data []byte) error {
	return nil
}

func (h *Handler) HandleBinlogDump(pos mysql.Position) (*replication.BinlogStreamer, error) {
	tracelog.InfoLogger.Printf("HandleBinlogDump: requested position %s:%d", pos.Name, pos.Pos)
	// Ignore position as we always start from the beginning. It's safe as GTIDs provide deduplication.
	go h.streamToReplica()
	return h.replicaStreamer, nil
}

func (h *Handler) HandleBinlogDumpGTID(gtidSet *mysql.MysqlGTIDSet) (*replication.BinlogStreamer, error) {
	tracelog.InfoLogger.Printf("HandleBinlogDumpGTID: GTID=%s", gtidSet.String())
	h.dumpCommandProcessor.requiredGTIDs = gtidSet
	go h.streamToReplica()
	return h.replicaStreamer, nil
}

func (h *Handler) HandleQuery(query string) (*mysql.Result, error) {
	query = strings.TrimSpace(query)

	switch {
	case heartbeatPeriodAssignmentPattern.MatchString(query):
		heartbeatPeriod, err := parseHeartbeatPeriod(query)
		if err != nil {
			return nil, err
		}
		h.dumpCommandProcessor.heartbeatPeriod = heartbeatPeriod
		tracelog.InfoLogger.Printf("Replica requested heartbeat period: %s", heartbeatPeriod)
		return &mysql.Result{Status: 34}, nil
	case strings.EqualFold(query, "select @master_binlog_checksum"):
		resultSet, _ := mysql.BuildSimpleTextResultset([]string{"master_binlog_checksum"}, [][]interface{}{{"CRC32"}})
		return &mysql.Result{Status: 34, Warnings: 0, InsertId: 0, AffectedRows: 0, Resultset: resultSet}, nil
	case strings.EqualFold(query, "select @source_binlog_checksum"):
		// "1" - CRC algorithm from zlib
		resultSet, _ := mysql.BuildSimpleTextResultset([]string{"source_binlog_checksum"}, [][]interface{}{{"1"}})
		return &mysql.Result{Status: 34, Warnings: 0, InsertId: 0, AffectedRows: 0, Resultset: resultSet}, nil
	case strings.EqualFold(query, "show global variables like 'binlog_checksum'"):
		resultSet, _ := mysql.BuildSimpleTextResultset([]string{"BINLOG_CHECKSUM"}, [][]interface{}{{"CRC32"}})
		return &mysql.Result{Status: 34, Warnings: 0, InsertId: 0, AffectedRows: 0, Resultset: resultSet}, nil
	case strings.EqualFold(query, "select @@global.server_id"):
		resultSet, err := mysql.BuildSimpleTextResultset([]string{"SERVER_ID"}, [][]interface{}{{h.dumpCommandProcessor.serverID}})
		tracelog.ErrorLogger.FatalOnError(err)
		return &mysql.Result{Status: 34, Warnings: 0, InsertId: 0, AffectedRows: 0, Resultset: resultSet}, nil
	case strings.EqualFold(query, "select @@global.gtid_mode"):
		resultSet, _ := mysql.BuildSimpleTextResultset([]string{"GTID_MODE"}, [][]interface{}{{"ON"}})
		return &mysql.Result{Status: 34, Warnings: 0, InsertId: 0, AffectedRows: 0, Resultset: resultSet}, nil
	case strings.EqualFold(query, "select @@global.server_uuid"):
		// The server UUID received by the query does not affect replication.
		// During replication, the UUID is taken from events.
		resultSet, _ := mysql.BuildSimpleTextResultset([]string{"SERVER_UUID"}, [][]interface{}{{"0"}})
		return &mysql.Result{Status: 34, Warnings: 0, InsertId: 0, AffectedRows: 0, Resultset: resultSet}, nil
	case strings.EqualFold(query, "select @@global.rpl_semi_sync_master_enabled"):
		resultSet, _ := mysql.BuildSimpleTextResultset([]string{"@@global.rpl_semi_sync_master_enabled"}, [][]interface{}{{"0"}})
		return &mysql.Result{Status: 34, Warnings: 0, InsertId: 0, AffectedRows: 0, Resultset: resultSet}, nil
	case strings.EqualFold(query, "select @@global.rpl_semi_sync_source_enabled"):
		resultSet, _ := mysql.BuildSimpleTextResultset([]string{"@@global.rpl_semi_sync_source_enabled"}, [][]interface{}{{"0"}})
		return &mysql.Result{Status: 34, Warnings: 0, InsertId: 0, AffectedRows: 0, Resultset: resultSet}, nil
	default:
		tracelog.DebugLogger.Printf("Unhandled query: %s", query)
		return nil, nil
	}
}

func parseHeartbeatPeriod(query string) (time.Duration, error) {
	matches := heartbeatPeriodAssignmentPattern.FindAllStringSubmatch(query, -1)
	if len(matches) == 0 {
		return 0, fmt.Errorf("heartbeat period assignment is missing")
	}

	var heartbeatPeriod time.Duration
	for _, match := range matches {
		nanoseconds, err := strconv.ParseInt(match[1], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid heartbeat period %q: %w", match[1], err)
		}
		heartbeatPeriod = time.Duration(nanoseconds)
	}

	return heartbeatPeriod, nil
}

func HandleBinlogServer(ctx context.Context, since string, until string, untilBinlogLastModified string) {
	// get necessary settings
	st, err := internal.ConfigureStorage(ctx)
	tracelog.ErrorLogger.FatalOnError(err)
	startTS, untilTS, endBinlogTS, err := getTimestamps(ctx, st.RootFolder(), since, until, untilBinlogLastModified)
	tracelog.ErrorLogger.FatalOnError(err)

	// validate WALG_MYSQL_BINLOG_SERVER_REPLICA_SOURCE
	replicaSource, err := conf.GetRequiredSetting(conf.MysqlBinlogServerReplicaSource)
	tracelog.ErrorLogger.FatalOnError(err)
	_, err = parseMySQLDatasource(replicaSource)
	tracelog.ErrorLogger.FatalOnError(err)

	dstDir, err := internal.GetLogsDstSettings(conf.MysqlBinlogDstSetting)
	tracelog.ErrorLogger.FatalOnError(err)

	serverAddress, err := conf.GetRequiredSetting(conf.MysqlBinlogServerHost)
	tracelog.ErrorLogger.FatalOnError(err)
	serverPort, err := conf.GetRequiredSetting(conf.MysqlBinlogServerPort)
	tracelog.ErrorLogger.FatalOnError(err)

	serverIDSetting, err := conf.GetRequiredSetting(conf.MysqlBinlogServerID)
	tracelog.ErrorLogger.FatalOnError(err)
	serverID, err := strconv.Atoi(serverIDSetting)
	tracelog.ErrorLogger.FatalOnError(err)
	heartbeatDisabled, err := conf.GetBoolSettingDefault(conf.MysqlBinlogServerDisableHeartbeat, false)
	tracelog.ErrorLogger.FatalOnError(err)

	l, err := net.Listen("tcp", serverAddress+":"+serverPort)
	tracelog.ErrorLogger.FatalOnError(err)
	tracelog.InfoLogger.Printf("Listening on %s, wait connection", l.Addr())

	srv := server.NewServer("5.7.42", mysql.DEFAULT_COLLATION_ID, mysql.AUTH_NATIVE_PASSWORD, nil, nil)
	// Process one replication connection at a time. Any connection error
	// returns control to Accept; confirmed replica catch-up finishes the command.
	for {
		c, err := l.Accept()
		if err != nil {
			tracelog.ErrorLogger.Printf("Error accepting connection: %v", err)
			continue
		}
		tracelog.InfoLogger.Printf("Connection accepted from %s", c.RemoteAddr())

		user, err := conf.GetRequiredSetting(conf.MysqlBinlogServerUser)
		if err != nil {
			tracelog.ErrorLogger.Printf("Config error: %v", err)
			c.Close()
			continue
		}
		password, err := conf.GetRequiredSetting(conf.MysqlBinlogServerPassword)
		if err != nil {
			tracelog.ErrorLogger.Printf("Config error: %v", err)
			c.Close()
			continue
		}

		params := binlogSourceParams{
			rootFolder:        st.RootFolder(),
			dstDir:            dstDir,
			startTS:           startTS,
			untilTS:           untilTS,
			endBinlogTS:       endBinlogTS,
			serverID:          serverID,
			heartbeatDisabled: heartbeatDisabled,
		}
		err = handleBinlogConnection(ctx, c, srv, replicaSource, params, user, password)
		if errors.Is(err, errReplicaCaughtUp) {
			return
		}
		tracelog.WarningLogger.Printf("Replication connection closed: %v. Waiting for new connection...", err)
	}
}

func handleBinlogConnection(
	ctx context.Context,
	c net.Conn,
	srv *server.Server,
	replicaSource string,
	params binlogSourceParams,
	user string,
	password string,
) error {
	h := newHandler(ctx, replicaSource, params)
	defer func() {
		h.cancel()
		c.Close()
	}()

	authHandler := server.NewInMemoryAuthenticationHandler(mysql.AUTH_NATIVE_PASSWORD)
	if errAuth := authHandler.AddUser(user, password); errAuth != nil {
		return fmt.Errorf("failed to set user auth: %w", errAuth)
	}

	conn, err := srv.NewCustomizedConn(c, authHandler, h)
	if err != nil {
		if strings.Contains(err.Error(), "EOF") || strings.Contains(err.Error(), "bad") {
			return fmt.Errorf("handshake dropped (network issue/proxy): %w", err)
		}
		return fmt.Errorf("error creating connection: %w", err)
	}

	defer func() {
		if !conn.Closed() {
			conn.Close()
		}
	}()

	for {
		if err := conn.HandleCommand(); err != nil {
			return err
		}
	}
}
