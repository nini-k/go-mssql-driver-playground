package go_mssql_driver_playground

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func printDBStats(stats *sql.DBStats) {
	fmt.Println("----------- db stat -----------")
	fmt.Println("in use -", stats.InUse)
	fmt.Println("idle -", stats.Idle)
	fmt.Println("opened -", stats.OpenConnections)
	fmt.Println("waited count -", stats.WaitCount)
	fmt.Println("wait duration", stats.WaitDuration)
}

type listener struct {
	lis   net.Listener
	close chan struct{}
}

func (li *listener) Listen(address string) error {
	l, err := net.Listen("tcp", address)
	if err != nil {
		return errors.Wrapf(err, "failed to listen at %s", address)
	}
	li.lis = l
	fmt.Printf("listen at %s\n", address)

	go func() {
		select {
		case <-li.close:
			return
		default:
			for {
				conn, err := l.Accept()
				if err != nil {
					panic(errors.Wrap(err, "error accepting"))
				}

				go func(conn net.Conn) {
					// Make a buffer to hold incoming data.
					buf := make([]byte, 1024)
					_, err := conn.Read(buf)
					if err != nil {
						fmt.Println("Error reading:", err.Error())
					}

					fmt.Printf("listener read: %s\n", string(buf))
				}(conn)
			}
		}
	}()

	return nil
}

func (li *listener) Close() {
	close(li.close)
	li.lis.Close()
}

func TestRun(t *testing.T) {
	// Проверяем, что mssql драйвер не закрывает коннект, после закрытия контекста
	t.Run("mssql driver without connection_timeout", func(t *testing.T) {
		const port = "8881"
		address := fmt.Sprintf("localhost:%s", port)
		l := listener{}
		if err := l.Listen(address); err != nil {
			panic(err)
		}

		db, err := sql.Open("sqlserver", fmt.Sprintf("sqlserver://sa:MyPass@word@localhost:%s", port))
		if err != nil {
			panic(err)
		}
		defer db.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		done := make(chan struct{})
		go func() {
			select {
			case <-done:
				return
			default:
				// Этот запрос никогда не выполнится, поскольку не получает ответа от сервера
				// поэтому спавним горутину которую закроем по каналу done
				_, err = db.QueryContext(ctx, "SELECT 1;")
				fmt.Println(errors.Wrap(err, "db.Query"))
			}
		}()

		<-ctx.Done()

		stats := db.Stats()
		printDBStats(&stats)
		// проверяем, после разрыва контекста коннект не закрылся
		require.Equal(t, 1, stats.OpenConnections)

		close(done)
	})

	// Проверяем, что mysql драйвер закрывает открытые коннекты по закрытию контекста
	t.Run("mysql driver", func(t *testing.T) {
		const port = "8882"
		address := fmt.Sprintf("localhost:%s", port)
		l := listener{}
		if err := l.Listen(address); err != nil {
			panic(err)
		}

		db, err := sql.Open("mysql", fmt.Sprintf("root:1234@tcp(localhost:%s)/db_test", port))
		if err != nil {
			panic(err)
		}
		defer db.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		// Проверяем, что если ответ от сервера долго не приходит протухание контекста рвет коннект к дб
		_, err = db.QueryContext(ctx, "SELECT 1;")
		require.Error(t, err)
		require.ErrorIs(t, err, context.DeadlineExceeded)

		stats := db.Stats()
		printDBStats(&stats)

		// проверяем, после закрытия контекста коннект закрылся
		require.Equal(t, 0, stats.OpenConnections)
	})

	// Чекаем, что mssql драйвер закрывает коннект, после закрытия контекста, если выставим connection_timeout
	t.Run("mssql driver with connection_timeout=4", func(t *testing.T) {
		const port = "8883"
		address := fmt.Sprintf("localhost:%s", port)
		l := listener{}
		if err := l.Listen(address); err != nil {
			panic(err)
		}

		db, err := sql.Open("sqlserver", fmt.Sprintf("sqlserver://sa:MyPass@word@localhost:%s?connection+timeout=4", port))
		if err != nil {
			panic(err)
		}
		defer db.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		_, err = db.QueryContext(ctx, "SELECT 1;")
		// Получаем ошибку i/o timeout
		require.Error(t, err)
		require.Contains(t, err.Error(), "i/o timeout")

		stats := db.Stats()
		printDBStats(&stats)

		// Проверяем, что после протухание контекста рвет открытый коннект
		require.Equal(t, 0, stats.OpenConnections)
	})

	// Проверяем, что коннекты упираются в узкое горлышко пула и часть из них висит в ожидании пока не отвалятся с context.DeadlineExceeded из за зомби коннекта
	t.Run("mssql waiting connections fails with context.DeadlineExceeded error due to zombie connection", func(t *testing.T) {
		const port = "8884"

		address := fmt.Sprintf("localhost:%s", port)
		l := listener{}
		if err := l.Listen(address); err != nil {
			panic(err)
		}

		db, err := sql.Open("sqlserver", fmt.Sprintf("sqlserver://sa:MyPass@word@localhost:%s", port))
		if err != nil {
			panic(err)
		}
		defer db.Close()

		ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)

		db.SetMaxOpenConns(1)
		db.SetMaxIdleConns(1)

		countAsyncQueriesToDb := 5
		done := make(chan struct{})
		for i := 0; i < countAsyncQueriesToDb; i++ {
			go func() {
				select {
				case <-done:
					return
				default:
					_, err = db.QueryContext(ctx, "SELECT 1;")
					// Проверяем, что коннекты в ожидании упали с context deadline exceeded
					require.ErrorIs(t, err, context.DeadlineExceeded)
				}
			}()
		}

		// Ждем когда контекст протухнит
		<-ctx.Done()

		stats := db.Stats()
		printDBStats(&stats)

		// Проверяем, что после протухание контекста не рвет коннект к дб
		require.Equal(t, 1, stats.OpenConnections)
		// Проверяем, что остальные коннекты были в ожидании
		require.Equal(t, int64(countAsyncQueriesToDb-1), stats.WaitCount)

		close(done)
	})

	// Проверяем, что connection_timeout не аффектит остальные открытые коннекты
	t.Run("mssql connection_timeout does not affected author connections", func(t *testing.T) {
		db, err := sql.Open("sqlserver", "sqlserver://sa:MyPass@word@localhost:1433?connection+timeout=5")
		if err != nil {
			panic(err)
		}
		defer db.Close()

		ctx := context.Background()
		countAsyncQueriesToDb := 5

		wg := sync.WaitGroup{}
		wg.Add(countAsyncQueriesToDb)

		errs := make(chan error, countAsyncQueriesToDb)
		for i := countAsyncQueriesToDb; i > 0; i-- {
			go func(second int) {
				defer wg.Done()
				// один запрос свалится из-за connection_timeout
				_, err = db.QueryContext(ctx, fmt.Sprintf("WAITFOR DELAY '00:00:0%d'", second))
				errs <- err
			}(i)
		}

		go func() {
			wg.Wait()
			close(errs)
		}()

		_errs := make([]error, 0, len(errs))
		for e := range errs {
			_errs = append(_errs, e)
		}

		countSuccess := 0
		countFails := 0
		for _, e := range _errs {
			if e == nil {
				countSuccess++
			} else {
				countFails++
			}
		}

		// проверяем, что все запросы кроме одного выполнились
		require.Equal(t, countAsyncQueriesToDb-1, countSuccess)
		require.Equal(t, 1, countFails)

		// проверяем что одна из ошибок по i/o timeout
		for _, e := range _errs {
			if e != nil {
				require.Contains(t, e.Error(), "i/o timeout")
			}
		}
	})
}
