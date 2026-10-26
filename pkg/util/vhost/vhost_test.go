// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package vhost

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestHandleClosesConnectionWhenResolvedListenerCloses(t *testing.T) {
	server, client := net.Pipe()
	t.Cleanup(func() { _ = server.Close() })
	t.Cleanup(func() { _ = client.Close() })

	resolved := make(chan struct{})
	resume := make(chan struct{})
	mux := &Muxer{
		timeout:        time.Second,
		registryRouter: NewRouters(),
		vhostFunc: func(conn net.Conn) (net.Conn, map[string]string, error) {
			return conn, map[string]string{"Host": "example.com"}, nil
		},
		successHook: func(net.Conn, map[string]string) error {
			close(resolved)
			<-resume
			return nil
		},
	}
	listener, err := mux.Listen(context.Background(), &RouteConfig{Domain: "example.com"})
	require.NoError(t, err)
	done := make(chan struct{})
	go func() {
		defer close(done)
		mux.handle(server)
	}()

	// Pause after route resolution, so Close happens before the handoff.
	<-resolved
	require.NoError(t, client.SetReadDeadline(time.Now().Add(time.Second)))
	require.NoError(t, listener.Close())
	close(resume)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("connection handler did not return after the listener closed")
	}

	_, err = client.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF, "rejected connection must be closed, not left waiting for the client timeout")
}

func TestHandleLeavesAcceptedConnectionOpen(t *testing.T) {
	server, client := net.Pipe()
	t.Cleanup(func() { _ = server.Close() })
	t.Cleanup(func() { _ = client.Close() })
	mux := &Muxer{
		timeout:        time.Second,
		registryRouter: NewRouters(),
		vhostFunc: func(conn net.Conn) (net.Conn, map[string]string, error) {
			return conn, map[string]string{"Host": "example.com"}, nil
		},
	}
	listener, err := mux.Listen(context.Background(), &RouteConfig{Domain: "example.com"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	done := make(chan struct{})
	go func() {
		defer close(done)
		mux.handle(server)
	}()
	conn, err := listener.Accept()
	require.NoError(t, err)
	<-done

	require.NoError(t, client.SetDeadline(time.Now().Add(time.Second)))
	require.NoError(t, conn.SetDeadline(time.Now().Add(time.Second)))
	written := make(chan error, 1)
	go func() {
		_, err := conn.Write([]byte("ok"))
		written <- err
	}()
	buf := make([]byte, 2)
	_, err = io.ReadFull(client, buf)
	require.NoError(t, err)
	require.Equal(t, "ok", string(buf))
	require.NoError(t, <-written)
}
