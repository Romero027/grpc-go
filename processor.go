/*
 *
 * Copyright 2016 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package grpc

import (
	"context"
	"log"
)

// ADNInvoker is called by UnaryClientInterceptor to complete RPCs.
type ADNInvoker func(ctx context.Context, method string, req, reply interface{}, cc *ClientConn, opts ...CallOption) error

// UnaryClientInterceptor intercepts the execution of a unary RPC on the client.
// Unary interceptors can be specified as a DialOption, using
// WithUnaryInterceptor() or WithChainUnaryInterceptor(), when creating a
// ClientConn. When a unary interceptor(s) is set on a ClientConn, gRPC
// delegates all unary RPC invocations to the interceptor, and it is the
// responsibility of the interceptor to call invoker to complete the processing
// of the RPC.
//
// method is the RPC name. req and reply are the corresponding request and
// response messages. cc is the ClientConn on which the RPC was invoked. invoker
// is the handler to complete the RPC and it is the responsibility of the
// interceptor to call it. opts contain all applicable call options, including
// defaults from the ClientConn as well as per-call options.
//
// The returned error must be compatible with the status package.
type ADNClientProcessor func(ctx context.Context, method string, req, reply interface{}, cc *ClientConn, invoker ADNInvoker, opts ...CallOption) error

// UnaryServerInfo consists of various information about a unary RPC on
// server side. All per-rpc information may be mutated by the interceptor.
type ADNServerInfo struct {
	// Server is the service implementation the user provides. This is read-only.
	Server interface{}
	// FullMethod is the full RPC method string, i.e., /package.service/method.
	FullMethod string
}

// UnaryHandler defines the handler invoked by UnaryServerInterceptor to complete the normal
// execution of a unary RPC.
//
// If a UnaryHandler returns an error, it should either be produced by the
// status package, or be one of the context errors. Otherwise, gRPC will use
// codes.Unknown as the status code and err.Error() as the status message of the
// RPC.
type ADNHandler func(ctx context.Context, req interface{}) (interface{}, error)

// UnaryServerInterceptor provides a hook to intercept the execution of a unary RPC on the server. info
// contains all the information of this RPC the interceptor can operate on. And handler is the wrapper
// of the service method implementation. It is the responsibility of the interceptor to invoke handler
// to complete the RPC.
type ADNServerProcessor func(ctx context.Context, req interface{}, info *ADNServerInfo, handler ADNHandler) (resp interface{}, err error)

// ChainUnaryClient creates a single interceptor out of a chain of many interceptors.
//
// Execution is done in left-to-right order, including passing of context.
// For example ChainUnaryClient(one, two, three) will execute one before two before three.
func ChainADNClientProcessors(processors ...ADNClientProcessor) ADNClientProcessor {
	n := len(processors)

	// Dummy interceptor maintained for backward compatibility to avoid returning nil.
	if n == 0 {
		return func(ctx context.Context, method string, req, reply interface{}, cc *ClientConn, invoker ADNInvoker, opts ...CallOption) error {
			return invoker(ctx, method, req, reply, cc, opts...)
		}
	}

	if n == 1 {
		return processors[0]
	}

	return func(ctx context.Context, method string, req, reply interface{}, cc *ClientConn, invoker ADNInvoker, opts ...CallOption) error {
		currInvoker := invoker
		for i := n - 1; i > 0; i-- {
			innerInvoker, i := currInvoker, i
			currInvoker = func(currentCtx context.Context, currentMethod string, currentReq, currentRepl interface{}, currentConn *ClientConn, currentOpts ...CallOption) error {
				return processors[i](currentCtx, currentMethod, currentReq, currentRepl, currentConn, innerInvoker, currentOpts...)
			}
		}
		return processors[0](ctx, method, req, reply, cc, currInvoker, opts...)
	}
}

// ChainUnaryServer creates a single interceptor out of a chain of many interceptors.
//
// Execution is done in left-to-right order, including passing of context.
// For example ChainUnaryServer(one, two, three) will execute one before two before three, and three
// will see context changes of one and two.
//
// While this can be useful in some scenarios, it is generally advisable to use google.golang.org/grpc.ChainUnaryInterceptor directly.
func ChainADNServerProcessor(processors ...ADNServerProcessor) ADNServerProcessor {
	log.Println("Hello from UnaryServerInterceptor2")
	n := len(processors)

	// Dummy interceptor maintained for backward compatibility to avoid returning nil.
	if n == 0 {
		return func(ctx context.Context, req interface{}, _ *ADNServerInfo, handler ADNHandler) (interface{}, error) {
			return handler(ctx, req)
		}
	}

	// The degenerate case, just return the single wrapped interceptor directly.
	if n == 1 {
		return processors[0]
	}

	// Return a function which satisfies the interceptor interface, and which is
	// a closure over the given list of interceptors to be chained.
	return func(ctx context.Context, req interface{}, info *ADNServerInfo, handler ADNHandler) (interface{}, error) {
		currHandler := handler
		// Iterate backwards through all interceptors except the first (outermost).
		// Wrap each one in a function which satisfies the handler interface, but
		// is also a closure over the `info` and `handler` parameters. Then pass
		// each pseudo-handler to the next outer interceptor as the handler to be called.
		for i := n - 1; i > 0; i-- {
			// Rebind to loop-local vars so they can be closed over.
			innerHandler, i := currHandler, i
			currHandler = func(currentCtx context.Context, currentReq interface{}) (interface{}, error) {
				return processors[i](currentCtx, currentReq, info, innerHandler)
			}
		}
		// Finally return the result of calling the outermost interceptor with the
		// outermost pseudo-handler created above as its handler.
		return processors[0](ctx, req, info, currHandler)
	}
}
