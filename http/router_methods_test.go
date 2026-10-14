//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"errors"
	stdhttp "net/http"
	"strings"
	"testing"
)

// TestRouterMethodShortcuts checks that every method's shortcut registers a
// route for that method alone, and that Walk lists them, with the routes of
// methods outside the standard set after them in order, and stops at the
// first error fn returns for one of those too.
func TestRouterMethodShortcuts(t *testing.T) {
	rec := new(recorder)
	r := newTestRouter(rec)
	r.Patch("/r", rec.handler("PATCH"))
	r.Delete("/r", rec.handler("DELETE"))
	r.Connect("/r", rec.handler("CONNECT"))
	r.Options("/r", rec.handler("OPTIONS"))
	r.Trace("/r", rec.handler("TRACE"))
	r.MethodFunc("PURGE", "/r", rec.handler("PURGE"))
	r.MethodFunc("LINK", "/r", rec.handler("LINK"))
	for _, method := range []string{"PATCH", "DELETE", "CONNECT", "OPTIONS", "TRACE", "PURGE", "LINK"} {
		expectRoute(t, rec.route(r, method, "/r"), method)
	}
	expectRoute(t, rec.route(r, stdhttp.MethodGet, "/r"), "405 PATCH, DELETE, CONNECT, OPTIONS, TRACE, LINK, PURGE")

	var walked []string
	if err := r.Walk(func(method, pattern string, _ Handler) error {
		if pattern == "/r" {
			walked = append(walked, method)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(walked, " "); got != "PATCH DELETE CONNECT OPTIONS TRACE LINK PURGE" {
		t.Fatalf("Walk listed %q", got)
	}
	stop := errors.New("stop")
	if err := r.Walk(func(method, _ string, _ Handler) error {
		if method == "LINK" {
			return stop
		}
		return nil
	}); !errors.Is(err, stop) {
		t.Fatalf("Walk returned %v, want the error fn returned for LINK", err)
	}
}

// TestRouterEmpty checks a router with no routes: it answers every request
// with a plain 404, and has nothing to walk.
func TestRouterEmpty(t *testing.T) {
	r := new(Router)
	stream := new(extStream)
	_, c := newExtRequest(stdhttp.MethodGet, "/anything", stream)
	Serve(r, c)
	if got := stream.response(t); got.StatusCode != stdhttp.StatusNotFound {
		t.Fatalf("an empty router answered %d", got.StatusCode)
	}
	if err := r.Walk(func(string, string, Handler) error { return errors.New("called") }); err != nil {
		t.Fatalf("walking an empty router: %v", err)
	}
}

// TestRouterRegistrationPanics checks the registrations that are programming
// errors, which panic as net/http's ServeMux does, rather than leaving a
// route that never matches.
func TestRouterRegistrationPanics(t *testing.T) {
	r := NewRouter()
	for name, register := range map[string]func(){
		"Route without a leading slash": func() { r.Route("api", nil) },
		"Mount of a nil handler":        func() { r.Mount("/m", nil) },
		"Mount without a leading slash": func() { r.Mount("m", NewRouter()) },
		"route without a leading slash": func() { r.Get("users", func(*Context) {}) },
		"route for an empty method":     func() { r.MethodFunc("", "/x", func(*Context) {}) },
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s did not panic", name)
				}
			}()
			register()
		}()
	}
}

// TestRouterGroupNotFound checks that NotFound set on a router from Group
// sets the handler of the router it came from, and that a nil handler then
// restores the plain 404.
func TestRouterGroupNotFound(t *testing.T) {
	rec := new(recorder)
	r := NewRouter()
	r.Get("/x", rec.handler("x"))
	group := r.Group(nil)
	group.NotFound(rec.handler("group 404"))
	expectRoute(t, rec.route(r, stdhttp.MethodGet, "/missing"), "group 404")

	group.NotFound(nil)
	stream := new(extStream)
	_, c := newExtRequest(stdhttp.MethodGet, "/missing", stream)
	Serve(r, c)
	if got := stream.response(t); got.StatusCode != stdhttp.StatusNotFound {
		t.Fatalf("the restored 404 answered %d", got.StatusCode)
	}
}

// TestRouterParamsEdges checks Param and Params where nothing matches: a
// Context no router served, a name the route has no parameter for, and a
// range over Params that stops early. It also checks that a parameter whose
// value would take a "/" does not match.
func TestRouterParamsEdges(t *testing.T) {
	if got := new(Context).Param("id"); got != "" {
		t.Fatalf("Param on an unrouted Context = %q", got)
	}
	r := NewRouter()
	var param, missing string
	var first []string
	r.Get("/archive/{year}-{month}", func(c *Context) {
		param, missing = c.Param("year"), c.Param("day")
		for k := range c.Params() {
			first = append(first, k)
			break
		}
	})
	rec := new(recorder)
	rec.route(r, stdhttp.MethodGet, "/archive/2026-10")
	if param != "2026" || missing != "" || len(first) != 1 || first[0] != "year" {
		t.Fatalf("year %q, day %q, first params %v", param, missing, first)
	}
	r.NotFound(rec.handler("404"))
	expectRoute(t, rec.route(r, stdhttp.MethodGet, "/archive/20/26-10"), "404")
}

// TestRouterHandsOnToAnotherRouter has a route's handler hand its request on
// to a second router, as a handler that delegates does: the second routes it
// afresh, rather than taking the first route's state, which is shared by
// every request that route serves, as its own.
func TestRouterHandsOnToAnotherRouter(t *testing.T) {
	rec := new(recorder)
	inner := NewRouter()
	inner.Get("/users/{id}", rec.handler("inner"))
	outer := NewRouter()
	outer.Get("/users/static", func(c *Context) {
		if c.RoutePattern() != "/users/static" {
			t.Errorf("outer route %q", c.RoutePattern())
		}
		inner.ServeHTTP(c)
	})
	expectRoute(t, rec.route(outer, stdhttp.MethodGet, "/users/static"), "inner", "id", "static")
}

// TestRouterWithoutURL routes a request that has no URL, with the route
// state an earlier request left in its Context, which is cleared rather
// than read: the request is not found.
func TestRouterWithoutURL(t *testing.T) {
	rec := new(recorder)
	r := newTestRouter(rec)
	r.Get("/users/{id}", rec.handler("id"))
	c := new(Context)
	expectRoute(t, rec.routeOn(c, r, stdhttp.MethodGet, "/users/7"), "id", "id", "7")
	c.Request = &stdhttp.Request{Method: stdhttp.MethodGet}
	rec.last = nil
	r.ServeHTTP(c)
	expectRoute(t, rec.last, "404")
}

// TestRouterRecyclesSharedRouteState serves requests through a router on a
// server that recycles its Contexts: a route with no parameters leaves its
// request the state its endpoint shares, which a recycled Context drops
// rather than clearing, so the next request, routed by a parameter, gets its
// own.
func TestRouterRecyclesSharedRouteState(t *testing.T) {
	r := NewRouter()
	r.Get("/static", func(c *Context) { _ = c.Respond(stdhttp.StatusOK, "text/plain", []byte("static")) })
	r.Get("/users/{id}", func(c *Context) { _ = c.Respond(stdhttp.StatusOK, "text/plain", []byte(c.Param("id"))) })
	config := DefaultConfig()
	setReuseAll(&config)
	addr := serve(t, NewHandlerWithConfig(config, r))
	conn := dialRaw(t, addr)
	for _, want := range []string{"static", "7", "static", "8"} {
		path := "/static"
		if want != "static" {
			path = "/users/" + want
		}
		conn.send("GET " + path + " HTTP/1.1\r\nHost: test\r\n\r\n")
		if _, body := conn.response(stdhttp.MethodGet); body != want {
			t.Fatalf("GET %s = %q", path, body)
		}
	}
}
