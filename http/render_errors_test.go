//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || windows

package http

import (
	"html/template"
	stdhttp "net/http"
	"testing"
)

// TestContextRenderErrors checks the render helpers' failures and the
// template helper's unnamed form: a value XML cannot encode is reported and
// nothing is answered, and an empty template name executes the template
// itself.
func TestContextRenderErrors(t *testing.T) {
	stream := new(extStream)
	_, c := newExtRequest(stdhttp.MethodGet, "/xml", stream)
	if err := c.XML(stdhttp.StatusOK, make(chan int)); err == nil {
		t.Fatal("XML encoded a channel")
	}
	if err := c.IndentedXML(stdhttp.StatusOK, make(chan int)); err == nil {
		t.Fatal("IndentedXML encoded a channel")
	}
	if len(stream.responses) != 0 {
		t.Fatal("a failed encoding was answered")
	}
	tmpl := template.Must(template.New("page").Parse(`<i>{{.}}</i>`))
	if err := c.HTMLTemplate(stdhttp.StatusOK, tmpl, "", "x"); err != nil {
		t.Fatal(err)
	}
	if got := stream.response(t); string(got.Body) != "<i>x</i>" {
		t.Fatalf("body %q", got.Body)
	}
}

// TestAttachmentNameControlCharacters checks that the name a client saves
// a file as cannot break out of its quoted-string: quotes and control
// characters are replaced, and a directory part is dropped.
func TestAttachmentNameControlCharacters(t *testing.T) {
	if got := attachmentName(`dir\a"b` + "\r\n\x7f.txt"); got != "a_b___.txt" {
		t.Fatalf("attachmentName = %q", got)
	}
}
