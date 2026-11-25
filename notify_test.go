package pharos

import (
	"context"
	"strings"
	"sync"
	"testing"
)

// captureChannel records every delivered Notification.
type captureChannel struct {
	mu   sync.Mutex
	sent []Notification
}

func (c *captureChannel) Send(_ context.Context, n Notification) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sent = append(c.sent, n)
	return nil
}

func (c *captureChannel) all() []Notification {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]Notification(nil), c.sent...)
}

// prefixEncryptor is a trivial reversible "encryptor": ciphertext = "enc:" + plaintext.
type prefixEncryptor struct{}

func (prefixEncryptor) Encrypt(p string) (string, error) { return "enc:" + p, nil }
func (prefixEncryptor) Decrypt(c string) (string, error) {
	return strings.TrimPrefix(c, "enc:"), nil
}

func TestNotifier_RequiresStoreAndChannel(t *testing.T) {
	if _, err := NewStatusNotifier(NotifierConfig{Channel: &captureChannel{}}); err == nil {
		t.Fatal("missing Store must error")
	}
	if _, err := NewStatusNotifier(NotifierConfig{Store: NewMemStore()}); err == nil {
		t.Fatal("missing Channel must error")
	}
}

func TestNotifier_AdminAndSubscribersDecryptedAndTitled(t *testing.T) {
	store := NewMemStore()
	store.now = at1000
	// two verified subscribers, emails stored ENCRYPTED; one unverified (must be skipped)
	if err := store.InsertSubscriber(ctx, &Subscriber{EmailEncrypted: "enc:a@x.com", EmailHash: "ha", VerifyTokenHash: "va", Verified: true, VerifiedAt: 1}); err != nil {
		t.Fatalf("store.InsertSubscriber: %v", err)
	}
	if err := store.InsertSubscriber(ctx, &Subscriber{EmailEncrypted: "enc:b@x.com", EmailHash: "hb", VerifyTokenHash: "vb", Verified: true, VerifiedAt: 1}); err != nil {
		t.Fatalf("store.InsertSubscriber: %v", err)
	}
	if err := store.InsertSubscriber(ctx, &Subscriber{EmailEncrypted: "enc:c@x.com", EmailHash: "hc", VerifyTokenHash: "vc", Verified: false}); err != nil {
		t.Fatalf("store.InsertSubscriber: %v", err)
	}

	ch := &captureChannel{}
	n, err := NewStatusNotifier(NotifierConfig{
		Store:          store,
		Channel:        ch,
		Encryptor:      prefixEncryptor{},
		AdminRecipient: "ops@x.com",
		BatchPause:     0,
	})
	if err != nil {
		t.Fatal(err)
	}
	n.Start() // non-blocking: registers the worker before returning
	n.NotifyStatusChange(StatusChangeEvent{EndpointSlug: "api", EndpointName: "API", OldStatus: StatusOperational, NewStatus: StatusMajorOutage, Reason: "500s", IncidentID: "inc1"})
	n.Stop() // closes stopCh then drains any queued event before returning

	got := ch.all()
	// 1 admin + 2 verified subscribers (unverified skipped)
	if len(got) != 3 {
		t.Fatalf("expected admin + 2 verified subscribers = 3 sends; got %d: %+v", len(got), got)
	}
	var admin int
	subs := map[string]bool{}
	for _, msg := range got {
		if !strings.Contains(msg.Title, "🔴") || !strings.Contains(msg.Title, "major outage") {
			t.Fatalf("title must reflect major_outage; got %q", msg.Title)
		}
		if !strings.Contains(msg.Message, "Incident ID: inc1") {
			t.Fatalf("message must carry the incident id; got %q", msg.Message)
		}
		switch msg.Audience {
		case "admin":
			admin++
			if msg.To != "ops@x.com" {
				t.Fatalf("admin recipient wrong: %q", msg.To)
			}
		case "subscriber":
			subs[msg.To] = true
			if strings.HasPrefix(msg.To, "enc:") {
				t.Fatalf("subscriber email must be DECRYPTED before delivery; got %q", msg.To)
			}
		}
	}
	if admin != 1 || !subs["a@x.com"] || !subs["b@x.com"] || subs["c@x.com"] {
		t.Fatalf("want 1 admin + decrypted a,b (not unverified c); admin=%d subs=%v", admin, subs)
	}
}
