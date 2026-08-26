package pharos

import (
	"context"
	"sync"
	"time"
)

// Notification is one message to deliver. Audience lets a channel route admin vs subscriber traffic
// differently (an admin alert and a subscriber alert are distinct audiences).
type Notification struct {
	Kind     string // e.g. "service-alert"
	To       string // recipient address (admin, or a subscriber's decrypted email)
	Title    string
	Message  string
	Audience string // "admin" | "subscriber"
}

// NotificationChannel delivers a Notification. It is the pluggable delivery sink: a consumer wires
// email / SMS / webhook / a notification service, and can compose several. Delivery being an injected
// seam rather than a hard-wired call is what makes the status system delivery-agnostic.
type NotificationChannel interface {
	Send(ctx context.Context, n Notification) error
}

// Encryptor encrypts on subscribe and decrypts a subscriber's stored email just-in-time to address a
// message. The store holds only ciphertext; the notifier never persists plaintext. Injected — a
// deployment with real subscriber PII wires AES-GCM/etc. A nil Encryptor uses the stored value as-is,
// for a deployment that has not wired encryption yet.
type Encryptor interface {
	Encrypt(plaintext string) (string, error)
	Decrypt(ciphertext string) (string, error)
}

// StatusChangeEvent is a status transition needing notification.
type StatusChangeEvent struct {
	EndpointSlug string
	EndpointName string
	OldStatus    string
	NewStatus    string
	Reason       string
	IncidentID   string
}

// NotifierConfig wires a StatusNotifier. Store and Channel are required.
type NotifierConfig struct {
	Store          StatusStore
	Channel        NotificationChannel
	Encryptor      Encryptor     // optional; nil ⇒ stored email used as-is
	AdminRecipient string        // admin alert address; "" ⇒ skip the admin alert
	BatchSize      int           // subscriber fan-out batch size (default 50)
	BatchPause     time.Duration // pause between batches (default 1s); 0 in tests
	QueueSize      int           // event queue depth (default 100)
	Logf           func(string, ...any)
}

// StatusNotifier fans a status change out to the admin recipient and every verified subscriber over
// the injected channel, decrypting each subscriber's email just-in-time.
type StatusNotifier struct {
	store          StatusStore
	channel        NotificationChannel
	enc            Encryptor
	adminRecipient string
	batchSize      int
	batchPause     time.Duration
	logf           func(string, ...any)

	eventCh chan StatusChangeEvent
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

// NewStatusNotifier validates the config and returns a notifier.
func NewStatusNotifier(c NotifierConfig) (*StatusNotifier, error) {
	if c.Store == nil {
		return nil, &configError{"pharos: NotifierConfig.Store is required"}
	}
	if c.Channel == nil {
		return nil, &configError{"pharos: NotifierConfig.Channel is required"}
	}
	n := &StatusNotifier{
		store:          c.Store,
		channel:        c.Channel,
		enc:            c.Encryptor,
		adminRecipient: c.AdminRecipient,
		batchSize:      c.BatchSize,
		batchPause:     c.BatchPause,
		logf:           c.Logf,
		stopCh:         make(chan struct{}),
	}
	if n.batchSize <= 0 {
		n.batchSize = 50
	}
	if c.QueueSize <= 0 {
		c.QueueSize = 100
	}
	n.eventCh = make(chan StatusChangeEvent, c.QueueSize)
	if n.logf == nil {
		n.logf = func(string, ...any) {}
	}
	return n, nil
}

// Start launches the delivery worker (non-blocking, unlike the polling services): it registers with
// the wait group synchronously before returning, so a Stop that follows always waits for the worker.
// The worker processes queued events until Stop, draining the queue on shutdown.
func (n *StatusNotifier) Start() {
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		for {
			select {
			case e := <-n.eventCh:
				n.deliver(e)
			case <-n.stopCh:
				for len(n.eventCh) > 0 {
					n.deliver(<-n.eventCh)
				}
				return
			}
		}
	}()
}

// Stop stops the loop after draining the queue.
func (n *StatusNotifier) Stop() {
	close(n.stopCh)
	n.wg.Wait()
}

// NotifyStatusChange queues an event; a full queue drops it (with a log) rather than blocking the
// caller (the health checker).
func (n *StatusNotifier) NotifyStatusChange(e StatusChangeEvent) {
	select {
	case n.eventCh <- e:
	default:
		n.logf("notifier: queue full, dropping event for %s", e.EndpointSlug)
	}
}

// deliver sends the admin alert then fans out to verified subscribers.
func (n *StatusNotifier) deliver(e StatusChangeEvent) {
	title := buildAlertTitle(e)
	message := buildAlertMessage(e)

	if n.adminRecipient != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		if err := n.channel.Send(ctx, Notification{Kind: "service-alert", To: n.adminRecipient, Title: title, Message: message, Audience: "admin"}); err != nil {
			n.logf("notifier: admin alert %s: %v", e.EndpointSlug, err)
		}
		cancel()
	}
	n.notifySubscribers(title, message)
}

// notifySubscribers delivers to every verified subscriber in batches, decrypting each email.
func (n *StatusNotifier) notifySubscribers(title, message string) {
	subs, err := n.store.GetVerifiedSubscribers(context.Background())
	if err != nil {
		n.logf("notifier: get subscribers: %v", err)
		return
	}
	var sent, failed int
	for i := 0; i < len(subs); i += n.batchSize {
		end := i + n.batchSize
		if end > len(subs) {
			end = len(subs)
		}
		for _, s := range subs[i:end] {
			email, derr := n.decryptEmail(s.EmailEncrypted)
			if derr != nil {
				failed++
				n.logf("notifier: decrypt subscriber %s: %v", s.ID, derr)
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			if err := n.channel.Send(ctx, Notification{Kind: "service-alert", To: email, Title: title, Message: message, Audience: "subscriber"}); err != nil {
				failed++
				n.logf("notifier: notify subscriber %s: %v", s.ID, err)
			} else {
				sent++
			}
			cancel()
		}
		if end < len(subs) && n.batchPause > 0 {
			time.Sleep(n.batchPause)
		}
	}
	n.logf("notifier: subscriber fan-out complete (sent=%d failed=%d)", sent, failed)
}

// decryptEmail returns the plaintext email; with no Encryptor the stored value is used as-is.
func (n *StatusNotifier) decryptEmail(stored string) (string, error) {
	if n.enc == nil {
		return stored, nil
	}
	return n.enc.Decrypt(stored)
}

func buildAlertTitle(e StatusChangeEvent) string {
	name := e.EndpointName
	if name == "" {
		name = e.EndpointSlug
	}
	switch e.NewStatus {
	case StatusOperational:
		return "✓ " + name + " is operational"
	case StatusDegraded:
		return "⚠ " + name + " is degraded"
	case StatusPartialOutage:
		return "⚠ " + name + " partial outage"
	case StatusMajorOutage:
		return "🔴 " + name + " major outage"
	case StatusMaintenance:
		return "🔧 " + name + " in maintenance"
	default:
		return "Status change: " + name
	}
}

func buildAlertMessage(e StatusChangeEvent) string {
	msg := "Endpoint: " + e.EndpointSlug + "\n" +
		"Previous status: " + e.OldStatus + "\n" +
		"Current status: " + e.NewStatus + "\n"
	if e.Reason != "" {
		msg += "Reason: " + e.Reason + "\n"
	}
	if e.IncidentID != "" {
		msg += "Incident ID: " + e.IncidentID + "\n"
	}
	return msg
}
