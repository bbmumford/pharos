package pharos

import "testing"

// With no Encryptor the notifier sends no subscriber mail, so a stored value
// is never used as an address; the admin alert still goes.
func TestNotifier_NoEncryptorWithholdsSubscriberMail(t *testing.T) {
	store := NewMemStore()
	store.now = at1000
	store.InsertSubscriber(ctx, &Subscriber{EmailEncrypted: "a@x.com", EmailHash: "ha", VerifyTokenHash: "va", Verified: true, VerifiedAt: 1})
	ch := &captureChannel{}
	n, err := NewStatusNotifier(NotifierConfig{Store: store, Channel: ch, AdminRecipient: "ops@x.com"})
	if err != nil {
		t.Fatal(err)
	}
	n.Start()
	n.NotifyStatusChange(StatusChangeEvent{EndpointSlug: "api", EndpointName: "API", OldStatus: StatusOperational, NewStatus: StatusMajorOutage})
	n.Stop()
	for _, msg := range ch.all() {
		if msg.Audience == "subscriber" {
			t.Fatalf("a subscriber alert went to %q with no Encryptor wired", msg.To)
		}
	}
	if got := ch.all(); len(got) != 1 || got[0].Audience != "admin" {
		t.Fatalf("want the admin alert alone; got %+v", got)
	}
}
