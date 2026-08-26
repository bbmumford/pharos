package pharos

import (
	"errors"
	"testing"
	"time"
)

func newSubSvc(t *testing.T, store *MemStore, tokens ...string) *SubscriberService {
	t.Helper()
	i := 0
	svc, err := NewSubscriberService(SubscriberConfig{
		Store:     store,
		Encryptor: prefixEncryptor{},
		Now:       func() time.Time { return time.Unix(1000, 0) },
		NewToken: func() string {
			tk := tokens[i%len(tokens)]
			i++
			return tk
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return svc
}

func TestSubscriber_RequiresEncryptor(t *testing.T) {
	if _, err := NewSubscriberService(SubscriberConfig{Store: NewMemStore()}); err == nil {
		t.Fatal("a nil Encryptor must error (PII must be encrypted at rest)")
	}
}

func TestSubscriber_SubscribeStoresCiphertextAndHashes(t *testing.T) {
	store := NewMemStore()
	svc := newSubSvc(t, store, "vtok", "utok")
	vt, ut, err := svc.Subscribe(ctx, "  Alice@Example.com ")
	if err != nil || vt != "vtok" || ut != "utok" {
		t.Fatalf("Subscribe returned %q %q %v", vt, ut, err)
	}
	// exists by the normalized-email hash
	if ok, _ := store.SubscriberExistsByEmailHash(ctx, hashHex("alice@example.com")); !ok {
		t.Fatal("subscriber must be findable by the normalized email hash")
	}
	// the store holds ciphertext + token HASHES, never plaintext or raw tokens
	var got *Subscriber
	for _, s := range store.subscribers {
		got = s
	}
	if got.EmailEncrypted != "enc:alice@example.com" {
		t.Fatalf("email must be stored encrypted; got %q", got.EmailEncrypted)
	}
	if got.VerifyTokenHash != hashHex("vtok") || got.UnsubTokenHash != hashHex("utok") {
		t.Fatal("tokens must be stored hashed, not raw")
	}
	if got.Verified {
		t.Fatal("a new subscriber starts unverified")
	}
}

func TestSubscriber_DuplicateEmailRejected(t *testing.T) {
	store := NewMemStore()
	svc := newSubSvc(t, store, "v1", "u1", "v2", "u2")
	if _, _, err := svc.Subscribe(ctx, "a@x.com"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := svc.Subscribe(ctx, "A@X.com"); !errors.Is(err, ErrAlreadySubscribed) {
		t.Fatal("a duplicate email (case-insensitive) must be rejected")
	}
}

func TestSubscriber_VerifyAndUnsubscribeByRawToken(t *testing.T) {
	store := NewMemStore()
	svc := newSubSvc(t, store, "vtok", "utok")
	if _, _, err := svc.Subscribe(ctx, "a@x.com"); err != nil {
		t.Fatal(err)
	}
	// wrong token does not verify
	if err := svc.Verify(ctx, "nope"); !errors.Is(err, ErrNotFound) {
		t.Fatal("an unknown verify token must be ErrNotFound")
	}
	if err := svc.Verify(ctx, "vtok"); err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if v, _ := store.GetVerifiedSubscribers(ctx); len(v) != 1 {
		t.Fatal("after Verify the subscriber must be in the verified set")
	}
	if err := svc.Unsubscribe(ctx, "utok"); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	if v, _ := store.GetVerifiedSubscribers(ctx); len(v) != 0 {
		t.Fatal("after Unsubscribe the subscriber must drop out of the verified set")
	}
}
