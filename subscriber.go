package pharos

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"time"
)

// ErrAlreadySubscribed is returned by Subscribe when the email is already registered.
var ErrAlreadySubscribed = errors.New("pharos: email already subscribed")

// SubscriberService manages status-page email subscriptions and owns the crypto seam: the email is
// encrypted at rest (injected Encryptor) with a separate deterministic hash for uniqueness, and the
// verify/unsubscribe tokens are stored only as hashes. Plaintext email and raw tokens never reach the
// store. It is the writer for the subscriber columns the store holds (encrypted email, hashes),
// completing the subscribe capability.
type SubscriberService struct {
	store    StatusStore
	enc      Encryptor
	now      func() time.Time
	newToken func() string
}

// SubscriberConfig wires a SubscriberService. Store and Encryptor are required — subscriber PII must
// be encrypted at rest.
type SubscriberConfig struct {
	Store     StatusStore
	Encryptor Encryptor
	Now       func() time.Time
	// NewToken generates a raw (unhashed) token; default is 32 random bytes hex. Injectable for tests.
	NewToken func() string
}

// NewSubscriberService validates the config and returns the service.
func NewSubscriberService(c SubscriberConfig) (*SubscriberService, error) {
	if c.Store == nil {
		return nil, &configError{"pharos: SubscriberConfig.Store is required"}
	}
	if c.Encryptor == nil {
		return nil, &configError{"pharos: SubscriberConfig.Encryptor is required (subscriber email must be encrypted at rest)"}
	}
	s := &SubscriberService{store: c.Store, enc: c.Encryptor, now: c.Now, newToken: c.NewToken}
	if s.now == nil {
		s.now = time.Now
	}
	if s.newToken == nil {
		s.newToken = randomToken
	}
	return s, nil
}

func randomToken() string {
	b := make([]byte, 32)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// hashHex is the deterministic token/email hash used for storage + lookup.
func hashHex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// normalizeEmail lower-cases and trims so the uniqueness hash is stable across casings.
func normalizeEmail(email string) string { return strings.ToLower(strings.TrimSpace(email)) }

// Subscribe registers an email: it encrypts the address, hashes it for uniqueness, mints verify +
// unsubscribe tokens (stored only as hashes), and inserts the subscriber. It returns the RAW tokens
// so the caller can send a confirmation link and an unsubscribe link — they are never recoverable
// from the store afterward. A duplicate email returns ErrAlreadySubscribed without inserting.
func (s *SubscriberService) Subscribe(ctx context.Context, email string) (verifyToken, unsubToken string, err error) {
	email = normalizeEmail(email)
	if email == "" {
		return "", "", errors.New("pharos: empty email")
	}
	emailHash := hashHex(email)
	exists, err := s.store.SubscriberExistsByEmailHash(ctx, emailHash)
	if err != nil {
		return "", "", err
	}
	if exists {
		return "", "", ErrAlreadySubscribed
	}
	ciphertext, err := s.enc.Encrypt(email)
	if err != nil {
		return "", "", err
	}
	verifyToken = s.newToken()
	unsubToken = s.newToken()
	if err := s.store.InsertSubscriber(ctx, &Subscriber{
		EmailEncrypted:  ciphertext,
		EmailHash:       emailHash,
		VerifyTokenHash: hashHex(verifyToken),
		UnsubTokenHash:  hashHex(unsubToken),
		Verified:        false,
		CreatedAt:       s.now().Unix(),
	}); err != nil {
		return "", "", err
	}
	return verifyToken, unsubToken, nil
}

// Verify confirms a subscription from the raw verify token (hashed before lookup).
func (s *SubscriberService) Verify(ctx context.Context, rawVerifyToken string) error {
	return s.store.VerifySubscriber(ctx, hashHex(rawVerifyToken))
}

// Unsubscribe removes a subscription from the raw unsubscribe token (hashed before lookup).
func (s *SubscriberService) Unsubscribe(ctx context.Context, rawUnsubToken string) error {
	return s.store.UnsubscribeByToken(ctx, hashHex(rawUnsubToken))
}
