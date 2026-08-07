package safety

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"unicode/utf8"
)

const (
	minimumProviderSafetyKeyBytes = 32
	maximumProviderIdentityBytes  = 512
)

func ProviderSafetyID(key []byte, tenantID, userID string) (string, error) {
	if len(key) < minimumProviderSafetyKeyBytes {
		return "", fmt.Errorf("provider safety key is too short")
	}
	if err := validateProviderIdentity(tenantID); err != nil {
		return "", fmt.Errorf("tenant identity: %w", err)
	}
	if err := validateProviderIdentity(userID); err != nil {
		return "", fmt.Errorf("user identity: %w", err)
	}
	ownedKey := append([]byte(nil), key...)
	defer clear(ownedKey)
	mac := hmac.New(sha256.New, ownedKey)
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(tenantID)))
	_, _ = mac.Write(length[:])
	_, _ = mac.Write([]byte(tenantID))
	binary.BigEndian.PutUint64(length[:], uint64(len(userID)))
	_, _ = mac.Write(length[:])
	_, _ = mac.Write([]byte(userID))
	digest := mac.Sum(nil)
	defer clear(digest)
	return base64.RawURLEncoding.EncodeToString(digest), nil
}

func validateProviderIdentity(value string) error {
	if value == "" || len(value) > maximumProviderIdentityBytes || !utf8.ValidString(value) {
		return fmt.Errorf("must be nonempty valid UTF-8 within bounds")
	}
	return nil
}
