package handoff

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExternalDeletionReasonCodesAreStableAndContentFree(t *testing.T) {
	for _, code := range []string{DeletionReasonUserRequested, DeletionReasonPrivacyRequest, DeletionReasonAdminRequested} {
		assert.NoError(t, ValidateExternalDeletionReason(code))
	}
	for _, code := range []string{"", "contains transcript text", "password=secret", DeletionReasonTTLExpired, DeletionReasonLegacyDeleted} {
		assert.Error(t, ValidateExternalDeletionReason(code))
	}
}
