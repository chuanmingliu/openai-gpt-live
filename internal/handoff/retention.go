package handoff

import "fmt"

const (
	DeletionReasonUserRequested  = "user_requested"
	DeletionReasonPrivacyRequest = "privacy_request"
	DeletionReasonAdminRequested = "admin_requested"
	DeletionReasonTTLExpired     = "ttl_expired"
	DeletionReasonLegacyDeleted  = "legacy_deleted"
)

func ValidateExternalDeletionReason(code string) error {
	switch code {
	case DeletionReasonUserRequested, DeletionReasonPrivacyRequest, DeletionReasonAdminRequested:
		return nil
	default:
		return fmt.Errorf("%w: deletion reason code is invalid", ErrInvalidRequest)
	}
}

func ValidDeletionAuditReason(code string) bool {
	if ValidateExternalDeletionReason(code) == nil {
		return true
	}
	return code == DeletionReasonTTLExpired || code == DeletionReasonLegacyDeleted
}
