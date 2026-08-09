package loadgateway

import (
	"testing"
	"time"
)

func TestLoadIdentityOutlivesMaximumFormalSoak(t *testing.T) {
	const maximumFormalSoak = 2 * time.Hour
	if loadIdentityLifetime <= maximumFormalSoak {
		t.Fatalf("load identity lifetime %s does not outlive formal soak %s", loadIdentityLifetime, maximumFormalSoak)
	}
}
