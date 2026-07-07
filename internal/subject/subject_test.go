package subject

import "testing"

func TestSanitize(t *testing.T) {
	tests := []struct{ in, want string }{
		{"belfalas.w.jw4.us:9100", "belfalas-w-jw4-us"},
		{"belfalas:9100", "belfalas"},
		{"david.local:9100", "david-local"},
		{"10.36.2.40:9100", "10-36-2-40"},
		{"nkul:9100", "nkul"},
	}
	for _, tt := range tests {
		if got := Sanitize(tt.in); got != tt.want {
			t.Errorf("Sanitize(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}
