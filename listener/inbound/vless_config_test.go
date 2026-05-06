package inbound

import "testing"

func TestVlessXHTTPConfigBuildPropagatesPaddingMethod(t *testing.T) {
	config := XHTTPConfig{
		XPaddingMethod: "tokenish",
	}

	built := config.Build()
	if built.XPaddingMethod != config.XPaddingMethod {
		t.Fatalf("unexpected padding method: got %q want %q", built.XPaddingMethod, config.XPaddingMethod)
	}
}
