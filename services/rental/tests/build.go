//go:build functional

package tests

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func BuildRental(t *testing.T) string {
	t.Helper()
	return buildService(t, rentalModule(t), "./cmd/rental")
}

func BuildPayment(t *testing.T) string {
	t.Helper()
	return buildService(t, filepath.Join(rentalModule(t), "..", "payment"), "./cmd")
}

func rentalModule(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("Rental go.mod not found")
		}
		dir = parent
	}
}

func buildService(t *testing.T, moduleDir, entrypoint string) string {
	t.Helper()
	binary := filepath.Join(t.TempDir(), "service")
	cmd := exec.CommandContext(t.Context(), "go", "build", "-buildvcs=false", "-race", "-o", binary, entrypoint)
	cmd.Dir = moduleDir
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build %s: %v\n%s", entrypoint, err, output)
	}
	return binary
}
