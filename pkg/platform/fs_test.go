package platform

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRequireExecutable(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	missing := filepath.Join(dir, "missing")
	require.Error(t, RequireExecutable(missing))

	require.Error(t, RequireExecutable(dir))

	nonExec := filepath.Join(dir, "plain")
	require.NoError(t, os.WriteFile(nonExec, []byte("x"), 0o644))
	require.Error(t, RequireExecutable(nonExec))

	execPath := filepath.Join(dir, "tool")
	require.NoError(t, os.WriteFile(execPath, []byte("#!/bin/sh\n"), 0o755))
	require.NoError(t, RequireExecutable(execPath))
}
