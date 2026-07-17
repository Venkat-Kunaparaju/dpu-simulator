package platform

import (
	"fmt"
	"os"
)

// RequireExecutable returns nil if path exists, is a regular file, and has any
// execute bit set. The error message names the path but not a specific tool.
func RequireExecutable(path string) error {
	st, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("executable not found at %s", path)
		}
		return fmt.Errorf("%s: %w", path, err)
	}
	if st.IsDir() {
		return fmt.Errorf("%s is a directory, not an executable", path)
	}
	if st.Mode()&0o111 == 0 {
		return fmt.Errorf("%s is not executable", path)
	}
	return nil
}
