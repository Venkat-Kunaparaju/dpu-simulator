package kubeburner

import (
	"fmt"
	"strings"
)

// matchExpression is one Kubernetes nodeAffinity matchExpressions entry.
type matchExpression struct {
	Key      string
	Operator string
	Values   []string
}

// SelectorToNodeAffinityYAML converts a kubectl-style label selector into the
// indented matchExpressions block substituted for __NODE_AFFINITY__ in workload
// templates. Supported fragments: key=, key=value, key!=, key!=value (comma-separated).
func SelectorToNodeAffinityYAML(selector string) (string, error) {
	expressions, err := parseSelector(selector)
	if err != nil {
		return "", err
	}
	if len(expressions) == 0 {
		return "", fmt.Errorf("empty node selector")
	}

	var b strings.Builder
	b.WriteString("            nodeSelectorTerms:\n")
	b.WriteString("            - matchExpressions:\n")
	for _, expr := range expressions {
		fmt.Fprintf(&b, "              - key: %s\n", expr.Key)
		fmt.Fprintf(&b, "                operator: %s\n", expr.Operator)
		if len(expr.Values) > 0 {
			b.WriteString("                values:\n")
			for _, v := range expr.Values {
				fmt.Fprintf(&b, "                  - %s\n", v)
			}
		}
	}
	// Trim trailing newline so template replacement does not leave a blank line
	// between affinity and containers when the placeholder sits alone on a line.
	return strings.TrimRight(b.String(), "\n"), nil
}

func parseSelector(selector string) ([]matchExpression, error) {
	var expressions []matchExpression
	for _, raw := range strings.Split(selector, ",") {
		part := strings.TrimSpace(raw)
		if part == "" {
			continue
		}
		if key, val, ok := strings.Cut(part, "!="); ok {
			key = strings.TrimSpace(key)
			val = strings.TrimSpace(val)
			if key == "" {
				return nil, fmt.Errorf("unsupported selector fragment: %s", part)
			}
			if val == "" {
				expressions = append(expressions, matchExpression{Key: key, Operator: "DoesNotExist"})
			} else {
				expressions = append(expressions, matchExpression{Key: key, Operator: "NotIn", Values: []string{val}})
			}
			continue
		}
		if key, val, ok := strings.Cut(part, "="); ok {
			key = strings.TrimSpace(key)
			val = strings.TrimSpace(val)
			if key == "" {
				return nil, fmt.Errorf("unsupported selector fragment: %s", part)
			}
			if val == "" {
				expressions = append(expressions, matchExpression{Key: key, Operator: "Exists"})
			} else {
				expressions = append(expressions, matchExpression{Key: key, Operator: "In", Values: []string{val}})
			}
			continue
		}
		return nil, fmt.Errorf("unsupported selector fragment: %s", part)
	}
	return expressions, nil
}
