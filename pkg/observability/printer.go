package observability

import "fmt"

const Separator = "\n\n========================================"

func PrintStep(step, service, action string, fields ...string) {
	fmt.Println(Separator)
	fmt.Printf("  STEP     : %s\n", step)
	fmt.Printf("  SERVICE  : %s\n", service)
	fmt.Printf("  ACTION   : %s\n", action)
	for i := 0; i < len(fields)-1; i += 2 {
		fmt.Printf("  %-9s: %s\n", fields[i], fields[i+1])
	}
	fmt.Println("========================================")
}
