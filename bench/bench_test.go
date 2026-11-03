package bench_test

import "testing"

func BenchmarkPlaceholder(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = i
	}
}
