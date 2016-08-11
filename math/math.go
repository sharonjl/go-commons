package math

func IntRoundTo(n, r int) int {
	m := n % r
	if m == 0 {
		return n
	}
	return (n + r) - m
}

func UintRoundTo(n, r uint) uint {
	m := n % r
	if m == 0 {
		return n
	}
	return (n + r) - m
}