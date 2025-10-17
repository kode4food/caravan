package closer

// Closer is a value whose underlying resources must be explicitly closed
type Closer interface {
	// Close will instruct the Closer to stop and free its resources
	Close()

	// IsClosed returns a channel that can participate in a select in order
	// to determine whether the Closer has been closed
	IsClosed() <-chan struct{}
}

// IsClosed returns whether the Closer's underlying channel is closed
func IsClosed(c Closer) bool {
	select {
	case <-c.IsClosed():
		return true
	default:
		return false
	}
}
