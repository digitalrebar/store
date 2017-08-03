package store

type ro struct {
	readOnly bool
}

func (r *ro) ReadOnly() bool { return r.readOnly }
func (r *ro) SetReadOnly() bool {
	if r.readOnly {
		return false
	}
	r.readOnly = true
	return true
}
