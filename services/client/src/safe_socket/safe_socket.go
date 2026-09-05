package safe_socket

import "io"

func SendAll(socket io.Writer, bytes []byte) error {
	total := 0
	for total < len(bytes) {
		n, err := socket.Write(bytes[total:])
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrNoProgress
		}
		total += n
	}
	return nil
}

func RecvAll(socket io.Reader, size int) ([]byte, error) {
	buf := make([]byte, size)
	total := 0
	for total < size {
		n, err := socket.Read(buf[total:])
		if err != nil {
			return nil, err
		}
		if n == 0 {
			return nil, io.ErrNoProgress
		}
		total += n
	}
	return buf, nil
}
