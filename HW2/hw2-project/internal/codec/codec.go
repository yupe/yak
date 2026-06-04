package codec

import (
	"encoding/json"
	"fmt"
)

type JSON[T any] struct{}

func (JSON[T]) Encode(value interface{}) ([]byte, error) {
	v, ok := value.(T)
	if !ok {
		return nil, fmt.Errorf("illegal type: %T", value)
	}
	return json.Marshal(v)
}

func (JSON[T]) Decode(data []byte) (interface{}, error) {
	var v T
	if err := json.Unmarshal(data, &v); err != nil {
		return nil, err
	}
	return v, nil
}
