package kvraft

type KVStore interface {
	Get(key string) (string, Err)
	Put(key, value string) Err
	Append(key, value string) Err
}

type SimpleKV struct {
	KVMp map[string]string
}

func Makekv() *SimpleKV {
	return &SimpleKV{KVMp: make(map[string]string)}
}

func (skv *SimpleKV) Get(key string) (string, Err) {
	if val, ok := skv.KVMp[key]; ok {
		return val, OK
	}
	return "", ErrNoKey

}
func (skv *SimpleKV) Put(key, value string) Err {
	skv.KVMp[key] = value
	return OK
}
func (skv *SimpleKV) Append(key, value string) Err {
	skv.KVMp[key] += value
	return OK
}
