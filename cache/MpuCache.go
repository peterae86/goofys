package cache

type PartCache struct {
	PartId      int64
	StartOffset int64
	EndOffset   int64
	dasda       map[int64]string
}
