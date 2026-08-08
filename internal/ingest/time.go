package ingest

import "time"

var timeLayouts = []string{
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02T15:04:05.000Z07:00",
	"2006-01-02 15:04:05",
}

func parseLayout(layout, s string) (int64, error) {
	t, err := time.Parse(layout, s)
	if err != nil {
		return 0, err
	}
	return t.UnixNano(), nil
}
