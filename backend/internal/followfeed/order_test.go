package followfeed

import (
	"feedsystem_video_go/internal/video"
	"testing"
	"time"
)

func TestCompositeCursorAndMerge(t *testing.T) {
	now := time.Unix(1700000000, 123000000).UTC()
	v := func(id uint, d time.Duration) *video.Video { return &video.Video{ID: id, CreateTime: now.Add(d)} }
	sources := [][]*video.Video{{v(12, 0), v(9, -time.Second)}, {v(13, 0), v(10, 0)}, {v(13, 0), v(11, 0)}}
	got := Merge(sources, 5)
	want := []uint{13, 12, 11, 10, 9}
	if len(got) != len(want) {
		t.Fatalf("got %v", got)
	}
	for i, id := range want {
		if got[i].ID != id {
			t.Fatalf("position %d: %d != %d", i, got[i].ID, id)
		}
		c, err := DecodeCursor(EncodeCursor(got[i]))
		if err != nil || c.ID != id || !c.Time.Equal(got[i].CreateTime) {
			t.Fatalf("cursor round trip: %+v %v", c, err)
		}
	}
	if !(member(now, 13) > member(now, 12)) {
		t.Fatal("lexical ID ordering")
	}
	for _, bad := range []string{"not-a-cursor", "eA", "!!!!!!!!!!!!!!!!"} {
		if _, err := DecodeCursor(bad); err == nil {
			t.Fatal("accepted invalid cursor", bad)
		}
	}
}

func TestDefaultThreshold(t *testing.T) {
	t.Setenv("FEED_CELEBRITY_THRESHOLD", "2")
	if DefaultOptions().CelebrityThreshold != 2 {
		t.Fatal("threshold override ignored")
	}
}
