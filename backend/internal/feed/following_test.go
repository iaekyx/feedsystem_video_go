package feed

import (
	"context"
	"os"
	"testing"
	"time"

	"feedsystem_video_go/internal/followfeed"
	"feedsystem_video_go/internal/social"
	"feedsystem_video_go/internal/video"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func TestFollowingResponseCursorAndPersonalLikes(t *testing.T) {
	dsn := os.Getenv("TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("isolated MySQL required")
	}
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	sql, _ := db.DB()
	defer sql.Close()
	if err := db.AutoMigrate(&video.Video{}, &video.Like{}, &social.Social{}); err != nil {
		t.Fatal(err)
	}
	user := uint(time.Now().UnixNano() / 1000)
	if err := db.Create(&social.Social{FollowerID: user, VloggerID: user + 1}).Error; err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	videos := []video.Video{
		{AuthorID: user + 1, Title: "old", PlayURL: "test", CoverURL: "test", CreateTime: now},
		{AuthorID: user + 1, Title: "new", PlayURL: "test", CoverURL: "test", CreateTime: now},
	}
	if err := db.Create(&videos).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&video.Like{VideoID: videos[1].ID, AccountID: user}).Error; err != nil {
		t.Fatal(err)
	}
	s := NewFeedService(NewFeedRepository(db), video.NewLikeRepository(db), nil)
	ctx := context.Background()
	first, err := s.ListFollowingCursor(ctx, 1, followfeed.Cursor{}, user)
	if err != nil || len(first.VideoList) != 1 || !first.HasMore || !first.VideoList[0].IsLiked || first.VideoList[0].ID != videos[1].ID {
		t.Fatalf("first page: %+v %v", first, err)
	}
	cursor, err := followfeed.DecodeCursor(first.NextCursor)
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.ListFollowingCursor(ctx, 1, cursor, user)
	if err != nil || len(second.VideoList) != 1 || second.HasMore || second.VideoList[0].IsLiked || second.VideoList[0].ID != videos[0].ID {
		t.Fatalf("second page: %+v %v", second, err)
	}
	// A repeat request re-reads personalized state instead of a stale page cache.
	if err := db.Where("account_id = ?", user).Delete(&video.Like{}).Error; err != nil {
		t.Fatal(err)
	}
	first, err = s.ListFollowingCursor(ctx, 1, followfeed.Cursor{}, user)
	if err != nil || first.VideoList[0].IsLiked {
		t.Fatal("stale liked status", err)
	}
}
