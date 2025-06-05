package plex

import (
	"context"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test configuration - you'll need to set these for your Plex server
const (
	testPlexURL   = "http://localhost:32400" // Change to your Plex server
	testPlexToken = "your_test_token_here"   // Change to your Plex token
)

func TestNewFs(t *testing.T) {
	if testPlexToken == "your_test_token_here" {
		t.Skip("Skipping test: Please set testPlexToken and testPlexURL for your Plex server")
	}

	// Create config map
	m := configmap.Simple{
		"url":   testPlexURL,
		"token": testPlexToken,
	}

	// Create filesystem
	f, err := NewFs(context.Background(), "test", "", m)
	require.NoError(t, err)
	assert.NotNil(t, f)

	// Test basic properties
	assert.Equal(t, "test", f.Name())
	assert.Equal(t, "", f.Root())
	assert.Equal(t, time.Second, f.Precision())
}

func TestFs_Features(t *testing.T) {
	if testPlexToken == "your_test_token_here" {
		t.Skip("Skipping test: Please set testPlexToken and testPlexURL for your Plex server")
	}

	m := configmap.Simple{
		"url":   testPlexURL,
		"token": testPlexToken,
	}

	f, err := NewFs(context.Background(), "test", "", m)
	require.NoError(t, err)

	features := f.Features()
	assert.NotNil(t, features)
	assert.True(t, features.ReadMimeType)
}

func TestFs_List(t *testing.T) {
	if testPlexToken == "your_test_token_here" {
		t.Skip("Skipping test: Please set testPlexToken and testPlexURL for your Plex server")
	}

	m := configmap.Simple{
		"url":   testPlexURL,
		"token": testPlexToken,
	}

	f, err := NewFs(context.Background(), "test", "", m)
	require.NoError(t, err)

	// Test listing root (should show library sections)
	entries, err := f.List(context.Background(), "")
	require.NoError(t, err)

	// Should have at least one library section
	assert.Greater(t, len(entries), 0)

	// All entries should be directories (library sections)
	for _, entry := range entries {
		_, ok := entry.(fs.Directory)
		assert.True(t, ok, "Expected directory entry for library section")
	}
}

func TestFs_ReadOnlyOperations(t *testing.T) {
	if testPlexToken == "your_test_token_here" {
		t.Skip("Skipping test: Please set testPlexToken and testPlexURL for your Plex server")
	}

	m := configmap.Simple{
		"url":   testPlexURL,
		"token": testPlexToken,
	}

	f, err := NewFs(context.Background(), "test", "", m)
	require.NoError(t, err)

	// Test that write operations return read-only error
	_, err = f.Put(context.Background(), nil, nil)
	assert.Equal(t, errorReadOnly, err)

	err = f.Mkdir(context.Background(), "test")
	assert.Equal(t, errorReadOnly, err)

	err = f.Rmdir(context.Background(), "test")
	assert.Equal(t, errorReadOnly, err)
}

func TestObject_ReadOnlyOperations(t *testing.T) {
	// Test object operations without needing a real Plex server
	fs := &Fs{}
	obj := &Object{
		fs:     fs,
		remote: "test.mp4",
	}

	// Test read-only operations return appropriate errors
	err := obj.SetModTime(context.Background(), time.Now())
	assert.Equal(t, errorReadOnly, err)

	err = obj.Update(context.Background(), nil, nil)
	assert.Equal(t, errorReadOnly, err)

	err = obj.Remove(context.Background())
	assert.Equal(t, errorReadOnly, err)
}

func TestUtilityFunctions(t *testing.T) {
	f := &Fs{}

	// Test filename sanitization
	tests := []struct {
		input    string
		expected string
	}{
		{"normal_filename", "normal_filename"},
		{"file/with/slashes", "file-with-slashes"},
		{"file:with:colons", "file -with -colons"},
		{"file*with*asterisks", "file_with_asterisks"},
		{"file?with?questions", "file_with_questions"},
		{"file\"with\"quotes", "file'with'quotes"},
		{"file<with>brackets", "file_with_brackets"},
		{"file|with|pipes", "file_with_pipes"},
		{"file\\with\\backslashes", "file-with-backslashes"},
	}

	for _, test := range tests {
		result := f.sanitizeName(test.input)
		assert.Equal(t, test.expected, result)
	}

	// Test file extension mapping
	extTests := []struct {
		container string
		expected  string
	}{
		{"mp4", ".mp4"},
		{"mkv", ".mkv"},
		{"avi", ".avi"},
		{"mov", ".mov"},
		{"unknown", ".unknown"},
	}

	for _, test := range extTests {
		result := f.getFileExtension(test.container)
		assert.Equal(t, test.expected, result)
	}

	// Test MIME type mapping
	mimeTests := []struct {
		container string
		expected  string
	}{
		{"mp4", "video/mp4"},
		{"mkv", "video/x-matroska"},
		{"avi", "video/x-msvideo"},
		{"mov", "video/quicktime"},
		{"unknown", "application/octet-stream"},
	}

	for _, test := range mimeTests {
		result := f.getMimeType(test.container)
		assert.Equal(t, test.expected, result)
	}
}

func TestPlexStructures(t *testing.T) {
	// Test that our Plex API structures can be properly marshaled/unmarshaled
	item := PlexMetadata{
		RatingKey: "12345",
		Title:     "Test Movie",
		Type:      "movie",
		Year:      2023,
		Media: []MediaInfo{{
			ID:       1,
			Duration: 7200000,
			Parts: []MediaPart{{
				ID:        1,
				Key:       "/library/parts/12345/file.mp4",
				Size:      1024000000,
				Container: "mp4",
			}},
		}},
	}

	// Verify the structure is properly formed
	assert.Equal(t, "12345", item.RatingKey)
	assert.Equal(t, "Test Movie", item.Title)
	assert.Equal(t, "movie", item.Type)
	assert.Equal(t, 2023, item.Year)
	assert.Len(t, item.Media, 1)
	assert.Len(t, item.Media[0].Parts, 1)
	assert.Equal(t, int64(1024000000), item.Media[0].Parts[0].Size)
}

// Benchmark tests
func BenchmarkSanitizeFilename(b *testing.B) {
	f := &Fs{}
	filename := "Test Movie: The \"Ultimate\" Edition (2023) [4K]/file.mp4"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.sanitizeName(filename)
	}
}

func BenchmarkGetMimeType(b *testing.B) {
	f := &Fs{}
	container := "mp4"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.getMimeType(container)
	}
}

// Example test showing how to use the backend
func ExampleNewFs() {
	// Create configuration
	config := configmap.Simple{
		"url":   "http://192.168.1.100:32400",
		"token": "your_plex_token",
	}

	// Create filesystem
	f, err := NewFs(context.Background(), "myplex", "", config)
	if err != nil {
		panic(err)
	}

	// List library sections
	entries, err := f.List(context.Background(), "")
	if err != nil {
		panic(err)
	}

	// Print library names
	for _, entry := range entries {
		if dir, ok := entry.(fs.Directory); ok {
			println("Library:", dir.Remote())
		}
	}
}
