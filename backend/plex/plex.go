// Package plex provides a filesystem interface to Plex Media Server
//
// This backend allows rclone to treat a Plex Media Server as a read-only
// remote filesystem, enabling operations like copy and mount.
package plex

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/config/obscure"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/rclone/rclone/lib/rest"
)

var (
	errorReadOnly = errors.New("plex backend is read only")
)

// Register with Fs
func init() {
	fs.Register(&fs.RegInfo{
		Name:        "plex",
		Description: "Plex Media Server",
		NewFs:       NewFs,
		Config:      Config,
		Options: []fs.Option{{
			Name:     "username",
			Help:     "Plex username (email).\n\nUsed to authenticate with Plex.tv and discover available servers.",
			Required: false,
		}, {
			Name:       "password",
			Help:       "Plex password.\n\nUsed with username to authenticate with Plex.tv.",
			IsPassword: true,
			Required:   false,
		}, {
			Name:     "url",
			Help:     "URL of Plex server.\n\nE.g. https://192.168.1.100:32400 or http://plex.example.com:32400\nLeave empty to select from discovered servers during config.",
			Required: false,
		}, {
			Name:       "token",
			Help:       "Plex authentication token.\n\nGet this from Plex Web interface: Settings > Account > Privacy > Show\nOr use username/password authentication.",
			IsPassword: true,
			Required:   false,
		}, {
			Name:     "insecure_skip_verify",
			Help:     "Skip verification of SSL certificates.\n\nUse this if your Plex server uses self-signed certificates.",
			Default:  false,
			Advanced: true,
		}, {
			Name:     "library_sections",
			Help:     "Comma separated list of library section IDs to include.\n\nLeave empty to include all libraries. Use 'rclone lsd plex:' to see available sections.",
			Default:  "",
			Advanced: true,
		}},
	})
}

// Config function for setting up the Plex backend during rclone config
func Config(ctx context.Context, name string, m configmap.Mapper, in fs.ConfigIn) (*fs.ConfigOut, error) {
	// Check if we have username and password for Plex.tv authentication
	username, _ := m.Get("username")
	password, _ := m.Get("password")
	existingURL, _ := m.Get("url")
	existingToken, _ := m.Get("token")

	// If we already have URL and token, or if no username/password provided, skip configuration
	if (existingURL != "" && existingToken != "") || (username == "" || password == "") {
		return nil, nil
	}

	// State machine for configuration
	switch in.State {
	case "":
		// First state: authenticate and get servers
		// Decrypt the password if it's been obscured
		if password != "" {
			if decrypted, err := obscure.Reveal(password); err == nil {
				password = decrypted
			}
		}

		// Authenticate with Plex.tv
		user, err := authenticateWithPlexTV(ctx, username, password)
		if err != nil {
			return &fs.ConfigOut{
				Error: fmt.Sprintf("Authentication with Plex.tv failed: %v", err),
			}, nil
		}

		// Discover available servers
		servers, err := discoverPlexServers(ctx, user.User.AuthToken)
		if err != nil {
			return &fs.ConfigOut{
				Error: fmt.Sprintf("Server discovery failed: %v", err),
			}, nil
		}

		if len(servers) == 0 {
			return &fs.ConfigOut{
				Error: fmt.Sprintf("No Plex servers found for user %s", username),
			}, nil
		}

		// Store the auth token for next state
		m.Set("config_auth_token", user.User.AuthToken)

		// If only one server, use it automatically
		if len(servers) == 1 {
			selectedURL := getBestConnection(&servers[0])
			if selectedURL == "" {
				return &fs.ConfigOut{
					Error: fmt.Sprintf("No usable connection found for server %s", servers[0].Name),
				}, nil
			}

			ownershipStatus := "shared"
			if servers[0].Owned {
				ownershipStatus = "owned"
			}

			// Set the URL and server-specific token directly
			m.Set("url", selectedURL)
			m.Set("token", obscure.MustObscure(servers[0].AccessToken))

			return &fs.ConfigOut{
				Result: fmt.Sprintf("Using single available server: %s at %s [%s]", servers[0].Name, selectedURL, ownershipStatus),
			}, nil
		}

		// Multiple servers: ask user to choose
		var choices []string
		var urls []string
		for _, server := range servers {
			bestConn := getBestConnection(&server)
			ownershipStatus := "shared"
			if server.Owned {
				ownershipStatus = "owned"
			}

			if bestConn != "" {
				choices = append(choices, fmt.Sprintf("%s (%s) [%s]", server.Name, bestConn, ownershipStatus))
				urls = append(urls, bestConn)
			} else {
				choices = append(choices, fmt.Sprintf("%s (no connections available) [%s]", server.Name, ownershipStatus))
				urls = append(urls, "")
			}
		}

		// Store the URLs and access tokens for the next state
		for i, url := range urls {
			m.Set(fmt.Sprintf("config_server_url_%d", i), url)
			m.Set(fmt.Sprintf("config_server_token_%d", i), servers[i].AccessToken)
		}

		return &fs.ConfigOut{
			State: "choose_server",
			Option: &fs.Option{
				Name:     "config_server_choice",
				Help:     "Select which Plex server to use",
				Required: true,
				Examples: func() []fs.OptionExample {
					var examples []fs.OptionExample
					for i, choice := range choices {
						examples = append(examples, fs.OptionExample{
							Value: fmt.Sprintf("%d", i),
							Help:  choice,
						})
					}
					return examples
				}(),
			},
		}, nil

	case "choose_server":
		// Parse the choice
		choiceStr := in.Result
		choice := 0
		if choiceStr != "" {
			var err error
			choice, err = strconv.Atoi(choiceStr)
			if err != nil {
				return &fs.ConfigOut{
					Error: "Invalid choice: must be a number",
					State: "choose_server",
				}, nil
			}
		}

		// Get the URL and token for the chosen server
		selectedURL, _ := m.Get(fmt.Sprintf("config_server_url_%d", choice))
		if selectedURL == "" {
			return &fs.ConfigOut{
				Error: "Selected server has no usable connections",
				State: "choose_server",
			}, nil
		}

		// Get the server-specific access token
		serverToken, _ := m.Get(fmt.Sprintf("config_server_token_%d", choice))
		if serverToken == "" {
			return &fs.ConfigOut{
				Error: "No access token found for selected server",
				State: "choose_server",
			}, nil
		}

		// Set the final configuration with server-specific token
		m.Set("url", selectedURL)
		m.Set("token", obscure.MustObscure(serverToken))

		// Clean up temporary config
		m.Set("config_auth_token", "")
		for i := 0; i < 10; i++ { // clean up to 10 server URLs and tokens
			m.Set(fmt.Sprintf("config_server_url_%d", i), "")
			m.Set(fmt.Sprintf("config_server_token_%d", i), "")
		}

		return &fs.ConfigOut{
			Result: fmt.Sprintf("Configuration complete. Using server at %s", selectedURL),
		}, nil

	default:
		return &fs.ConfigOut{
			Error: "Unknown configuration state",
		}, nil
	}
}

// Plex API Response Structures
type MediaContainer struct {
	XMLName     xml.Name       `xml:"MediaContainer" json:"-"`
	Size        int            `xml:"size,attr" json:"size"`
	Title1      string         `xml:"title1,attr" json:"title1"`
	Directories []Directory    `xml:"Directory" json:"Directory"`
	Videos      []Video        `xml:"Video" json:"Video"`
	Metadata    []PlexMetadata `xml:"Metadata" json:"Metadata"`
}

type Directory struct {
	Key             string     `xml:"key,attr" json:"key"`
	Title           string     `xml:"title,attr" json:"title"`
	Type            string     `xml:"type,attr" json:"type"`
	UpdatedAt       int64      `xml:"updatedAt,attr" json:"updatedAt"`
	CreatedAt       int64      `xml:"createdAt,attr" json:"createdAt"`
	Year            int        `xml:"year,attr" json:"year"`
	RatingKey       string     `xml:"ratingKey,attr" json:"ratingKey"`
	ParentRatingKey string     `xml:"parentRatingKey,attr" json:"parentRatingKey"`
	LeafCount       int        `xml:"leafCount,attr" json:"leafCount"`
	ViewedLeafCount int        `xml:"viewedLeafCount,attr" json:"viewedLeafCount"`
	Index           int        `xml:"index,attr" json:"index"`
	Locations       []Location `xml:"Location" json:"Location"`
}

type Location struct {
	ID   int    `xml:"id,attr" json:"id"`
	Path string `xml:"path,attr" json:"path"`
}

type PlexMetadata struct {
	RatingKey             string      `xml:"ratingKey,attr" json:"ratingKey"`
	Key                   string      `xml:"key,attr" json:"key"`
	Type                  string      `xml:"type,attr" json:"type"`
	Title                 string      `xml:"title,attr" json:"title"`
	ParentKey             string      `xml:"parentKey,attr" json:"parentKey"`
	ParentRatingKey       string      `xml:"parentRatingKey,attr" json:"parentRatingKey"`
	GrandparentKey        string      `xml:"grandparentKey,attr" json:"grandparentKey"`
	GrandparentRatingKey  string      `xml:"grandparentRatingKey,attr" json:"grandparentRatingKey"`
	GrandparentTitle      string      `xml:"grandparentTitle,attr" json:"grandparentTitle"`
	ParentTitle           string      `xml:"parentTitle,attr" json:"parentTitle"`
	ParentIndex           int         `xml:"parentIndex,attr" json:"parentIndex"`
	Index                 int         `xml:"index,attr" json:"index"`
	Year                  int         `xml:"year,attr" json:"year"`
	OriginallyAvailableAt string      `xml:"originallyAvailableAt,attr" json:"originallyAvailableAt"`
	Duration              int64       `xml:"duration,attr" json:"duration"`
	AddedAt               int64       `xml:"addedAt,attr" json:"addedAt"`
	UpdatedAt             int64       `xml:"updatedAt,attr" json:"updatedAt"`
	Media                 []MediaInfo `xml:"Media" json:"Media"`
	Guid                  []PlexGuid  `xml:"Guid" json:"Guid"`
}

type PlexGuid struct {
	ID string `xml:"id,attr" json:"id"`
}

type Video struct {
	RatingKey            string      `xml:"ratingKey,attr" json:"ratingKey"`
	Key                  string      `xml:"key,attr" json:"key"`
	Type                 string      `xml:"type,attr" json:"type"`
	Title                string      `xml:"title,attr" json:"title"`
	ParentKey            string      `xml:"parentKey,attr" json:"parentKey"`
	ParentRatingKey      string      `xml:"parentRatingKey,attr" json:"parentRatingKey"`
	GrandparentKey       string      `xml:"grandparentKey,attr" json:"grandparentKey"`
	GrandparentRatingKey string      `xml:"grandparentRatingKey,attr" json:"grandparentRatingKey"`
	GrandparentTitle     string      `xml:"grandparentTitle,attr" json:"grandparentTitle"`
	ParentTitle          string      `xml:"parentTitle,attr" json:"parentTitle"`
	ParentIndex          int         `xml:"parentIndex,attr" json:"parentIndex"`
	Index                int         `xml:"index,attr" json:"index"`
	Year                 int         `xml:"year,attr" json:"year"`
	Duration             int64       `xml:"duration,attr" json:"duration"`
	AddedAt              int64       `xml:"addedAt,attr" json:"addedAt"`
	UpdatedAt            int64       `xml:"updatedAt,attr" json:"updatedAt"`
	Media                []MediaInfo `xml:"Media" json:"Media"`
	Guid                 []PlexGuid  `xml:"Guid" json:"Guid"`
}

type MediaInfo struct {
	ID                    int         `xml:"id,attr" json:"id"`
	Duration              int64       `xml:"duration,attr" json:"duration"`
	Bitrate               int         `xml:"bitrate,attr" json:"bitrate"`
	Width                 int         `xml:"width,attr" json:"width"`
	Height                int         `xml:"height,attr" json:"height"`
	AspectRatio           string      `xml:"aspectRatio,attr" json:"aspectRatio"`
	AudioChannels         int         `xml:"audioChannels,attr" json:"audioChannels"`
	AudioCodec            string      `xml:"audioCodec,attr" json:"audioCodec"`
	VideoCodec            string      `xml:"videoCodec,attr" json:"videoCodec"`
	VideoResolution       string      `xml:"videoResolution,attr" json:"videoResolution"`
	Container             string      `xml:"container,attr" json:"container"`
	VideoFrameRate        string      `xml:"videoFrameRate,attr" json:"videoFrameRate"`
	OptimizedForStreaming bool        `xml:"optimizedForStreaming,attr" json:"optimizedForStreaming"`
	Parts                 []MediaPart `xml:"Part" json:"Part"`
}

type MediaPart struct {
	ID                    int    `xml:"id,attr" json:"id"`
	Key                   string `xml:"key,attr" json:"key"`
	Duration              int64  `xml:"duration,attr" json:"duration"`
	File                  string `xml:"file,attr" json:"file"`
	Size                  int64  `xml:"size,attr" json:"size"`
	Container             string `xml:"container,attr" json:"container"`
	Has64bitOffsets       bool   `xml:"has64bitOffsets,attr" json:"has64bitOffsets"`
	OptimizedForStreaming bool   `xml:"optimizedForStreaming,attr" json:"optimizedForStreaming"`
}

// Plex.tv Authentication Structures
type PlexUser struct {
	User struct {
		ID            int    `json:"id"`
		UUID          string `json:"uuid"`
		Email         string `json:"email"`
		JoinedAt      string `json:"joined_at"`
		Username      string `json:"username"`
		Title         string `json:"title"`
		Thumb         string `json:"thumb"`
		HasPassword   bool   `json:"hasPassword"`
		AuthToken     string `json:"authToken"`
		AuthTokenHash string `json:"authenticationToken"`
	} `json:"user"`
}

type PlexResource struct {
	Name                   string           `xml:"name,attr" json:"name"`
	Product                string           `xml:"product,attr" json:"product"`
	ProductVersion         string           `xml:"productVersion,attr" json:"productVersion"`
	Platform               string           `xml:"platform,attr" json:"platform"`
	PlatformVersion        string           `xml:"platformVersion,attr" json:"platformVersion"`
	Device                 string           `xml:"device,attr" json:"device"`
	ClientIdentifier       string           `xml:"clientIdentifier,attr" json:"clientIdentifier"`
	CreatedAt              string           `xml:"createdAt,attr" json:"createdAt"`
	LastSeenAt             string           `xml:"lastSeenAt,attr" json:"lastSeenAt"`
	Provides               string           `xml:"provides,attr" json:"provides"`
	Owned                  bool             `xml:"owned,attr" json:"owned"`
	AccessToken            string           `xml:"accessToken,attr" json:"accessToken"`
	PublicAddress          string           `xml:"publicAddress,attr" json:"publicAddress"`
	HTTPSRequired          bool             `xml:"httpsRequired,attr" json:"httpsRequired"`
	Synced                 bool             `xml:"synced,attr" json:"synced"`
	Relay                  bool             `xml:"relay,attr" json:"relay"`
	DNSRebindingProtection bool             `xml:"dnsRebindingProtection,attr" json:"dnsRebindingProtection"`
	NatLoopbackSupported   bool             `xml:"natLoopbackSupported,attr" json:"natLoopbackSupported"`
	PublicAddressMatches   bool             `xml:"publicAddressMatches,attr" json:"publicAddressMatches"`
	Presence               bool             `xml:"presence,attr" json:"presence"`
	Connections            []PlexConnection `xml:"Connection" json:"connections"`
}

type PlexConnection struct {
	Protocol string `xml:"protocol,attr" json:"protocol"`
	Address  string `xml:"address,attr" json:"address"`
	Port     int    `xml:"port,attr" json:"port"`
	URI      string `xml:"uri,attr" json:"uri"`
	Local    bool   `xml:"local,attr" json:"local"`
	Relay    bool   `xml:"relay,attr" json:"relay"`
	IPv6     bool   `xml:"IPv6,attr" json:"IPv6"`
}

// Options defines the configuration for this backend
type Options struct {
	Username           string `config:"username"`
	Password           string `config:"password"`
	URL                string `config:"url"`
	Token              string `config:"token"`
	InsecureSkipVerify bool   `config:"insecure_skip_verify"`
	LibrarySections    string `config:"library_sections"`
}

// PlexRecentItem represents an item from recently added/newest endpoints
type PlexRecentItem struct {
	RatingKey    string `xml:"ratingKey,attr" json:"ratingKey"`
	Type         string `xml:"type,attr" json:"type"`
	Title        string `xml:"title,attr" json:"title"`
	AddedAt      int64  `xml:"addedAt,attr" json:"addedAt"`
	UpdatedAt    int64  `xml:"updatedAt,attr" json:"updatedAt"`
	ParentKey    string `xml:"parentKey,attr" json:"parentKey"`
	GrandparentKey string `xml:"grandparentKey,attr" json:"grandparentKey"`
	ParentTitle  string `xml:"parentTitle,attr" json:"parentTitle"`
	GrandparentTitle string `xml:"grandparentTitle,attr" json:"grandparentTitle"`
}

// PlexPollState tracks the state for efficient polling
type PlexPollState struct {
	LastRecentlyAddedAt int64 // Latest addedAt timestamp we've seen
	LastNewestUpdatedAt int64 // Latest updatedAt timestamp we've seen
	KnownItems         map[string]int64 // Map of ratingKey -> timestamp for tracking changes
}

// PlexDirectory represents a directory in the Plex filesystem with metadata support
type PlexDirectory struct {
	fs        *Fs
	name      string
	modTime   time.Time
	ratingKey string
	plexType  string // "library", "show", "season", "movie"
	metadata  fs.Metadata
}

// Fs returns the parent Fs
func (d *PlexDirectory) Fs() fs.Info {
	return d.fs
}

// Name returns the name of the directory
func (d *PlexDirectory) Name() string {
	return d.name
}

// Size returns the size, which is always 0 for directories
func (d *PlexDirectory) Size() int64 {
	return 0
}

// ModTime returns the modification time
func (d *PlexDirectory) ModTime(ctx context.Context) time.Time {
	return d.modTime
}

// IsDir returns true as this is always a directory
func (d *PlexDirectory) IsDir() bool {
	return true
}

// Metadata returns the metadata for this directory
func (d *PlexDirectory) Metadata(ctx context.Context) (fs.Metadata, error) {
	if d.metadata == nil {
		d.metadata = make(fs.Metadata)
		d.metadata["plex-rating-key"] = d.ratingKey
		d.metadata["plex-type"] = d.plexType
		d.metadata["plex-title"] = d.name
		d.metadata["mtime"] = d.modTime.Format(time.RFC3339Nano)
	}
	return d.metadata, nil
}

// SetMetadata sets metadata for this directory (not supported in read-only backend)
func (d *PlexDirectory) SetMetadata(ctx context.Context, metadata fs.Metadata) error {
	return errorReadOnly
}

// String returns a description of the directory
func (d *PlexDirectory) String() string {
	return d.name
}

// Remote returns the remote path of the directory
func (d *PlexDirectory) Remote() string {
	return d.name
}

// Items returns the count of items (unknown for Plex directories)
func (d *PlexDirectory) Items() int64 {
	return -1
}

// ID returns the directory's ID if known
func (d *PlexDirectory) ID() string {
	return d.ratingKey
}

// authenticateWithPlexTV authenticates with Plex.tv using username and password
func authenticateWithPlexTV(ctx context.Context, username, password string) (*PlexUser, error) {
	client := fshttp.NewClient(ctx)

	// Prepare the authentication request
	data := url.Values{}
	data.Set("user[login]", username)
	data.Set("user[password]", password)

	req, err := http.NewRequestWithContext(ctx, "POST", "https://plex.tv/users/sign_in.json", strings.NewReader(data.Encode()))
	if err != nil {
		return nil, fmt.Errorf("failed to create authentication request: %w", err)
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("X-Plex-Client-Identifier", "rclone-plex-backend")
	req.Header.Set("X-Plex-Product", "rclone")
	req.Header.Set("X-Plex-Version", "1.0")
	req.Header.Set("X-Plex-Platform", "Go")
	req.Header.Set("X-Plex-Platform-Version", "1.0")
	req.Header.Set("X-Plex-Device", "rclone")
	req.Header.Set("X-Plex-Device-Name", "rclone")

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("authentication request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("authentication failed with status %d: %s", resp.StatusCode, string(body))
	}

	var user PlexUser
	if err := json.NewDecoder(resp.Body).Decode(&user); err != nil {
		return nil, fmt.Errorf("failed to decode user response: %w", err)
	}

	if user.User.AuthToken == "" && user.User.AuthTokenHash == "" {
		return nil, fmt.Errorf("no authentication token received")
	}

	// Use AuthToken if available, otherwise use AuthTokenHash
	if user.User.AuthToken == "" {
		user.User.AuthToken = user.User.AuthTokenHash
	}

	return &user, nil
}

// PlexResourcesContainer for XML response from Plex.tv API
type PlexResourcesContainer struct {
	XMLName   xml.Name       `xml:"MediaContainer"`
	Resources []PlexResource `xml:"Device"`
}

// discoverPlexServers discovers available Plex servers for a user
func discoverPlexServers(ctx context.Context, authToken string) ([]PlexResource, error) {
	client := fshttp.NewClient(ctx)

	req, err := http.NewRequestWithContext(ctx, "GET", "https://plex.tv/api/resources?includeHttps=1&includeRelay=1", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create server discovery request: %w", err)
	}

	req.Header.Set("X-Plex-Token", authToken)
	req.Header.Set("X-Plex-Client-Identifier", "rclone-plex-backend")
	req.Header.Set("Accept", "application/xml")

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("server discovery request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("server discovery failed with status %d: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	var container PlexResourcesContainer
	if err := xml.Unmarshal(body, &container); err != nil {
		return nil, fmt.Errorf("failed to decode resources response: %w", err)
	}

	// Filter for servers only (not clients) - include both owned and shared servers
	var servers []PlexResource
	for _, resource := range container.Resources {
		if strings.Contains(resource.Provides, "server") {
			servers = append(servers, resource)
		}
	}

	return servers, nil
}

// getBestConnection returns the best connection URI for a server
func getBestConnection(server *PlexResource) string {
	if len(server.Connections) == 0 {
		return ""
	}

	var bestConnection *PlexConnection
	
	// First pass: look for clean public URLs (not plex.direct)
	for _, conn := range server.Connections {
		// Skip local connections and plex.direct URLs in first pass
		if conn.Local || strings.Contains(conn.URI, ".plex.direct") {
			continue
		}
		
		if bestConnection == nil {
			bestConnection = &conn
			continue
		}
		
		// Prefer HTTPS over HTTP
		if conn.Protocol == "https" && bestConnection.Protocol != "https" {
			bestConnection = &conn
		}
	}
	
	// If we found a good public URL, use it
	if bestConnection != nil {
		return bestConnection.URI
	}
	
	// Second pass: if no clean public URLs, fall back to any remote connection
	for _, conn := range server.Connections {
		// Skip local connections in second pass
		if conn.Local {
			continue
		}
		
		if bestConnection == nil {
			bestConnection = &conn
			continue
		}
		
		// Prefer HTTPS over HTTP
		if conn.Protocol == "https" && bestConnection.Protocol != "https" {
			bestConnection = &conn
		}
	}
	
	// If we found any remote connection, use it
	if bestConnection != nil {
		return bestConnection.URI
	}
	
	// Final fallback: use any connection (including local)
	bestConnection = &server.Connections[0]
	for _, conn := range server.Connections {
		// Prefer HTTPS over HTTP even for local connections
		if conn.Protocol == "https" && bestConnection.Protocol != "https" {
			bestConnection = &conn
		}
	}

	return bestConnection.URI
}

// getRecentlyAdded gets recently added items from a library section
func (f *Fs) getRecentlyAdded(ctx context.Context, sectionID string) ([]PlexRecentItem, error) {
	endpoint := fmt.Sprintf("/library/sections/%s/recentlyAdded", sectionID)
	container, err := f.makeAPICall(ctx, endpoint)
	if err != nil {
		return nil, err
	}

	var items []PlexRecentItem
	// Process both Videos and Directories (shows/seasons)
	for _, video := range container.Videos {
		items = append(items, PlexRecentItem{
			RatingKey:        video.RatingKey,
			Type:             video.Type,
			Title:            video.Title,
			AddedAt:          video.AddedAt,
			UpdatedAt:        video.UpdatedAt,
			ParentKey:        video.ParentKey,
			GrandparentKey:   video.GrandparentKey,
			ParentTitle:      video.ParentTitle,
			GrandparentTitle: video.GrandparentTitle,
		})
	}
	
	for _, metadata := range container.Metadata {
		items = append(items, PlexRecentItem{
			RatingKey:        metadata.RatingKey,
			Type:             metadata.Type,
			Title:            metadata.Title,
			AddedAt:          metadata.AddedAt,
			UpdatedAt:        metadata.UpdatedAt,
			ParentKey:        metadata.ParentKey,
			GrandparentKey:   metadata.GrandparentKey,
			ParentTitle:      metadata.ParentTitle,
			GrandparentTitle: metadata.GrandparentTitle,
		})
	}

	return items, nil
}

// getNewest gets newest items from a library section
func (f *Fs) getNewest(ctx context.Context, sectionID string) ([]PlexRecentItem, error) {
	endpoint := fmt.Sprintf("/library/sections/%s/newest", sectionID)
	container, err := f.makeAPICall(ctx, endpoint)
	if err != nil {
		return nil, err
	}

	var items []PlexRecentItem
	// Process both Videos and Directories
	for _, video := range container.Videos {
		items = append(items, PlexRecentItem{
			RatingKey:        video.RatingKey,
			Type:             video.Type,
			Title:            video.Title,
			AddedAt:          video.AddedAt,
			UpdatedAt:        video.UpdatedAt,
			ParentKey:        video.ParentKey,
			GrandparentKey:   video.GrandparentKey,
			ParentTitle:      video.ParentTitle,
			GrandparentTitle: video.GrandparentTitle,
		})
	}
	
	for _, metadata := range container.Metadata {
		items = append(items, PlexRecentItem{
			RatingKey:        metadata.RatingKey,
			Type:             metadata.Type,
			Title:            metadata.Title,
			AddedAt:          metadata.AddedAt,
			UpdatedAt:        metadata.UpdatedAt,
			ParentKey:        metadata.ParentKey,
			GrandparentKey:   metadata.GrandparentKey,
			ParentTitle:      metadata.ParentTitle,
			GrandparentTitle: metadata.GrandparentTitle,
		})
	}

	return items, nil
}

// getLibrarySectionIDs returns the IDs of library sections to monitor
func (f *Fs) getLibrarySectionIDs(ctx context.Context) ([]string, error) {
	if len(f.sections) > 0 {
		// Use configured sections
		return f.sections, nil
	}

	// Get all library sections
	container, err := f.makeAPICall(ctx, "/library/sections")
	if err != nil {
		return nil, err
	}

	var sectionIDs []string
	for _, section := range container.Directories {
		sectionIDs = append(sectionIDs, section.Key)
	}

	return sectionIDs, nil
}

// mapItemToPath converts a Plex item to a VFS path for notification
func (f *Fs) mapItemToPath(item PlexRecentItem) string {
	switch item.Type {
	case "episode":
		// For episodes: GrandparentTitle/Season X/Episode
		if item.GrandparentTitle != "" && item.ParentTitle != "" {
			return fmt.Sprintf("%s/%s", 
				f.sanitizeName(item.GrandparentTitle), 
				f.sanitizeName(item.ParentTitle))
		}
		return f.sanitizeName(item.Title)
	case "season":
		// For seasons: GrandparentTitle/Season
		if item.GrandparentTitle != "" {
			return f.sanitizeName(item.GrandparentTitle)
		}
		return f.sanitizeName(item.Title)
	case "movie":
		// For movies: just the movie directory
		return f.sanitizeName(item.Title)
	case "show":
		// For shows: the show directory
		return f.sanitizeName(item.Title)
	default:
		return f.sanitizeName(item.Title)
	}
}

// Fs represents a remote plex server
type Fs struct {
	name     string                    // name of this remote
	root     string                    // the path we are working on
	opt      Options                   // parsed options
	features *fs.Features              // optional features
	srv      *rest.Client              // the connection to the plex server
	pacer    *fs.Pacer                 // To pace the API calls
	sections []string                  // library sections to include
	dirCache map[string]*PlexDirectory // Cache for directory metadata
	cacheMux sync.RWMutex              // Mutex for directory cache
}

// Object describes a plex media file
type Object struct {
	fs          *Fs           // what this object is part of
	remote      string        // The remote path
	hasMetaData bool          // whether info below has been set
	size        int64         // size of the object
	modTime     time.Time     // modification time of the object
	id          string        // Plex media ID
	mediaType   string        // Type of media (movie, episode, etc.)
	downloadURL string        // Direct download URL
	mimeType    string        // MIME type
	metadata    *PlexMetadata // Plex metadata
}

// NewFs constructs an Fs from the path, container:path
func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	// Parse config into Options struct
	opt := new(Options)
	err := configstruct.Set(m, opt)
	if err != nil {
		return nil, err
	}

	// Validate required configuration
	if opt.URL == "" {
		return nil, fmt.Errorf("server URL is required")
	}
	if opt.Token == "" {
		return nil, fmt.Errorf("authentication token is required")
	}

	// Decrypt the token if it's been obscured by rclone
	token, err := obscure.Reveal(opt.Token)
	if err != nil {
		// If decryption fails, assume token is not encrypted
		token = opt.Token
	}
	opt.Token = token

	// Parse the URL
	u, err := url.Parse(opt.URL)
	if err != nil {
		return nil, fmt.Errorf("invalid URL %q: %w", opt.URL, err)
	}

	// Set up the rest client
	client := fshttp.NewClient(ctx)
	srv := rest.NewClient(client).SetRoot(u.String())

	// Create Fs object
	f := &Fs{
		name:     name,
		root:     root,
		opt:      *opt,
		srv:      srv,
		pacer:    fs.NewPacer(ctx, pacer.NewS3(pacer.MinSleep(10*time.Millisecond), pacer.MaxSleep(2*time.Second))),
		dirCache: make(map[string]*PlexDirectory),
	}

	// Parse library sections if provided
	if opt.LibrarySections != "" {
		f.sections = strings.Split(opt.LibrarySections, ",")
		for i := range f.sections {
			f.sections[i] = strings.TrimSpace(f.sections[i])
		}
	}

	f.features = (&fs.Features{
		ReadMimeType:     true,
		ReadDirMetadata:  true,  // Enable directory metadata
		WriteDirMetadata: false, // Read-only backend
	}).Fill(ctx, f)

	// Test the connection
	err = f.testConnection(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Plex server: %w", err)
	}

	return f, nil
}

// cacheDirectory stores a directory in the cache
func (f *Fs) cacheDirectory(path string, dir *PlexDirectory) {
	f.cacheMux.Lock()
	defer f.cacheMux.Unlock()
	f.dirCache[path] = dir
	fs.Debugf(f, "Cached directory: %s -> %s (rating key: %s)", path, dir.name, dir.ratingKey)
}

// getCachedDirectory retrieves a directory from the cache
func (f *Fs) getCachedDirectory(path string) *PlexDirectory {
	f.cacheMux.RLock()
	defer f.cacheMux.RUnlock()
	return f.dirCache[path]
}

// getMediaByRatingKey retrieves media directly by rating key
func (f *Fs) getMediaByRatingKey(ctx context.Context, ratingKey string) (*PlexMetadata, error) {
	endpoint := fmt.Sprintf("/library/metadata/%s", ratingKey)
	fs.Debugf(f, "Getting media by rating key: %s, endpoint: %s", ratingKey, endpoint)

	container, err := f.makeAPICall(ctx, endpoint)
	if err != nil {
		fs.Debugf(f, "API call failed for rating key %s: %v", ratingKey, err)
		return nil, err
	}

	fs.Debugf(f, "API response for rating key %s: %d metadata items, %d videos", ratingKey, len(container.Metadata), len(container.Videos))

	// Check Metadata first
	if len(container.Metadata) > 0 {
		metadata := &container.Metadata[0]
		fs.Debugf(f, "Found in Metadata: Title='%s', Type='%s', Media count=%d", metadata.Title, metadata.Type, len(metadata.Media))
		return metadata, nil
	}

	// Fallback to Videos array and convert to PlexMetadata
	if len(container.Videos) > 0 {
		video := container.Videos[0]
		fs.Debugf(f, "Found in Videos: Title='%s', Type='%s', Media count=%d", video.Title, video.Type, len(video.Media))

		// Convert Video to PlexMetadata
		metadata := &PlexMetadata{
			RatingKey:            video.RatingKey,
			Key:                  video.Key,
			Type:                 video.Type,
			Title:                video.Title,
			ParentKey:            video.ParentKey,
			ParentRatingKey:      video.ParentRatingKey,
			GrandparentKey:       video.GrandparentKey,
			GrandparentRatingKey: video.GrandparentRatingKey,
			GrandparentTitle:     video.GrandparentTitle,
			ParentTitle:          video.ParentTitle,
			ParentIndex:          video.ParentIndex,
			Index:                video.Index,
			Year:                 video.Year,
			Duration:             video.Duration,
			AddedAt:              video.AddedAt,
			UpdatedAt:            video.UpdatedAt,
			Media:                video.Media,
			Guid:                 video.Guid,
		}

		fs.Debugf(f, "Converted Video to PlexMetadata: Title='%s', Type='%s', Media count=%d", metadata.Title, metadata.Type, len(metadata.Media))
		return metadata, nil
	}

	fs.Debugf(f, "No metadata or videos found for rating key %s", ratingKey)
	return nil, fs.ErrorObjectNotFound
}

// testConnection tests the connection to the Plex server
func (f *Fs) testConnection(ctx context.Context) error {
	// Test connection by listing library sections - this is a standard Plex API endpoint
	_, err := f.makeAPICall(ctx, "/library/sections")
	if err != nil {
		return fmt.Errorf("connection test failed: %w", err)
	}

	return nil
}

// makeAPICall makes a call to the Plex API and parses the response
func (f *Fs) makeAPICall(ctx context.Context, apiPath string) (*MediaContainer, error) {
	opts := rest.Opts{
		Method: "GET",
		Path:   apiPath,
		Parameters: url.Values{
			"X-Plex-Token": {f.opt.Token},
		},
		Options: []fs.OpenOption{},
	}

	var container MediaContainer
	err := f.pacer.Call(func() (bool, error) {
		httpResp, err := f.srv.Call(ctx, &opts)
		if err != nil {
			return f.shouldRetry(ctx, httpResp, err)
		}
		defer httpResp.Body.Close()

		if httpResp.StatusCode >= 200 && httpResp.StatusCode < 300 {
			body, err := io.ReadAll(httpResp.Body)
			if err != nil {
				return false, fmt.Errorf("failed to read response: %w", err)
			}

			// Try to parse as XML first
			if err := xml.Unmarshal(body, &container); err != nil {
				// If XML fails, try JSON
				if jsonErr := json.Unmarshal(body, &container); jsonErr != nil {
					return false, fmt.Errorf("failed to parse response as XML or JSON: XML=%v, JSON=%v", err, jsonErr)
				}
			}
			return false, nil
		}

		return f.shouldRetry(ctx, httpResp, fmt.Errorf("HTTP %d", httpResp.StatusCode))
	})

	if err != nil {
		return nil, err
	}

	return &container, nil
}

// shouldRetry returns a boolean as to whether this resp and err deserve to be retried
func (f *Fs) shouldRetry(ctx context.Context, resp *http.Response, err error) (bool, error) {
	if fserrors.ContextError(ctx, &err) {
		return false, err
	}
	retryErrorCodes := []int{
		429, // Too Many Requests
		500, // Internal Server Error
		502, // Bad Gateway
		503, // Service Unavailable
		504, // Gateway Timeout
		509, // Bandwidth Limit Exceeded
	}
	return fserrors.ShouldRetry(err) || fserrors.ShouldRetryHTTP(resp, retryErrorCodes), err
}

// Name of the remote (as passed into NewFs)
func (f *Fs) Name() string {
	return f.name
}

// Root of the remote (as passed into NewFs)
func (f *Fs) Root() string {
	return f.root
}

// String converts this Fs to a string
func (f *Fs) String() string {
	return fmt.Sprintf("Plex root '%s'", f.root)
}

// Features returns the optional features of this Fs
func (f *Fs) Features() *fs.Features {
	return f.features
}

// Precision return the precision of this Fs
func (f *Fs) Precision() time.Duration {
	return time.Second
}

// Hashes returns the supported hash sets
func (f *Fs) Hashes() hash.Set {
	return hash.Set(hash.None)
}

// List the objects and directories in dir into entries
func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	fs.Debugf(f, "Listing directory: %s", dir)

	// Clean up the directory path
	dir = strings.Trim(dir, "/")

	// Combine the filesystem root with the requested directory
	fullPath := f.root
	if dir != "" {
		if fullPath != "" {
			fullPath = strings.TrimSuffix(fullPath, "/") + "/" + dir
		} else {
			fullPath = dir
		}
	}
	fullPath = strings.Trim(fullPath, "/")

	fs.Debugf(f, "Full path to list: %s", fullPath)

	if fullPath == "" {
		// Root level - return library sections as directories
		return f.listLibrarySections(ctx)
	}

	parts := strings.Split(fullPath, "/")
	sectionName := parts[0]

	// Try to get section from cache first
	cachedSection := f.getCachedDirectory(sectionName)
	if cachedSection != nil {
		fs.Debugf(f, "Using cached section: %s (rating key: %s)", sectionName, cachedSection.ratingKey)
		return f.listMediaInSection(ctx, cachedSection, parts[1:])
	}

	// Fallback to name-based lookup
	section, err := f.getLibrarySectionByName(ctx, sectionName)
	if err != nil {
		return nil, err
	}

	return f.listMediaInSection(ctx, section, parts[1:])
}

// listMediaInSection handles listing within a library section using cached metadata
func (f *Fs) listMediaInSection(ctx context.Context, section *PlexDirectory, remainingParts []string) (fs.DirEntries, error) {
	fs.Debugf(f, "listMediaInSection: section=%s, remainingParts=%v", section.name, remainingParts)

	switch len(remainingParts) {
	case 0:
		// Section root - return all media with metadata
		fs.Debugf(f, "Listing all media in section %s", section.name)
		return f.listMediaWithMetadata(ctx, section)
	case 1:
		// Media item level - check cache first
		mediaPath := section.name + "/" + remainingParts[0]
		fs.Debugf(f, "Looking for media at path: %s", mediaPath)

		if cachedMedia := f.getCachedDirectory(mediaPath); cachedMedia != nil {
			fs.Debugf(f, "Using cached media: %s (rating key: %s)", remainingParts[0], cachedMedia.ratingKey)
			if cachedMedia.plexType == "movie" {
				return f.listMovieFiles(ctx, cachedMedia, remainingParts[0])
			} else if cachedMedia.plexType == "show" {
				return f.listShowSeasons(ctx, cachedMedia, mediaPath)
			}
		}

		// Cache miss - populate cache by listing all media in this section
		fs.Debugf(f, "Cache miss for %s, populating cache by listing section %s", mediaPath, section.name)
		_, err := f.listMediaWithMetadata(ctx, section)
		if err != nil {
			return nil, err
		}

		// Try cache again after populating
		if cachedMedia := f.getCachedDirectory(mediaPath); cachedMedia != nil {
			fs.Debugf(f, "Found in cache after population: %s (rating key: %s)", remainingParts[0], cachedMedia.ratingKey)
			if cachedMedia.plexType == "movie" {
				return f.listMovieFiles(ctx, cachedMedia, remainingParts[0])
			} else if cachedMedia.plexType == "show" {
				return f.listShowSeasons(ctx, cachedMedia, mediaPath)
			}
		}

		fs.Debugf(f, "Media not found even after cache population: %s", mediaPath)
		return nil, fs.ErrorDirNotFound

	case 2:
		// Season/episode level for TV shows
		mediaPath := section.name + "/" + remainingParts[0]
		seasonPath := mediaPath + "/" + remainingParts[1]

		fs.Debugf(f, "Looking for season at path: %s", seasonPath)

		if cachedSeason := f.getCachedDirectory(seasonPath); cachedSeason != nil {
			fs.Debugf(f, "Using cached season: %s (rating key: %s)", remainingParts[1], cachedSeason.ratingKey)
			return f.listSeasonEpisodes(ctx, cachedSeason)
		}

		// Cache miss - first ensure we have the show cached
		if cachedShow := f.getCachedDirectory(mediaPath); cachedShow == nil {
			// Need to populate the media cache first
			fs.Debugf(f, "Show not cached, populating media cache for section %s", section.name)
			_, err := f.listMediaWithMetadata(ctx, section)
			if err != nil {
				return nil, err
			}
		}

		// Now try to get the show and populate its seasons
		if cachedShow := f.getCachedDirectory(mediaPath); cachedShow != nil {
			fs.Debugf(f, "Found show in cache, populating seasons for: %s (rating key: %s)", cachedShow.name, cachedShow.ratingKey)
			_, err := f.listShowSeasons(ctx, cachedShow, mediaPath)
			if err != nil {
				return nil, err
			}

			// Try cache again after populating seasons
			if cachedSeason := f.getCachedDirectory(seasonPath); cachedSeason != nil {
				fs.Debugf(f, "Found season in cache after population: %s (rating key: %s)", remainingParts[1], cachedSeason.ratingKey)
				return f.listSeasonEpisodes(ctx, cachedSeason)
			}
		}

		fs.Debugf(f, "Season not found even after cache population: %s", seasonPath)
		return nil, fs.ErrorDirNotFound

	default:
		return nil, fs.ErrorDirNotFound
	}
}

// listLibrarySections returns library sections as directories with metadata
func (f *Fs) listLibrarySections(ctx context.Context) (fs.DirEntries, error) {
	fs.Debugf(f, "Listing library sections")

	container, err := f.makeAPICall(ctx, "/library/sections")
	if err != nil {
		return nil, fmt.Errorf("failed to list library sections: %w", err)
	}

	var entries fs.DirEntries
	for _, section := range container.Directories {
		// Filter by sections if specified
		if len(f.sections) > 0 {
			found := false
			for _, allowedSection := range f.sections {
				if allowedSection == section.Key {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}

		modTime := time.Unix(section.UpdatedAt, 0)
		if modTime.IsZero() {
			modTime = time.Now()
		}

		// Create PlexDirectory with metadata
		dir := &PlexDirectory{
			fs:        f,
			name:      section.Title,
			modTime:   modTime,
			ratingKey: section.Key,
			plexType:  "library",
		}

		// Cache the directory
		f.cacheDirectory(section.Title, dir)

		entries = append(entries, dir)
		fs.Debugf(f, "Added library section: %s (rating key: %s)", section.Title, section.Key)
	}

	return entries, nil
}

// getLibrarySectionByName finds a library section by name (fallback method)
func (f *Fs) getLibrarySectionByName(ctx context.Context, name string) (*PlexDirectory, error) {
	container, err := f.makeAPICall(ctx, "/library/sections")
	if err != nil {
		return nil, err
	}

	for _, section := range container.Directories {
		// Filter by sections if specified
		if len(f.sections) > 0 {
			found := false
			for _, allowedSection := range f.sections {
				if allowedSection == section.Key {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}

		if section.Title == name {
			modTime := time.Unix(section.UpdatedAt, 0)
			if modTime.IsZero() {
				modTime = time.Now()
			}

			dir := &PlexDirectory{
				fs:        f,
				name:      section.Title,
				modTime:   modTime,
				ratingKey: section.Key,
				plexType:  "library",
			}

			// Cache the directory
			f.cacheDirectory(name, dir)

			return dir, nil
		}
	}
	return nil, fs.ErrorDirNotFound
}

// buildMediaPath creates the virtual directory name with metadata
func (f *Fs) buildMediaPath(metadata *PlexMetadata) string {
	// Extract year from originallyAvailableAt or year field
	year := f.extractYear(metadata)

	// Build directory name: "Title (Year)"
	return fmt.Sprintf("%s (%s)",
		f.sanitizeName(metadata.Title), year)
}

// extractYear extracts year from metadata
func (f *Fs) extractYear(metadata *PlexMetadata) string {
	if metadata.Year > 0 {
		return strconv.Itoa(metadata.Year)
	}
	if metadata.OriginallyAvailableAt != "" {
		// Parse date like "2009-12-18"
		if len(metadata.OriginallyAvailableAt) >= 4 {
			return metadata.OriginallyAvailableAt[:4]
		}
	}
	return "0000"
}

// sanitizeName sanitizes names for filesystem compatibility
func (f *Fs) sanitizeName(name string) string {
	// Replace problematic characters for filesystem compatibility
	name = strings.ReplaceAll(name, ":", " -")
	name = strings.ReplaceAll(name, "/", "-")
	name = strings.ReplaceAll(name, "\\", "-")
	name = strings.ReplaceAll(name, "\"", "'")
	name = strings.ReplaceAll(name, "*", "_")
	name = strings.ReplaceAll(name, "?", "_")
	name = strings.ReplaceAll(name, "<", "_")
	name = strings.ReplaceAll(name, ">", "_")
	name = strings.ReplaceAll(name, "|", "_")
	return name
}

// listMediaWithMetadata returns media items with metadata in their names
func (f *Fs) listMediaWithMetadata(ctx context.Context, section *PlexDirectory) (fs.DirEntries, error) {
	endpoint := fmt.Sprintf("/library/sections/%s/all", section.ratingKey)
	container, err := f.makeAPICall(ctx, endpoint)
	if err != nil {
		return nil, err
	}

	var entries fs.DirEntries

	// Handle different response formats
	if len(container.Metadata) > 0 {
		for _, item := range container.Metadata {
			// Build directory name with metadata
			dirName := f.buildMediaPath(&item)
			modTime := time.Unix(item.UpdatedAt, 0)
			if modTime.IsZero() {
				modTime = time.Now()
			}

			// Create PlexDirectory with metadata
			dir := &PlexDirectory{
				fs:        f,
				name:      dirName,
				modTime:   modTime,
				ratingKey: item.RatingKey,
				plexType:  item.Type,
			}

			// Cache the directory mapping: virtual name -> rating key
			cachePath := section.name + "/" + dirName
			f.cacheDirectory(cachePath, dir)
			fs.Debugf(f, "Cached media: '%s' -> rating key %s", cachePath, item.RatingKey)

			entries = append(entries, dir)
		}
	} else {
		// Fallback to processing Directories and Videos
		for _, dir := range container.Directories {
			// Convert Directory to PlexMetadata for processing
			metadata := PlexMetadata{
				RatingKey: dir.RatingKey,
				Type:      dir.Type,
				Title:     dir.Title,
				Year:      dir.Year,
				UpdatedAt: dir.UpdatedAt,
			}
			dirName := f.buildMediaPath(&metadata)
			modTime := time.Unix(dir.UpdatedAt, 0)
			if modTime.IsZero() {
				modTime = time.Now()
			}

			// Create PlexDirectory with metadata
			plexDir := &PlexDirectory{
				fs:        f,
				name:      dirName,
				modTime:   modTime,
				ratingKey: dir.RatingKey,
				plexType:  dir.Type,
			}

			// Cache the directory mapping: virtual name -> rating key
			cachePath := section.name + "/" + dirName
			f.cacheDirectory(cachePath, plexDir)
			fs.Debugf(f, "Cached media: '%s' -> rating key %s", cachePath, dir.RatingKey)

			entries = append(entries, plexDir)
		}

		for _, video := range container.Videos {
			// Convert Video to PlexMetadata for processing
			metadata := PlexMetadata{
				RatingKey: video.RatingKey,
				Type:      video.Type,
				Title:     video.Title,
				Year:      video.Year,
				UpdatedAt: video.UpdatedAt,
				Guid:      video.Guid,
			}
			dirName := f.buildMediaPath(&metadata)
			modTime := time.Unix(video.UpdatedAt, 0)
			if modTime.IsZero() {
				modTime = time.Now()
			}

			// Create PlexDirectory with metadata
			plexDir := &PlexDirectory{
				fs:        f,
				name:      dirName,
				modTime:   modTime,
				ratingKey: video.RatingKey,
				plexType:  video.Type,
			}

			// Cache the directory mapping: virtual name -> rating key
			cachePath := section.name + "/" + dirName
			f.cacheDirectory(cachePath, plexDir)
			fs.Debugf(f, "Cached media: '%s' -> rating key %s", cachePath, video.RatingKey)

			entries = append(entries, plexDir)
		}
	}

	return entries, nil
}

// listMovieFiles returns media files for a movie using cached metadata
func (f *Fs) listMovieFiles(ctx context.Context, movieDir *PlexDirectory, movieDirName string) (fs.DirEntries, error) {
	fs.Debugf(f, "listMovieFiles: Getting files for movie '%s' with rating key %s", movieDirName, movieDir.ratingKey)

	// Use rating key for direct lookup
	movie, err := f.getMediaByRatingKey(ctx, movieDir.ratingKey)
	if err != nil {
		fs.Debugf(f, "Failed to get movie metadata for rating key %s: %v", movieDir.ratingKey, err)
		return nil, err
	}

	fs.Debugf(f, "Got movie metadata: Title='%s', Media items=%d", movie.Title, len(movie.Media))

	var entries fs.DirEntries
	// Return media files for this movie
	for i, media := range movie.Media {
		fs.Debugf(f, "Processing media %d: Container='%s', Parts=%d", i, media.Container, len(media.Parts))
		for j, part := range media.Parts {
			filename := path.Base(part.File)
			if filename == "" || filename == "." {
				// Fallback filename
				filename = f.sanitizeName(movie.Title) + f.getFileExtension(media.Container)
			}

			fs.Debugf(f, "Found file %d.%d: '%s' (size: %d)", i, j, filename, part.Size)

			obj := &Object{
				fs:          f,
				remote:      filename,
				hasMetaData: true,
				size:        part.Size,
				modTime:     time.Unix(movie.UpdatedAt, 0),
				id:          movie.RatingKey,
				mediaType:   movie.Type,
				downloadURL: part.Key,
				mimeType:    f.getMimeType(media.Container),
				metadata:    movie,
			}
			entries = append(entries, obj)
		}
	}

	fs.Debugf(f, "listMovieFiles: Returning %d file entries", len(entries))
	return entries, nil
}

// listShowSeasons returns seasons for a TV show using cached metadata
func (f *Fs) listShowSeasons(ctx context.Context, showDir *PlexDirectory, fullShowPath string) (fs.DirEntries, error) {
	// Use rating key for direct lookup
	endpoint := fmt.Sprintf("/library/metadata/%s/children", showDir.ratingKey)
	container, err := f.makeAPICall(ctx, endpoint)
	if err != nil {
		return nil, err
	}

	var entries fs.DirEntries

	// Process seasons from Metadata or Directories
	allSeasons := container.Metadata
	if len(allSeasons) == 0 {
		// Convert Directories to PlexMetadata
		for _, dir := range container.Directories {
			if dir.Type == "season" {
				metadata := PlexMetadata{
					RatingKey: dir.RatingKey,
					Type:      dir.Type,
					Title:     dir.Title,
					Index:     dir.Index,
					UpdatedAt: dir.UpdatedAt,
				}
				allSeasons = append(allSeasons, metadata)
			}
		}
	}

	for _, season := range allSeasons {
		if season.Type != "season" {
			continue
		}

		seasonName := fmt.Sprintf("Season %d", season.Index)
		if season.Title != "" && !strings.Contains(season.Title, "Season") {
			seasonName = season.Title
		}

		modTime := time.Unix(season.UpdatedAt, 0)
		if modTime.IsZero() {
			modTime = time.Now()
		}

		// Create PlexDirectory with metadata
		dir := &PlexDirectory{
			fs:        f,
			name:      seasonName,
			modTime:   modTime,
			ratingKey: season.RatingKey,
			plexType:  season.Type,
		}

		// Cache the directory mapping using the full path
		cachePath := fullShowPath + "/" + seasonName
		f.cacheDirectory(cachePath, dir)
		fs.Debugf(f, "Cached season: '%s' -> rating key %s", cachePath, season.RatingKey)

		entries = append(entries, dir)
	}
	return entries, nil
}

// listSeasonEpisodes returns episode files for a season using cached metadata
func (f *Fs) listSeasonEpisodes(ctx context.Context, seasonDir *PlexDirectory) (fs.DirEntries, error) {
	// Use rating key for direct lookup
	endpoint := fmt.Sprintf("/library/metadata/%s/children", seasonDir.ratingKey)
	container, err := f.makeAPICall(ctx, endpoint)
	if err != nil {
		return nil, err
	}

	var entries fs.DirEntries

	// Process episodes from Metadata or Videos
	allEpisodes := container.Metadata
	if len(allEpisodes) == 0 {
		// Convert Videos to PlexMetadata
		for _, video := range container.Videos {
			if video.Type == "episode" {
				metadata := PlexMetadata{
					RatingKey: video.RatingKey,
					Type:      video.Type,
					Title:     video.Title,
					Index:     video.Index,
					UpdatedAt: video.UpdatedAt,
					Media:     video.Media,
				}
				allEpisodes = append(allEpisodes, metadata)
			}
		}
	}

	for _, episode := range allEpisodes {
		if episode.Type != "episode" {
			continue
		}

		for _, media := range episode.Media {
			for _, part := range media.Parts {
				filename := path.Base(part.File)
				if filename == "" || filename == "." {
					// Fallback filename
					filename = fmt.Sprintf("S%02dE%02d - %s%s",
						1, episode.Index, // Default to season 1 if not available
						f.sanitizeName(episode.Title),
						f.getFileExtension(media.Container))
				}

				obj := &Object{
					fs:          f,
					remote:      filename,
					hasMetaData: true,
					size:        part.Size,
					modTime:     time.Unix(episode.UpdatedAt, 0),
					id:          episode.RatingKey,
					mediaType:   episode.Type,
					downloadURL: part.Key,
					mimeType:    f.getMimeType(media.Container),
					metadata:    &episode,
				}
				entries = append(entries, obj)
			}
		}
	}
	return entries, nil
}

// parseSeasonNumber extracts season number from "Season X" format
func (f *Fs) parseSeasonNumber(seasonName string) int {
	re := regexp.MustCompile(`Season (\d+)`)
	matches := re.FindStringSubmatch(seasonName)
	if len(matches) >= 2 {
		if num, err := strconv.Atoi(matches[1]); err == nil {
			return num
		}
	}
	return 1 // Default to season 1
}

// NewObject finds the Object at remote
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	fs.Debugf(f, "Looking for object: %s", remote)

	// Split the path to identify the components
	parts := strings.Split(strings.Trim(remote, "/"), "/")
	if len(parts) < 3 {
		return nil, fs.ErrorObjectNotFound
	}

	sectionName := parts[0]
	mediaName := parts[1]
	filename := parts[2]

	// Try to use cached section
	cachedSection := f.getCachedDirectory(sectionName)
	if cachedSection == nil {
		// Section not cached, get it
		var err error
		cachedSection, err = f.getLibrarySectionByName(ctx, sectionName)
		if err != nil {
			return nil, err
		}
	}

	// Try to use cached media
	mediaPath := sectionName + "/" + mediaName
	cachedMedia := f.getCachedDirectory(mediaPath)
	if cachedMedia == nil {
		// Media not cached, populate cache by listing section
		fs.Debugf(f, "Media not cached, populating cache for section %s", sectionName)
		_, err := f.listMediaWithMetadata(ctx, cachedSection)
		if err != nil {
			return nil, err
		}
		// Try again after populating cache
		cachedMedia = f.getCachedDirectory(mediaPath)
		if cachedMedia == nil {
			fs.Debugf(f, "Media still not found after populating cache: %s", mediaPath)
			return nil, fs.ErrorObjectNotFound
		}
	}

	if len(parts) == 3 {
		// Movie: Section/MovieName (Year)/filename
		if cachedMedia.plexType == "movie" {
			return f.findMovieObject(ctx, cachedMedia, filename)
		}
		return nil, fs.ErrorObjectNotFound
	} else if len(parts) == 4 {
		// TV Show: Section/ShowName (Year)/Season X/filename
		seasonName := parts[2]
		filename = parts[3]

		// Try to use cached season
		seasonPath := mediaPath + "/" + seasonName
		cachedSeason := f.getCachedDirectory(seasonPath)
		if cachedSeason == nil {
			// Season not cached, populate by listing show seasons
			fs.Debugf(f, "Season not cached, populating seasons for show %s", mediaName)
			_, err := f.listShowSeasons(ctx, cachedMedia, mediaPath)
			if err != nil {
				return nil, err
			}
			// Try again after populating cache
			cachedSeason = f.getCachedDirectory(seasonPath)
			if cachedSeason == nil {
				fs.Debugf(f, "Season still not found after populating cache: %s", seasonPath)
				return nil, fs.ErrorObjectNotFound
			}
		}

		return f.findEpisodeObject(ctx, cachedSeason, filename)
	}

	return nil, fs.ErrorObjectNotFound
}

// findMovieObject finds a movie file object using cached metadata
func (f *Fs) findMovieObject(ctx context.Context, movieDir *PlexDirectory, filename string) (fs.Object, error) {
	movie, err := f.getMediaByRatingKey(ctx, movieDir.ratingKey)
	if err != nil {
		return nil, err
	}

	for _, media := range movie.Media {
		for _, part := range media.Parts {
			actualFilename := path.Base(part.File)
			if actualFilename == "" || actualFilename == "." {
				actualFilename = f.sanitizeName(movie.Title) + f.getFileExtension(media.Container)
			}

			if actualFilename == filename {
				return &Object{
					fs:          f,
					remote:      fmt.Sprintf("%s/%s/%s", movieDir.fs.name, movieDir.name, filename),
					hasMetaData: true,
					size:        part.Size,
					modTime:     time.Unix(movie.UpdatedAt, 0),
					id:          movie.RatingKey,
					mediaType:   movie.Type,
					downloadURL: part.Key,
					mimeType:    f.getMimeType(media.Container),
					metadata:    movie,
				}, nil
			}
		}
	}
	return nil, fs.ErrorObjectNotFound
}

// findEpisodeObject finds a TV episode file object using cached metadata
func (f *Fs) findEpisodeObject(ctx context.Context, seasonDir *PlexDirectory, filename string) (fs.Object, error) {
	endpoint := fmt.Sprintf("/library/metadata/%s/children", seasonDir.ratingKey)
	container, err := f.makeAPICall(ctx, endpoint)
	if err != nil {
		return nil, err
	}

	// Process episodes from Metadata or Videos
	allEpisodes := container.Metadata
	if len(allEpisodes) == 0 {
		for _, video := range container.Videos {
			if video.Type == "episode" {
				metadata := PlexMetadata{
					RatingKey: video.RatingKey,
					Type:      video.Type,
					Title:     video.Title,
					Index:     video.Index,
					UpdatedAt: video.UpdatedAt,
					Media:     video.Media,
				}
				allEpisodes = append(allEpisodes, metadata)
			}
		}
	}

	for _, episode := range allEpisodes {
		if episode.Type != "episode" {
			continue
		}

		for _, media := range episode.Media {
			for _, part := range media.Parts {
				actualFilename := path.Base(part.File)
				if actualFilename == "" || actualFilename == "." {
					actualFilename = fmt.Sprintf("S%02dE%02d - %s%s",
						1, episode.Index,
						f.sanitizeName(episode.Title),
						f.getFileExtension(media.Container))
				}

				if actualFilename == filename {
					return &Object{
						fs:          f,
						remote:      fmt.Sprintf("%s/%s/%s", seasonDir.fs.name, seasonDir.name, filename),
						hasMetaData: true,
						size:        part.Size,
						modTime:     time.Unix(episode.UpdatedAt, 0),
						id:          episode.RatingKey,
						mediaType:   episode.Type,
						downloadURL: part.Key,
						mimeType:    f.getMimeType(media.Container),
						metadata:    &episode,
					}, nil
				}
			}
		}
	}
	return nil, fs.ErrorObjectNotFound
}

// Put is not supported - read only backend
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	return nil, errorReadOnly
}

// Mkdir is not supported - read only backend
func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	return errorReadOnly
}

// Rmdir is not supported - read only backend
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	return errorReadOnly
}

// Utility functions
func (f *Fs) getFileExtension(container string) string {
	switch container {
	case "mp4":
		return ".mp4"
	case "mkv":
		return ".mkv"
	case "avi":
		return ".avi"
	case "mov":
		return ".mov"
	case "wmv":
		return ".wmv"
	case "flv":
		return ".flv"
	case "webm":
		return ".webm"
	case "mp3":
		return ".mp3"
	case "flac":
		return ".flac"
	case "m4a":
		return ".m4a"
	default:
		return "." + container
	}
}

func (f *Fs) getMimeType(container string) string {
	switch container {
	case "mp4":
		return "video/mp4"
	case "mkv":
		return "video/x-matroska"
	case "avi":
		return "video/x-msvideo"
	case "mov":
		return "video/quicktime"
	case "wmv":
		return "video/x-ms-wmv"
	case "flv":
		return "video/x-flv"
	case "webm":
		return "video/webm"
	case "mp3":
		return "audio/mpeg"
	case "flac":
		return "audio/flac"
	case "m4a":
		return "audio/mp4"
	default:
		return "application/octet-stream"
	}
}

// Object functions

// Fs returns the parent Fs
func (o *Object) Fs() fs.Info {
	return o.fs
}

// Return a string version
func (o *Object) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.remote
}

// Remote returns the remote path
func (o *Object) Remote() string {
	return o.remote
}

// Hash is not supported
func (o *Object) Hash(ctx context.Context, t hash.Type) (string, error) {
	return "", hash.ErrUnsupported
}

// Size returns the size of an object in bytes
func (o *Object) Size() int64 {
	return o.size
}

// ModTime returns the modification time of the object
func (o *Object) ModTime(ctx context.Context) time.Time {
	return o.modTime
}

// SetModTime is not supported - read only backend
func (o *Object) SetModTime(ctx context.Context, modTime time.Time) error {
	return errorReadOnly
}

// Storable returns whether this object is storable
func (o *Object) Storable() bool {
	return true
}

// Open an object for read
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	if o.downloadURL == "" {
		return nil, fmt.Errorf("no download URL available for %s", o.remote)
	}

	// Build download URL - the downloadURL from Plex is the part key
	fullURL := fmt.Sprintf("%s%s?download=1&X-Plex-Token=%s", o.fs.opt.URL, o.downloadURL, o.fs.opt.Token)

	// Make the request
	opts := rest.Opts{
		Method:  "GET",
		RootURL: fullURL,
		Options: options,
	}

	var resp *http.Response
	var err error
	err = o.fs.pacer.Call(func() (bool, error) {
		resp, err = o.fs.srv.Call(ctx, &opts)
		return o.fs.shouldRetry(ctx, resp, err)
	})

	if err != nil {
		return nil, fmt.Errorf("failed to open %s: %w", o.remote, err)
	}

	return resp.Body, nil
}

// Update is not supported - read only backend
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	return errorReadOnly
}

// Remove is not supported - read only backend
func (o *Object) Remove(ctx context.Context) error {
	return errorReadOnly
}

// MimeType of an Object if known, "" otherwise
func (o *Object) MimeType(ctx context.Context) string {
	return o.mimeType
}

// ChangeNotify calls the passed function with a path that has had changes.
// Uses efficient polling of recentlyAdded and newest endpoints for change detection.
func (f *Fs) ChangeNotify(ctx context.Context, notifyFunc func(string, fs.EntryType), pollIntervalChan <-chan time.Duration) {
	var pollState PlexPollState
	pollState.KnownItems = make(map[string]int64)
	
	fs.Debugf(f, "ChangeNotify: Starting Plex polling for changes")
	
	for interval := range pollIntervalChan {
		if interval == 0 {
			fs.Debugf(f, "ChangeNotify: Polling paused")
			continue
		}
		
		fs.Debugf(f, "ChangeNotify: Polling with interval %v", interval)
		
		// Get library section IDs to monitor
		sectionIDs, err := f.getLibrarySectionIDs(ctx)
		if err != nil {
			fs.Errorf(f, "ChangeNotify: Failed to get library sections: %v", err)
			continue
		}
		
		// Check each library section for changes
		for _, sectionID := range sectionIDs {
			// Check recently added items
			if err := f.checkRecentlyAdded(ctx, sectionID, &pollState, notifyFunc); err != nil {
				fs.Errorf(f, "ChangeNotify: Failed to check recently added for section %s: %v", sectionID, err)
			}
			
			// Check newest items (for updates to existing content)
			if err := f.checkNewest(ctx, sectionID, &pollState, notifyFunc); err != nil {
				fs.Errorf(f, "ChangeNotify: Failed to check newest for section %s: %v", sectionID, err)
			}
		}
	}
	
	fs.Debugf(f, "ChangeNotify: Polling stopped")
}

// checkRecentlyAdded checks for newly added items in a library section
func (f *Fs) checkRecentlyAdded(ctx context.Context, sectionID string, pollState *PlexPollState, notifyFunc func(string, fs.EntryType)) error {
	items, err := f.getRecentlyAdded(ctx, sectionID)
	if err != nil {
		return fmt.Errorf("failed to get recently added items: %w", err)
	}
	
	fs.Debugf(f, "ChangeNotify: Got %d recently added items from section %s", len(items), sectionID)
	
	for _, item := range items {
		// Check if this is a new item we haven't seen before
		if item.AddedAt > pollState.LastRecentlyAddedAt {
			// This is a newly added item
			path := f.mapItemToPath(item)
			if path != "" {
				fs.Debugf(f, "ChangeNotify: New item detected: %s (type: %s)", path, item.Type)
				
				// Determine entry type
				entryType := fs.EntryObject
				if item.Type == "show" || item.Type == "season" {
					entryType = fs.EntryDirectory
				}
				
				notifyFunc(path, entryType)
			}
			
			// Update our tracking
			if item.AddedAt > pollState.LastRecentlyAddedAt {
				pollState.LastRecentlyAddedAt = item.AddedAt
			}
		}
		
		// Also track this item for change detection
		pollState.KnownItems[item.RatingKey] = item.UpdatedAt
	}
	
	return nil
}

// checkNewest checks for updated items in a library section
func (f *Fs) checkNewest(ctx context.Context, sectionID string, pollState *PlexPollState, notifyFunc func(string, fs.EntryType)) error {
	items, err := f.getNewest(ctx, sectionID)
	if err != nil {
		return fmt.Errorf("failed to get newest items: %w", err)
	}
	
	fs.Debugf(f, "ChangeNotify: Got %d newest items from section %s", len(items), sectionID)
	
	for _, item := range items {
		// Check if this item has been updated since we last saw it
		if lastSeen, exists := pollState.KnownItems[item.RatingKey]; exists {
			if item.UpdatedAt > lastSeen {
				// This item has been updated
				path := f.mapItemToPath(item)
				if path != "" {
					fs.Debugf(f, "ChangeNotify: Updated item detected: %s (type: %s)", path, item.Type)
					
					// Determine entry type
					entryType := fs.EntryObject
					if item.Type == "show" || item.Type == "season" {
						entryType = fs.EntryDirectory
					}
					
					notifyFunc(path, entryType)
				}
			}
		}
		
		// Update our tracking
		pollState.KnownItems[item.RatingKey] = item.UpdatedAt
		
		if item.UpdatedAt > pollState.LastNewestUpdatedAt {
			pollState.LastNewestUpdatedAt = item.UpdatedAt
		}
	}
	
	return nil
}

// Check the interfaces are satisfied
var (
	_ fs.Fs            = (*Fs)(nil)
	_ fs.Object        = (*Object)(nil)
	_ fs.DirEntry      = (*PlexDirectory)(nil)
	_ fs.Metadataer    = (*PlexDirectory)(nil)
	_ fs.ChangeNotifier = (*Fs)(nil)
)
