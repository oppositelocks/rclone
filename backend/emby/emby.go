// Package emby provides a filesystem interface to Emby Media Server
//
// This backend allows rclone to treat an Emby Media Server as a read-only
// remote filesystem, enabling operations like copy and mount.
package emby

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
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
	errorReadOnly = errors.New("emby backend is read only")
)

// Register with Fs
func init() {
	fs.Register(&fs.RegInfo{
		Name:        "emby",
		Description: "Emby Media Server",
		NewFs:       NewFs,
		Config:      Config,
		Options: []fs.Option{
			{
				Name:     "connect_username",
				Help:     "Emby Connect username or email.\n\nLeave empty to use direct API key authentication (requires url and api_key in advanced options).",
				Required: false,
			},
			{
				Name:       "connect_password",
				Help:       "Emby Connect password.",
				IsPassword: true,
				Required:   false,
			},
			{
				Name:     "url",
				Help:     "URL of Emby server.\n\nOnly needed for direct API key authentication.\nE.g. https://192.168.1.100:8096 or http://emby.example.com:8096",
				Required: false,
				Advanced: true,
			},
			{
				Name:       "api_key",
				Help:       "Emby API key.\n\nOnly needed for direct API key authentication.\nGet this from Emby Web interface: Dashboard > Advanced > Security > API Keys",
				IsPassword: true,
				Required:   false,
				Advanced:   true,
			},
			{
				Name:     "insecure_skip_verify",
				Help:     "Skip verification of SSL certificates.\n\nUse this if your Emby server uses self-signed certificates.",
				Default:  false,
				Advanced: true,
			},
			{
				Name:     "library_names",
				Help:     "Comma separated list of library names to include.\n\nLeave empty to include all libraries. Use 'rclone lsd emby:' to see available libraries.",
				Default:  "",
				Advanced: true,
			},
		},
	})
}

// Constants for Emby client identification
const (
	embyClientName    = "rclone"
	embyDeviceName    = "rclone-backend"
	embyClientVersion = "1.70.0"
)

// Emby Connect authentication structures
type EmbyConnectAuthRequest struct {
	NameOrEmail string `json:"nameOrEmail"`
	RawPw       string `json:"rawpw"`
}

type EmbyConnectAuthResponse struct {
	User struct {
		Id          string `json:"Id"`
		Name        string `json:"Name"`
		DisplayName string `json:"DisplayName"`
		Email       string `json:"Email"`
		IsActive    string `json:"IsActive"`
	} `json:"User"`
	AccessToken string `json:"AccessToken"`
}

type EmbyConnectServer struct {
	AccessKey    string `json:"AccessKey"`
	SystemId     string `json:"SystemId"`
	Name         string `json:"Name"`
	Url          string `json:"Url"`
	LocalAddress string `json:"LocalAddress"`
}

type EmbyConnectExchangeResponse struct {
	LocalUserId string `json:"LocalUserId"`
	AccessToken string `json:"AccessToken"`
}

// Emby API Response Structures
type EmbyLibrary struct {
	Name              string   `json:"Name"`
	ServerId          string   `json:"ServerId"`
	Id                string   `json:"Id"`
	Etag              string   `json:"Etag"`
	DateCreated       string   `json:"DateCreated"`
	DateLastMediaAdded string   `json:"DateLastMediaAdded"`
	CanDelete         bool     `json:"CanDelete"`
	CanDownload       bool     `json:"CanDownload"`
	SortName          string   `json:"SortName"`
	PrimaryImageAspectRatio float64 `json:"PrimaryImageAspectRatio"`
	CollectionType    string   `json:"CollectionType"`
	ImageTags         map[string]string `json:"ImageTags"`
	BackdropImageTags []string `json:"BackdropImageTags"`
	LocationType      string   `json:"LocationType"`
	MediaType         string   `json:"MediaType"`
	Type              string   `json:"Type"`
	UserData          EmbyUserData `json:"UserData"`
	ParentId          string   `json:"ParentId"`
	Path              string   `json:"Path"`
}

type EmbyUserData struct {
	PlaybackPositionTicks int64  `json:"PlaybackPositionTicks"`
	PlayCount             int    `json:"PlayCount"`
	IsFavorite            bool   `json:"IsFavorite"`
	Played                bool   `json:"Played"`
	Key                   string `json:"Key"`
}

type EmbyItems struct {
	Items            []EmbyItem `json:"Items"`
	TotalRecordCount int        `json:"TotalRecordCount"`
	StartIndex       int        `json:"StartIndex"`
}

type EmbyItem struct {
	Name                    string       `json:"Name"`
	ServerId                string       `json:"ServerId"`
	Id                      string       `json:"Id"`
	Etag                    string       `json:"Etag"`
	DateCreated             string       `json:"DateCreated"`
	DateLastMediaAdded      string       `json:"DateLastMediaAdded"`
	Container               string       `json:"Container"`
	CanDelete               bool         `json:"CanDelete"`
	CanDownload             bool         `json:"CanDownload"`
	PreferredMetadataLanguage string     `json:"PreferredMetadataLanguage"`
	PreferredMetadataCountryCode string  `json:"PreferredMetadataCountryCode"`
	SortName                string       `json:"SortName"`
	ForcedSortName          string       `json:"ForcedSortName"`
	Video3DFormat           string       `json:"Video3DFormat"`
	PremiereDate            string       `json:"PremiereDate"`
	MediaSources            []MediaSource `json:"MediaSources"`
	ExternalUrls            []interface{} `json:"ExternalUrls"`
	Path                    string       `json:"Path"`
	Overview                string       `json:"Overview"`
	Taglines                []interface{} `json:"Taglines"`
	Genres                  []string     `json:"Genres"`
	SeriesName              string       `json:"SeriesName"`
	SeriesId                string       `json:"SeriesId"`
	SeasonId                string       `json:"SeasonId"`
	CommunityRating         float64      `json:"CommunityRating"`
	RunTimeTicks            int64        `json:"RunTimeTicks"`
	ProductionYear          int          `json:"ProductionYear"`
	IndexNumber             int          `json:"IndexNumber"`
	IndexNumberEnd          int          `json:"IndexNumberEnd"`
	ParentIndexNumber       int          `json:"ParentIndexNumber"`
	ProviderIds             map[string]string `json:"ProviderIds"`
	IsFolder                bool         `json:"IsFolder"`
	ParentId                string       `json:"ParentId"`
	Type                    string       `json:"Type"`
	Studios                 []interface{} `json:"Studios"`
	GenreItems              []interface{} `json:"GenreItems"`
	DisplayOrder            string       `json:"DisplayOrder"`
	ParentLogoItemId        string       `json:"ParentLogoItemId"`
	ParentBackdropItemId    string       `json:"ParentBackdropItemId"`
	ParentBackdropImageTags []string     `json:"ParentBackdropImageTags"`
	UserData                EmbyUserData `json:"UserData"`
	RecursiveItemCount      int          `json:"RecursiveItemCount"`
	ChildCount              int          `json:"ChildCount"`
	SpecialFeatureCount     int          `json:"SpecialFeatureCount"`
	DisplayPreferencesId    string       `json:"DisplayPreferencesId"`
	Status                  string       `json:"Status"`
	AirDays                 []interface{} `json:"AirDays"`
	Tags                    []interface{} `json:"Tags"`
	PrimaryImageAspectRatio float64      `json:"PrimaryImageAspectRatio"`
	ImageTags               map[string]string `json:"ImageTags"`
	BackdropImageTags       []string     `json:"BackdropImageTags"`
	ImageBlurHashes         map[string]map[string]string `json:"ImageBlurHashes"`
	LocationType            string       `json:"LocationType"`
	MediaType               string       `json:"MediaType"`
	EndDate                 string       `json:"EndDate"`
	Size                    int64        `json:"Size"`
	Width                   int          `json:"Width"`
	Height                  int          `json:"Height"`
}

type MediaSource struct {
	Protocol              string        `json:"Protocol"`
	Id                    string        `json:"Id"`
	Path                  string        `json:"Path"`
	Type                  string        `json:"Type"`
	Container             string        `json:"Container"`
	Size                  int64         `json:"Size"`
	Name                  string        `json:"Name"`
	IsRemote              bool          `json:"IsRemote"`
	ETag                  string        `json:"ETag"`
	RunTimeTicks          int64         `json:"RunTimeTicks"`
	ReadAtNativeFramerate bool          `json:"ReadAtNativeFramerate"`
	IgnoreDts             bool          `json:"IgnoreDts"`
	IgnoreIndex           bool          `json:"IgnoreIndex"`
	GenPtsInput           bool          `json:"GenPtsInput"`
	SupportsTranscoding   bool          `json:"SupportsTranscoding"`
	SupportsDirectStream  bool          `json:"SupportsDirectStream"`
	SupportsDirectPlay    bool          `json:"SupportsDirectPlay"`
	IsInfiniteStream      bool          `json:"IsInfiniteStream"`
	RequiresOpening       bool          `json:"RequiresOpening"`
	RequiresClosing       bool          `json:"RequiresClosing"`
	LiveStreamId          string        `json:"LiveStreamId"`
	BufferMs              int           `json:"BufferMs"`
	RequiresLooping       bool          `json:"RequiresLooping"`
	SupportsProbing       bool          `json:"SupportsProbing"`
	VideoType             string        `json:"VideoType"`
	MediaStreams          []MediaStream `json:"MediaStreams"`
	MediaAttachments      []interface{} `json:"MediaAttachments"`
	Formats               []string      `json:"Formats"`
	Bitrate               int           `json:"Bitrate"`
	RequiredHttpHeaders   map[string]string `json:"RequiredHttpHeaders"`
	TranscodingUrl        string        `json:"TranscodingUrl"`
	TranscodingSubProtocol string       `json:"TranscodingSubProtocol"`
	TranscodingContainer  string        `json:"TranscodingContainer"`
	AnalyzeDurationMs     int           `json:"AnalyzeDurationMs"`
	DefaultAudioStreamIndex int         `json:"DefaultAudioStreamIndex"`
	DefaultSubtitleStreamIndex int      `json:"DefaultSubtitleStreamIndex"`
}

type MediaStream struct {
	Codec                     string  `json:"Codec"`
	CodecTag                  string  `json:"CodecTag"`
	Language                  string  `json:"Language"`
	ColorRange                string  `json:"ColorRange"`
	ColorSpace                string  `json:"ColorSpace"`
	ColorTransfer             string  `json:"ColorTransfer"`
	ColorPrimaries            string  `json:"ColorPrimaries"`
	Comment                   string  `json:"Comment"`
	TimeBase                  string  `json:"TimeBase"`
	CodecTimeBase             string  `json:"CodecTimeBase"`
	Title                     string  `json:"Title"`
	VideoRange                string  `json:"VideoRange"`
	LocalizedUndefined        string  `json:"localizedUndefined"`
	LocalizedDefault          string  `json:"localizedDefault"`
	LocalizedForced           string  `json:"localizedForced"`
	DisplayTitle              string  `json:"DisplayTitle"`
	NalLengthSize             string  `json:"NalLengthSize"`
	IsInterlaced              bool    `json:"IsInterlaced"`
	IsAVC                     bool    `json:"IsAVC"`
	ChannelLayout             string  `json:"ChannelLayout"`
	BitRate                   int     `json:"BitRate"`
	BitDepth                  int     `json:"BitDepth"`
	RefFrames                 int     `json:"RefFrames"`
	PacketLength              int     `json:"PacketLength"`
	Channels                  int     `json:"Channels"`
	SampleRate                int     `json:"SampleRate"`
	IsDefault                 bool    `json:"IsDefault"`
	IsForced                  bool    `json:"IsForced"`
	Height                    int     `json:"Height"`
	Width                     int     `json:"Width"`
	AverageFrameRate          float64 `json:"AverageFrameRate"`
	RealFrameRate             float64 `json:"RealFrameRate"`
	Profile                   string  `json:"Profile"`
	Type                      string  `json:"Type"`
	AspectRatio               string  `json:"AspectRatio"`
	Index                     int     `json:"Index"`
	Score                     int     `json:"Score"`
	IsExternal                bool    `json:"IsExternal"`
	DeliveryMethod            string  `json:"DeliveryMethod"`
	DeliveryUrl               string  `json:"DeliveryUrl"`
	IsExternalUrl             bool    `json:"IsExternalUrl"`
	IsTextSubtitleStream      bool    `json:"IsTextSubtitleStream"`
	SupportsExternalStream    bool    `json:"SupportsExternalStream"`
	PixelFormat               string  `json:"PixelFormat"`
	Level                     float64 `json:"Level"`
}

// Options defines the configuration for this backend
type Options struct {
	URL                string `config:"url"`
	APIKey             string `config:"api_key"`
	ConnectUsername    string `config:"connect_username"`
	ConnectPassword    string `config:"connect_password"`
	InsecureSkipVerify bool   `config:"insecure_skip_verify"`
	LibraryNames       string `config:"library_names"`
}

// FolderState represents the state of a folder for change detection
type FolderState struct {
	ID          string `json:"id"`
	Etag        string `json:"etag"`
	Path        string `json:"path"`
	DateCreated string `json:"dateCreated"`
	IsFolder    bool   `json:"isFolder"`
}

// EmbyDirectory represents a directory in the Emby filesystem with metadata support
type EmbyDirectory struct {
	fs        *Fs
	name      string
	modTime   time.Time
	id        string
	itemType  string // "library", "series", "season", "movie", "folder"
	etag      string  // Etag for change detection
	metadata  fs.Metadata
}

// Fs returns the parent Fs
func (d *EmbyDirectory) Fs() fs.Info {
	return d.fs
}

// Name returns the name of the directory
func (d *EmbyDirectory) Name() string {
	return d.name
}

// Size returns the size, which is always 0 for directories
func (d *EmbyDirectory) Size() int64 {
	return 0
}

// ModTime returns the modification time
func (d *EmbyDirectory) ModTime(ctx context.Context) time.Time {
	return d.modTime
}

// IsDir returns true as this is always a directory
func (d *EmbyDirectory) IsDir() bool {
	return true
}

// Metadata returns the metadata for this directory
func (d *EmbyDirectory) Metadata(ctx context.Context) (fs.Metadata, error) {
	if d.metadata == nil {
		d.metadata = make(fs.Metadata)
		d.metadata["emby-id"] = d.id
		d.metadata["emby-type"] = d.itemType
		d.metadata["emby-title"] = d.name
		d.metadata["emby-etag"] = d.etag
		d.metadata["mtime"] = d.modTime.Format(time.RFC3339Nano)
	}
	return d.metadata, nil
}

// SetMetadata sets metadata for this directory (not supported in read-only backend)
func (d *EmbyDirectory) SetMetadata(ctx context.Context, metadata fs.Metadata) error {
	return errorReadOnly
}

// String returns a description of the directory
func (d *EmbyDirectory) String() string {
	return d.name
}

// Remote returns the remote path of the directory
func (d *EmbyDirectory) Remote() string {
	return d.name
}

// Items returns the count of items (unknown for Emby directories)
func (d *EmbyDirectory) Items() int64 {
	return -1
}

// ID returns the directory's ID if known
func (d *EmbyDirectory) ID() string {
	return d.id
}

// Fs represents a remote emby server
type Fs struct {
	name      string                   // name of this remote
	root      string                   // the path we are working on
	opt       Options                  // parsed options
	features  *fs.Features             // optional features
	srv       *rest.Client             // the connection to the emby server
	pacer     *fs.Pacer                // To pace the API calls
	libraries []string                 // library names to include
	apiToken  string                   // The API token to use (either API key or Connect access token)
	
	// For Emby Connect token refresh
	connectUserId   string                // User ID for Connect authentication
	connectServer   *EmbyConnectServer   // Selected server info for re-authentication
}

// Object describes an emby media file
type Object struct {
	fs          *Fs          // what this object is part of
	remote      string       // The remote path
	hasMetaData bool         // whether info below has been set
	size        int64        // size of the object
	modTime     time.Time    // modification time of the object
	id          string       // Emby item ID
	mediaType   string       // Type of media (Movie, Episode, etc.)
	downloadPath string      // Path for download
	mimeType    string       // MIME type
	item        *EmbyItem    // Emby item metadata
}

// generateDeviceId generates a unique device ID for this session
func generateDeviceId() string {
	bytes := make([]byte, 16)
	rand.Read(bytes)
	return hex.EncodeToString(bytes)
}

// decryptSecret safely decrypts an obscured secret (password, API key, etc.)
// Returns the decrypted value or the original value if decryption fails
func decryptSecret(obscuredValue string) string {
	if obscuredValue == "" {
		return ""
	}
	
	if decrypted, err := obscure.Reveal(obscuredValue); err == nil {
		// Successfully decrypted
		return decrypted
	}
	
	// If decryption fails, the value might not be encrypted
	// Return the original value (this handles cases where users provide raw passwords)
	return obscuredValue
}

// addEmbyConnectHeaders adds the required Emby Connect headers to an HTTP request
func addEmbyConnectHeaders(req *http.Request) {
	req.Header.Set("X-Application", embyClientName)
}

// addEmbyServerHeaders adds the required Emby server headers to an HTTP request
func addEmbyServerHeaders(req *http.Request, deviceId string) {
	req.Header.Set("X-Emby-Client", embyClientName)
	req.Header.Set("X-Emby-Device-Name", embyDeviceName)
	req.Header.Set("X-Emby-Device-Id", deviceId)
	req.Header.Set("X-Emby-Client-Version", embyClientVersion)
	req.Header.Set("X-Emby-Language", "en-us")
}

// authenticateEmbyConnectFull authenticates with Emby Connect and returns full response
func authenticateEmbyConnectFull(ctx context.Context, username, password string) (*EmbyConnectAuthResponse, error) {
	client := fshttp.NewClient(ctx)
	
	// Decrypt the password if it's been obscured by rclone
	decryptedPassword := decryptSecret(password)
	
	authReq := EmbyConnectAuthRequest{
		NameOrEmail: username,
		RawPw:       decryptedPassword,
	}
	
	reqBody, err := json.Marshal(authReq)
	if err != nil {
		return nil, err
	}
	
	fmt.Printf("Sending authentication request to Emby Connect...\n")
	
	req, err := http.NewRequestWithContext(ctx, "POST", "https://connect.emby.media/service/user/authenticate", strings.NewReader(string(reqBody)))
	if err != nil {
		return nil, err
	}
	
	// Add standard headers
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	
	// Add Emby Connect identification headers
	addEmbyConnectHeaders(req)
	
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Emby Connect: %w", err)
	}
	defer resp.Body.Close()
	
	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read authentication response: %w", err)
	}
	
	if resp.StatusCode != http.StatusOK {
		// Log detailed error for debugging but don't expose sensitive info to user
		fs.Debugf(nil, "Emby Connect authentication failed with status %d: %s", resp.StatusCode, string(body))
		
		// Provide user-friendly error messages based on status code
		switch resp.StatusCode {
		case http.StatusUnauthorized:
			return nil, errors.New("invalid Emby Connect username or password")
		case http.StatusForbidden:
			return nil, errors.New("Emby Connect account access denied")
		case http.StatusTooManyRequests:
			return nil, errors.New("too many authentication attempts, please wait and try again")
		default:
			return nil, fmt.Errorf("Emby Connect authentication failed (HTTP %d)", resp.StatusCode)
		}
	}
	
	var authResp EmbyConnectAuthResponse
	if err := json.Unmarshal(body, &authResp); err != nil {
		return nil, fmt.Errorf("failed to parse Emby Connect authentication response: %w", err)
	}
	
	return &authResp, nil
}

// getEmbyConnectServers gets the list of available servers for the authenticated user
func getEmbyConnectServers(ctx context.Context, connectToken, connectUserId string) ([]EmbyConnectServer, error) {
	client := fshttp.NewClient(ctx)
	
	// Build URL with userId parameter
	serverURL := fmt.Sprintf("https://connect.emby.media/service/servers?userId=%s", connectUserId)
	
	req, err := http.NewRequestWithContext(ctx, "GET", serverURL, nil)
	if err != nil {
		return nil, err
	}
	
	// Add Connect auth header
	req.Header.Set("X-Connect-UserToken", connectToken)
	
	// Add Emby Connect identification headers
	addEmbyConnectHeaders(req)
	
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Emby Connect for server list: %w", err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		fs.Debugf(nil, "Emby Connect server discovery failed with status %d: %s", resp.StatusCode, string(body))
		
		switch resp.StatusCode {
		case http.StatusUnauthorized:
			return nil, errors.New("Emby Connect authentication token expired or invalid")
		case http.StatusForbidden:
			return nil, errors.New("access denied to Emby Connect servers")
		default:
			return nil, fmt.Errorf("failed to get Emby server list (HTTP %d)", resp.StatusCode)
		}
	}
	
	var servers []EmbyConnectServer
	if err := json.NewDecoder(resp.Body).Decode(&servers); err != nil {
		return nil, fmt.Errorf("failed to parse Emby server list: %w", err)
	}
	
	return servers, nil
}

// exchangeConnectToken exchanges the Emby Connect AccessKey for a local server access token
func exchangeConnectToken(ctx context.Context, server *EmbyConnectServer, connectUserId string) (string, error) {
	client := fshttp.NewClient(ctx)
	
	// Generate device ID for this session
	deviceId := generateDeviceId()
	
	// Build the exchange URL - this goes to the actual Emby server, not connect.emby.media
	exchangeURL := fmt.Sprintf("%s/Connect/Exchange?format=json&ConnectUserId=%s", 
		server.Url, connectUserId)
	
	fs.Debugf(nil, "Exchanging connect token at: %s", exchangeURL)
	
	req, err := http.NewRequestWithContext(ctx, "GET", exchangeURL, nil)
	if err != nil {
		return "", err
	}
	
	// Add Emby server identification headers (this talks to the Emby server, not Connect)
	addEmbyServerHeaders(req, deviceId)
	
	// Add the AccessKey as X-Emby-Token header for Connect exchange
	req.Header.Set("X-Emby-Token", server.AccessKey)
	
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to connect to Emby server for token exchange: %w", err)
	}
	defer resp.Body.Close()
	
	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read token exchange response: %w", err)
	}
	
	if resp.StatusCode != http.StatusOK {
		fs.Debugf(nil, "Emby Connect token exchange failed with status %d: %s", resp.StatusCode, string(body))
		
		switch resp.StatusCode {
		case http.StatusUnauthorized:
			return "", errors.New("Emby server rejected Connect authentication - check server configuration")
		case http.StatusForbidden:
			return "", errors.New("access denied by Emby server")
		case http.StatusNotFound:
			return "", errors.New("Emby server does not support Connect authentication")
		default:
			return "", fmt.Errorf("token exchange failed (HTTP %d)", resp.StatusCode)
		}
	}
	
	var exchangeResp EmbyConnectExchangeResponse
	if err := json.Unmarshal(body, &exchangeResp); err != nil {
		return "", fmt.Errorf("failed to parse Emby Connect token exchange response: %w", err)
	}
	
	fs.Debugf(nil, "Successfully exchanged connect token for local access token")
	return exchangeResp.AccessToken, nil
}

// authenticateEmbyConnect handles the simplified Emby Connect authentication for non-interactive use
func authenticateEmbyConnect(ctx context.Context, username, password string) (*EmbyConnectServer, string, string, error) {
	// Authenticate with Emby Connect first
	authResp, err := authenticateEmbyConnectFull(ctx, username, password)
	if err != nil {
		return nil, "", "", err
	}

	// Get list of available servers for this user
	servers, err := getEmbyConnectServers(ctx, authResp.AccessToken, authResp.User.Id)
	if err != nil {
		return nil, "", "", fmt.Errorf("failed to get servers: %w", err)
	}

	if len(servers) == 0 {
		return nil, "", "", fmt.Errorf("no servers found for this Emby Connect account")
	}

	// For now, just return the first server
	// TODO: Allow user to choose which server to use
	server := servers[0]

	// Exchange Connect AccessKey for local server access token
	accessToken, err := exchangeConnectToken(ctx, &server, authResp.User.Id)
	if err != nil {
		return nil, "", "", fmt.Errorf("failed to exchange connect token: %w", err)
	}

	return &server, accessToken, authResp.User.Id, nil
}

// Config is called by rclone config
func Config(ctx context.Context, name string, m configmap.Mapper, config fs.ConfigIn) (*fs.ConfigOut, error) {
	username, _ := m.Get("connect_username")
	password, _ := m.Get("connect_password")
	
	// Only handle Emby Connect configuration if username is provided
	if username == "" {
		return nil, nil // Let standard config flow handle API key method
	}

	if password == "" {
		return nil, fmt.Errorf("connect_password is required when connect_username is provided")
	}

	fmt.Printf("Authenticating with Emby Connect as %s...\n", username)

	// Authenticate with Emby Connect
	authResp, err := authenticateEmbyConnectFull(ctx, username, password)
	if err != nil {
		return nil, fmt.Errorf("Emby Connect authentication failed: %w", err)
	}

	fmt.Println("Authentication successful! Fetching available servers...")

	// Get available servers - use User.Id as the ConnectUserId
	servers, err := getEmbyConnectServers(ctx, authResp.AccessToken, authResp.User.Id)
	if err != nil {
		return nil, fmt.Errorf("failed to get Emby servers: %w", err)
	}

	if len(servers) == 0 {
		return nil, fmt.Errorf("no Emby servers found for this account")
	}

	// Present server selection menu
	fmt.Println("\nAvailable Emby Servers:")
	for i, server := range servers {
		fmt.Printf("%d. %s (%s)\n", i+1, server.Name, server.Url)
	}
	
	fmt.Print("\nChoose server [1-", len(servers), "]: ")
	var choice int
	if _, err := fmt.Scanf("%d", &choice); err != nil || choice < 1 || choice > len(servers) {
		return nil, fmt.Errorf("invalid server selection")
	}

	selectedServer := servers[choice-1]

	// Exchange the Connect AccessKey for a local server access token
	fmt.Printf("Connecting to %s...\n", selectedServer.Name)
	localAccessToken, err := exchangeConnectToken(ctx, &selectedServer, authResp.User.Id)
	if err != nil {
		return nil, fmt.Errorf("failed to exchange connect token: %w", err)
	}

	// Set the configuration values in the mapper
	m.Set("selected_server_url", selectedServer.Url)
	m.Set("selected_server_id", selectedServer.SystemId)
	m.Set("access_token", localAccessToken)
	m.Set("connect_user_id", authResp.User.Id) // Store user ID for token refresh
	
	// Clear the url and api_key since we don't need them for connect method
	m.Set("url", "")
	m.Set("api_key", "")
	
	fmt.Printf("\nSelected server: %s (%s)\n", selectedServer.Name, selectedServer.Url)
	fmt.Println("Configuration complete!")

	return nil, nil
}

// NewFs constructs an Fs from the path, container:path
func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	// Parse config into Options struct
	opt := new(Options)
	err := configstruct.Set(m, opt)
	if err != nil {
		return nil, err
	}

	// Create Fs object first
	f := &Fs{
		name: name,
		root: root,
		opt:  *opt,
	}

	// Parse library names if provided
	if opt.LibraryNames != "" {
		f.libraries = strings.Split(opt.LibraryNames, ",")
		for i := range f.libraries {
			f.libraries[i] = strings.TrimSpace(f.libraries[i])
		}
	}

	// Configure authentication and get API token based on what's provided
	// Check if we have URL and API key for direct authentication
	if opt.URL != "" && opt.APIKey != "" {
		// Use API key authentication
		fs.Debugf(f, "Using API key authentication")
		
		// Decrypt the API key if it's been obscured by rclone
		f.apiToken = decryptSecret(opt.APIKey)
		
		// Ensure no Connect-specific fields are set for API key authentication
		f.connectServer = nil
		f.connectUserId = ""

		// Parse the URL
		u, err := url.Parse(opt.URL)
		if err != nil {
			return nil, fmt.Errorf("invalid URL %q: %w", opt.URL, err)
		}

		// Set up the rest client
		client := fshttp.NewClient(ctx)
		f.srv = rest.NewClient(client).SetRoot(u.String())
		
	} else if opt.ConnectUsername != "" && opt.ConnectPassword != "" {
		// Use Emby Connect authentication
		fs.Debugf(f, "Using Emby Connect authentication")

		// Check if we have stored server config from the config process
		selectedServerURL, hasServerURL := m.Get("selected_server_url")
		storedAccessToken, hasAccessToken := m.Get("access_token")
		storedServerId, _ := m.Get("selected_server_id")
		
		if hasServerURL && hasAccessToken && selectedServerURL != "" && storedAccessToken != "" {
			// Use stored configuration from config process
			f.apiToken = storedAccessToken
			
			// Store server info for potential token refresh
			f.connectServer = &EmbyConnectServer{
				Url:      selectedServerURL,
				SystemId: storedServerId,
			}
			
			// Try to get stored user ID for token refresh (may not exist in older configs)
			if storedUserId, hasUserId := m.Get("connect_user_id"); hasUserId && storedUserId != "" {
				f.connectUserId = storedUserId
			}
			
			// Parse the server URL
			u, err := url.Parse(selectedServerURL)
			if err != nil {
				return nil, fmt.Errorf("invalid server URL %q: %w", selectedServerURL, err)
			}

			// Set up the rest client
			client := fshttp.NewClient(ctx)
			f.srv = rest.NewClient(client).SetRoot(u.String())
		} else {
			// Authenticate with Emby Connect and get server details
			server, accessToken, userId, err := authenticateEmbyConnect(ctx, opt.ConnectUsername, opt.ConnectPassword)
			if err != nil {
				return nil, fmt.Errorf("Emby Connect authentication failed: %w", err)
			}

			f.apiToken = accessToken
			f.connectServer = server   // Store for token refresh
			f.connectUserId = userId   // Store for token refresh

			// Parse the server URL
			u, err := url.Parse(server.Url)
			if err != nil {
				return nil, fmt.Errorf("invalid server URL %q: %w", server.Url, err)
			}

			// Set up the rest client
			client := fshttp.NewClient(ctx)
			f.srv = rest.NewClient(client).SetRoot(u.String())
		}
		
	} else {
		return nil, fmt.Errorf("either provide connect_username and connect_password for Emby Connect authentication, or url and api_key in advanced options for direct API key authentication")
	}

	f.pacer = fs.NewPacer(ctx, pacer.NewS3(pacer.MinSleep(10*time.Millisecond), pacer.MaxSleep(2*time.Second)))

	f.features = (&fs.Features{
		ReadMimeType:     true,
		ReadDirMetadata:  true,  // Enable directory metadata
		WriteDirMetadata: false, // Read-only backend
	}).Fill(ctx, f)

	// Test the connection
	err = f.testConnection(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Emby server: %w", err)
	}

	return f, nil
}

// testConnection tests the connection to the Emby server
func (f *Fs) testConnection(ctx context.Context) error {
	opts := rest.Opts{
		Method: "GET",
		Path:   "/System/Info",
		Parameters: url.Values{
			"api_key": {f.apiToken},
		},
	}

	var result interface{}
	_, err := f.srv.CallJSON(ctx, &opts, nil, &result)
	if err != nil {
		return fmt.Errorf("connection test failed: %w", err)
	}

	return nil
}

// makeAPICall makes a call to the Emby API
func (f *Fs) makeAPICall(ctx context.Context, apiPath string, params url.Values) (json.RawMessage, error) {
	if params == nil {
		params = url.Values{}
	}
	params.Set("api_key", f.apiToken)

	opts := rest.Opts{
		Method:     "GET",
		Path:       apiPath,
		Parameters: params,
	}

	var result json.RawMessage
	err := f.pacer.Call(func() (bool, error) {
		httpResp, err := f.srv.CallJSON(ctx, &opts, nil, &result)
		return f.shouldRetry(ctx, httpResp, err)
	})

	if err != nil {
		return nil, err
	}

	return result, nil
}

// shouldRetry returns a boolean as to whether this resp and err deserve to be retried
func (f *Fs) shouldRetry(ctx context.Context, resp *http.Response, err error) (bool, error) {
	if fserrors.ContextError(ctx, &err) {
		return false, err
	}
	
	// Handle 401 Unauthorized - attempt token refresh for Connect authentication only
	// We know it's Connect auth if we have connectServer info AND Connect credentials
	if resp != nil && resp.StatusCode == 401 && f.connectServer != nil && 
	   f.opt.ConnectUsername != "" && f.opt.ConnectPassword != "" {
		fs.Debugf(f, "Received 401 Unauthorized, attempting to refresh Emby Connect token")
		
		if refreshErr := f.refreshConnectToken(ctx); refreshErr != nil {
			fs.Debugf(f, "Token refresh failed: %v", refreshErr)
			return false, fmt.Errorf("authentication failed and token refresh failed: %w", refreshErr)
		}
		
		fs.Debugf(f, "Token refresh successful, retrying request")
		return true, nil
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

// refreshConnectToken attempts to refresh the Emby Connect access token
func (f *Fs) refreshConnectToken(ctx context.Context) error {
	if f.connectServer == nil {
		return fmt.Errorf("no server information available for token refresh")
	}
	
	// Check that we have Emby Connect credentials available for refresh
	if f.opt.ConnectUsername == "" || f.opt.ConnectPassword == "" {
		return fmt.Errorf("Emby Connect credentials not available for token refresh - please reconfigure with 'rclone config'")
	}
	
	// Re-authenticate with Emby Connect
	authResp, err := authenticateEmbyConnectFull(ctx, f.opt.ConnectUsername, f.opt.ConnectPassword)
	if err != nil {
		return fmt.Errorf("failed to re-authenticate with Emby Connect: %w", err)
	}
	
	// Use the user ID from the new auth response (should be the same as stored)
	userId := authResp.User.Id
	if f.connectUserId != "" && f.connectUserId != userId {
		fs.Debugf(f, "Warning: User ID changed during token refresh (old: %s, new: %s)", f.connectUserId, userId)
	}
	f.connectUserId = userId
	
	// Exchange the new Connect token for a local server access token
	newAccessToken, err := exchangeConnectToken(ctx, f.connectServer, userId)
	if err != nil {
		return fmt.Errorf("failed to exchange refreshed connect token: %w", err)
	}
	
	// Update our stored token
	f.apiToken = newAccessToken
	fs.Debugf(f, "Successfully refreshed Emby Connect access token")
	
	return nil
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
	return fmt.Sprintf("Emby root '%s'", f.root)
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

// parseIdFromMetadata extracts the Emby ID from metadata if available
func parseIdFromMetadata(metadata fs.Metadata) string {
	if metadata != nil {
		if id, ok := metadata["emby-id"]; ok {
			return id
		}
	}
	return ""
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
		return f.listLibraries(ctx)
	}

	// For non-root paths, we need to resolve the path to an Emby item ID
	// First try to get the ID from directory metadata if we're navigating through rclone
	
	// Try to find the item by walking the path
	return f.listItemsByPath(ctx, fullPath)
}

// listLibraries returns library sections as directories
func (f *Fs) listLibraries(ctx context.Context) (fs.DirEntries, error) {
	fs.Debugf(f, "Listing libraries")

	result, err := f.makeAPICall(ctx, "/Library/VirtualFolders", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to list libraries: %w", err)
	}

	var libraries []EmbyLibrary
	if err := json.Unmarshal(result, &libraries); err != nil {
		return nil, fmt.Errorf("failed to parse libraries: %w", err)
	}

	var entries fs.DirEntries
	for _, library := range libraries {
		// Filter by library names if specified
		if len(f.libraries) > 0 {
			found := false
			for _, allowedLibrary := range f.libraries {
				if allowedLibrary == library.Name {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}

		modTime, _ := time.Parse(time.RFC3339, library.DateCreated)
		if modTime.IsZero() {
			// Use a consistent epoch time instead of time.Now() for VFS cache compatibility
			modTime = time.Unix(0, 0)
		}

		// Create EmbyDirectory with metadata
		dir := &EmbyDirectory{
			fs:       f,
			name:     library.Name,
			modTime:  modTime,
			id:       library.Id,
			itemType: "library",
			etag:     library.Etag,
		}

		entries = append(entries, dir)
		fs.Debugf(f, "Added library: %s (id: %s)", library.Name, library.Id)
	}

	return entries, nil
}

// listItemsByPath resolves a path and lists the items within it
func (f *Fs) listItemsByPath(ctx context.Context, fullPath string) (fs.DirEntries, error) {
	fs.Debugf(f, "listItemsByPath: resolving path %s", fullPath)

	parts := strings.Split(fullPath, "/")
	libraryName := parts[0]

	// First, get the library ID
	libraryId, err := f.getLibraryIdByName(ctx, libraryName)
	if err != nil {
		return nil, err
	}

	// If we're just listing the library root
	if len(parts) == 1 {
		return f.listItemsInContainer(ctx, libraryId)
	}

	// For deeper paths, we need to resolve each component
	currentId := libraryId
	
	for i := 1; i < len(parts); i++ {
		pathComponent := parts[i]
		fs.Debugf(f, "Resolving path component: %s in container %s", pathComponent, currentId)
		
		// Get all items in the current container
		items, err := f.getItemsInContainer(ctx, currentId)
		if err != nil {
			return nil, err
		}

		// Find the item that matches this path component
		found := false
		for _, item := range items.Items {
			// Debug: show what we're comparing against
			itemBasePath := path.Base(item.Path)
			fs.Debugf(f, "Comparing pathComponent '%s' against item: Name='%s', Path='%s', BasePath='%s', Type='%s', IsFolder=%t", 
				pathComponent, item.Name, item.Path, itemBasePath, item.Type, item.IsFolder)
			
			// Use the base path from the item's Path field
			if itemBasePath == pathComponent {
				currentId = item.Id
				found = true
				fs.Debugf(f, "Found matching item: %s -> %s", pathComponent, item.Id)
				break
			}
		}

		if !found {
			fs.Debugf(f, "Path component not found: %s", pathComponent)
			return nil, fs.ErrorDirNotFound
		}
	}

	// Now list the contents of the final resolved container
	return f.listItemsInContainer(ctx, currentId)
}

// getLibraryIdByName finds a library ID by its name
func (f *Fs) getLibraryIdByName(ctx context.Context, name string) (string, error) {
	result, err := f.makeAPICall(ctx, "/Library/VirtualFolders", nil)
	if err != nil {
		return "", err
	}

	var libraries []EmbyLibrary
	if err := json.Unmarshal(result, &libraries); err != nil {
		return "", err
	}

	for _, library := range libraries {
		// Filter by library names if specified
		if len(f.libraries) > 0 {
			found := false
			for _, allowedLibrary := range f.libraries {
				if allowedLibrary == library.Name {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}

		if library.Name == name {
			return library.Id, nil
		}
	}
	return "", fs.ErrorDirNotFound
}

// getItemsInContainer gets all items in a container by ID using pagination
func (f *Fs) getItemsInContainer(ctx context.Context, containerId string) (*EmbyItems, error) {
	const limit = 50000
	var allItems []EmbyItem
	startIndex := 0
	totalRecordCount := 0

	for {
		params := url.Values{
			"ParentId":   {containerId},
			"Recursive":  {"false"},
			"Fields":     {"Path,MediaSources,DateCreated"},
			"StartIndex": {fmt.Sprintf("%d", startIndex)},
			"Limit":      {fmt.Sprintf("%d", limit)},
		}

		fs.Debugf(f, "Fetching items from container %s: StartIndex=%d, Limit=%d", containerId, startIndex, limit)

		result, err := f.makeAPICall(ctx, "/Items", params)
		if err != nil {
			return nil, err
		}

		var batch EmbyItems
		if err := json.Unmarshal(result, &batch); err != nil {
			return nil, fmt.Errorf("failed to parse items batch: %w", err)
		}

		// Accumulate items from this batch
		allItems = append(allItems, batch.Items...)
		
		// Update total record count from the first response
		if startIndex == 0 {
			totalRecordCount = batch.TotalRecordCount
		}

		fs.Debugf(f, "Fetched %d items in this batch, %d total so far", len(batch.Items), len(allItems))

		// If we got fewer items than the limit, we've reached the end
		if len(batch.Items) < limit {
			fs.Debugf(f, "Reached end of results (got %d < %d limit)", len(batch.Items), limit)
			break
		}

		// Move to the next batch
		startIndex += limit
	}

	// Return combined results
	combinedItems := &EmbyItems{
		Items:            allItems,
		TotalRecordCount: totalRecordCount,
		StartIndex:       0, // Reset to 0 since we're returning all items
	}

	fs.Debugf(f, "getItemsInContainer: Returning %d total items from container %s", len(allItems), containerId)
	return combinedItems, nil
}

// listItemsInContainer lists all items in a container, returning both directories and files
func (f *Fs) listItemsInContainer(ctx context.Context, containerId string) (fs.DirEntries, error) {
	items, err := f.getItemsInContainer(ctx, containerId)
	if err != nil {
		return nil, err
	}

	var entries fs.DirEntries

	for _, item := range items.Items {
		modTime, _ := time.Parse(time.RFC3339, item.DateCreated)
		if modTime.IsZero() {
			// Use a consistent epoch time instead of time.Now() for VFS cache compatibility
			modTime = time.Unix(0, 0)
		}

		if item.IsFolder {
			// This is a directory - use the base path from the Path field
			dirName := path.Base(item.Path)
			if dirName == "" || dirName == "." {
				dirName = item.Name
			}

			// Create EmbyDirectory with metadata containing the ID
			dir := &EmbyDirectory{
				fs:       f,
				name:     dirName,
				modTime:  modTime,
				id:       item.Id,
				itemType: item.Type,
				etag:     item.Etag,
			}

			entries = append(entries, dir)
			fs.Debugf(f, "Added folder: %s (id: %s, type: %s)", dirName, item.Id, item.Type)
		} else {
			// This is a file - use the primary MediaSource only for VFS cache consistency
			// Using multiple MediaSources would create duplicate objects that confuse caching
			if len(item.MediaSources) > 0 {
				source := item.MediaSources[0] // Use primary/first source only
				
				// Generate filename using hybrid approach: prefer original, smart fallback
				filename := f.generateFileName(item.Name, source.Path, source.Container)

				obj := &Object{
					fs:           f,
					remote:       filename,
					hasMetaData:  true,
					size:         source.Size,
					modTime:      modTime,
					id:           item.Id,
					mediaType:    item.Type,
					downloadPath: source.Path,
					mimeType:     f.getMimeType(source.Container),
					item:         &item,
				}
				entries = append(entries, obj)
				fs.Debugf(f, "Added file: %s (size: %d)", filename, source.Size)
			}
		}
	}

	fs.Debugf(f, "listItemsInContainer: Returning %d entries", len(entries))
	return entries, nil
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

// generateFileName creates a filename using hybrid approach: prefer original, fallback to generated
func (f *Fs) generateFileName(itemName string, originalPath string, container string) string {
	// First priority: use original filename if available and valid
	if originalPath != "" {
		filename := path.Base(originalPath)
		if filename != "" && filename != "." && filename != "/" {
			// Original filename is valid, use it as-is
			// This preserves the exact filename that Emby has, including extension
			return filename
		}
	}
	
	// Second priority: generate filename from item name + appropriate extension
	if itemName != "" {
		sanitizedName := f.sanitizeName(itemName)
		extension := f.getFileExtension(container)
		return sanitizedName + extension
	}
	
	// Final fallback: use container as base name if we have nothing else
	if container != "" {
		extension := f.getFileExtension(container)
		return "media" + extension
	}
	
	// Last resort: generic filename
	return "unknown.bin"
}

// NewObject finds the Object at remote
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	fs.Debugf(f, "Looking for object: %s", remote)

	// Split the path to get the directory and filename
	dir := path.Dir(remote)
	filename := path.Base(remote)

	// Handle root directory case
	if dir == "." {
		dir = ""
	}

	// List the directory containing the file
	entries, err := f.List(ctx, dir)
	if err != nil {
		return nil, err
	}

	// Find the file in the entries
	for _, entry := range entries {
		entryName := path.Base(entry.Remote())
		if entryName == filename {
			if obj, ok := entry.(*Object); ok {
				// Update the remote path to match the requested path
				obj.remote = remote
				return obj, nil
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

// ChangeNotify calls the passed function with a path that has had changes.
// Uses efficient folder-based polling with Etag comparison for change detection.
func (f *Fs) ChangeNotify(ctx context.Context, fn func(string, fs.EntryType), ch <-chan time.Duration) {
	var previousFolders map[string]*FolderState
	
	fs.Debugf(f, "ChangeNotify: Starting efficient folder-based polling with Etag detection")
	
	for interval := range ch {
		if interval == 0 {
			fs.Debugf(f, "ChangeNotify: Polling paused")
			continue
		}
		
		fs.Debugf(f, "ChangeNotify: Polling with interval %v", interval)
		
		// Get current folder state via efficient folder-only queries
		currentFolders, err := f.getAllFoldersWithEtags(ctx)
		if err != nil {
			fs.Errorf(f, "ChangeNotify: Failed to get current folder state: %v", err)
			continue
		}
		
		fs.Debugf(f, "ChangeNotify: Retrieved %d current folders", len(currentFolders))
		
		if previousFolders != nil {
			// Detect deletions: in previous but not in current
			deletedCount := 0
			for id, folder := range previousFolders {
				if _, exists := currentFolders[id]; !exists {
					// Folder was deleted
					dirPath := f.getDirectoryPath(folder.Path)
					fn(dirPath, fs.EntryDirectory)
					deletedCount++
					
					fs.Debugf(f, "ChangeNotify: Detected folder deletion: %s", dirPath)
				}
			}
			
			// Detect additions/modifications: in current but not in previous, or with different Etag
			addedCount := 0
			modifiedCount := 0
			for id, folder := range currentFolders {
				if oldFolder, existed := previousFolders[id]; !existed {
					// New folder added
					dirPath := f.getDirectoryPath(folder.Path)
					fn(dirPath, fs.EntryDirectory)
					addedCount++
					
					fs.Debugf(f, "ChangeNotify: Detected folder addition: %s", dirPath)
				} else if oldFolder.Etag != folder.Etag {
					// Folder content was modified (Etag changed)
					dirPath := f.getDirectoryPath(folder.Path)
					fn(dirPath, fs.EntryDirectory)
					modifiedCount++
					
					fs.Debugf(f, "ChangeNotify: Detected folder modification: %s (Etag: %s -> %s)", 
						dirPath, oldFolder.Etag, folder.Etag)
				}
			}
			
			if deletedCount > 0 || addedCount > 0 || modifiedCount > 0 {
				fs.Debugf(f, "ChangeNotify: Changes detected - Added: %d, Modified: %d, Deleted: %d", 
					addedCount, modifiedCount, deletedCount)
			}
		} else {
			fs.Debugf(f, "ChangeNotify: First run, establishing baseline with %d folders", len(currentFolders))
		}
		
		// Update previous state for next iteration
		previousFolders = currentFolders
	}
	
	fs.Debugf(f, "ChangeNotify: Polling stopped")
}

// getAllFoldersWithEtags retrieves all folders from all libraries using efficient folder-only queries
func (f *Fs) getAllFoldersWithEtags(ctx context.Context) (map[string]*FolderState, error) {
	allFolders := make(map[string]*FolderState)
	
	// Get all libraries first
	result, err := f.makeAPICall(ctx, "/Library/VirtualFolders", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get libraries: %w", err)
	}

	var libraries []EmbyLibrary
	if err := json.Unmarshal(result, &libraries); err != nil {
		return nil, fmt.Errorf("failed to parse libraries: %w", err)
	}

	// For each library, get all folders with pagination
	for _, library := range libraries {
		// Filter by library names if specified
		if len(f.libraries) > 0 {
			found := false
			for _, allowedLibrary := range f.libraries {
				if allowedLibrary == library.Name {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}
		
		fs.Debugf(f, "ChangeNotify: Scanning folders in library %s (id: %s)", library.Name, library.Id)
		
		libraryFolders, err := f.getAllFoldersInLibrary(ctx, library.Id)
		if err != nil {
			return nil, fmt.Errorf("failed to get folders for library %s: %w", library.Name, err)
		}
		
		// Merge into the main map
		for id, folder := range libraryFolders {
			allFolders[id] = folder
		}
		
		fs.Debugf(f, "ChangeNotify: Library %s contributed %d folders", library.Name, len(libraryFolders))
	}
	
	return allFolders, nil
}

// getAllFoldersInLibrary gets all folders in a library with pagination, optimized for change detection
func (f *Fs) getAllFoldersInLibrary(ctx context.Context, libraryId string) (map[string]*FolderState, error) {
	const limit = 1000  // Much smaller batches since we're only getting folders
	allFolders := make(map[string]*FolderState)
	startIndex := 0

	for {
		params := url.Values{
			"ParentId":   {libraryId},
			"IsFolder":   {"true"},     // Only get folders
			"Recursive":  {"true"},     // Get all folders recursively
			"Fields":     {"Path,DateCreated,Etag"}, // Only fields we need for change detection
			"StartIndex": {fmt.Sprintf("%d", startIndex)},
			"Limit":      {fmt.Sprintf("%d", limit)},
		}

		fs.Debugf(f, "ChangeNotify: Fetching folders from library %s: StartIndex=%d, Limit=%d", 
			libraryId, startIndex, limit)

		result, err := f.makeAPICall(ctx, "/Items", params)
		if err != nil {
			return nil, err
		}

		var batch EmbyItems
		if err := json.Unmarshal(result, &batch); err != nil {
			return nil, fmt.Errorf("failed to parse folders batch: %w", err)
		}

		// Add folders from this batch to our map
		for _, item := range batch.Items {
			if item.IsFolder {
				folderState := &FolderState{
					ID:          item.Id,
					Etag:        item.Etag,
					Path:        item.Path,
					DateCreated: item.DateCreated,
					IsFolder:    item.IsFolder,
				}
				allFolders[item.Id] = folderState
			}
		}

		fs.Debugf(f, "ChangeNotify: Fetched %d folders in this batch, %d total for library", 
			len(batch.Items), len(allFolders))

		// If we got fewer items than the limit, we've reached the end
		if len(batch.Items) < limit {
			fs.Debugf(f, "ChangeNotify: Reached end of library %s folders (got %d < %d limit)", 
				libraryId, len(batch.Items), limit)
			break
		}

		// Move to the next batch
		startIndex += limit
	}

	return allFolders, nil
}

// getDirectoryPath returns the directory path that should be invalidated for an item
func (f *Fs) getDirectoryPath(itemPath string) string {
	if itemPath == "" {
		return ""
	}
	
	// For files, return the directory containing them
	// For directories, return the directory itself
	dirPath := path.Dir(itemPath)
	if dirPath == "." || dirPath == "/" {
		return ""
	}
	
	// Convert from Emby's absolute paths to our relative paths
	// Remove any leading path components that aren't part of our filesystem structure
	// This assumes the library name is the first component
	parts := strings.Split(strings.Trim(dirPath, "/"), "/")
	if len(parts) > 0 {
		// Return path relative to our filesystem root
		return strings.Join(parts, "/")
	}
	
	return ""
}

// Utility functions
func (f *Fs) getFileExtension(container string) string {
	switch strings.ToLower(container) {
	// Video containers
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
	case "ts":
		return ".ts"
	case "m2ts":
		return ".m2ts"
	case "mpegts":
		return ".ts"
	case "asf":
		return ".asf"
	case "3gp":
		return ".3gp"
	case "divx":
		return ".divx"
	case "xvid":
		return ".avi" // XviD typically uses .avi container
	case "ogv":
		return ".ogv"
	case "rm", "rmvb":
		return ".rm"
	case "vob":
		return ".vob"
	case "iso":
		return ".iso"
	case "img":
		return ".img"
	
	// Audio containers
	case "mp3":
		return ".mp3"
	case "flac":
		return ".flac"
	case "m4a":
		return ".m4a"
	case "aac":
		return ".aac"
	case "ogg":
		return ".ogg"
	case "wma":
		return ".wma"
	case "wav":
		return ".wav"
	case "aiff":
		return ".aiff"
	case "ape":
		return ".ape"
	case "ac3":
		return ".ac3"
	case "dts":
		return ".dts"
	case "opus":
		return ".opus"
	
	// Image containers
	case "jpg", "jpeg":
		return ".jpg"
	case "png":
		return ".png"
	case "gif":
		return ".gif"
	case "bmp":
		return ".bmp"
	case "tiff", "tif":
		return ".tiff"
	case "webp":
		return ".webp"
	
	// Subtitle formats
	case "srt":
		return ".srt"
	case "vtt":
		return ".vtt"
	case "ass":
		return ".ass"
	case "ssa":
		return ".ssa"
	case "sub":
		return ".sub"
	case "idx":
		return ".idx"
	
	default:
		// For unknown containers, use the container name directly as extension
		// This handles new formats automatically and is better than defaulting to .bin
		if container != "" {
			cleanContainer := strings.ToLower(container)
			// Remove any non-alphanumeric characters for safety
			cleanContainer = strings.Map(func(r rune) rune {
				if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
					return r
				}
				return -1 // Remove character
			}, cleanContainer)
			
			if cleanContainer != "" {
				return "." + cleanContainer
			}
		}
		return ".bin" // Final fallback for completely empty/invalid containers
	}
}

func (f *Fs) getMimeType(container string) string {
	switch strings.ToLower(container) {
	// Video MIME types
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
	case "ts", "m2ts", "mpegts":
		return "video/mp2t"
	case "asf":
		return "video/x-ms-asf"
	case "3gp":
		return "video/3gpp"
	case "divx":
		return "video/x-msvideo"
	case "xvid":
		return "video/x-msvideo"
	case "ogv":
		return "video/ogg"
	case "rm", "rmvb":
		return "application/vnd.rn-realmedia"
	case "vob":
		return "video/dvd"
	case "iso":
		return "application/x-iso9660-image"
	case "img":
		return "application/x-disk-image"
	
	// Audio MIME types
	case "mp3":
		return "audio/mpeg"
	case "flac":
		return "audio/flac"
	case "m4a":
		return "audio/mp4"
	case "aac":
		return "audio/aac"
	case "ogg":
		return "audio/ogg"
	case "wma":
		return "audio/x-ms-wma"
	case "wav":
		return "audio/wav"
	case "aiff":
		return "audio/aiff"
	case "ape":
		return "audio/x-ape"
	case "ac3":
		return "audio/ac3"
	case "dts":
		return "audio/vnd.dts"
	case "opus":
		return "audio/opus"
	
	// Image MIME types
	case "jpg", "jpeg":
		return "image/jpeg"
	case "png":
		return "image/png"
	case "gif":
		return "image/gif"
	case "bmp":
		return "image/bmp"
	case "tiff", "tif":
		return "image/tiff"
	case "webp":
		return "image/webp"
	
	// Subtitle MIME types
	case "srt":
		return "text/srt"
	case "vtt":
		return "text/vtt"
	case "ass", "ssa":
		return "text/x-ssa"
	case "sub":
		return "text/x-subtitle"
	case "idx":
		return "text/x-subtitle"
	
	default:
		// Provide intelligent fallbacks based on container characteristics
		containerLower := strings.ToLower(container)
		
		// Check if it might be a video format based on common patterns
		if strings.Contains(containerLower, "video") || 
		   strings.Contains(containerLower, "mpeg") ||
		   strings.Contains(containerLower, "h264") ||
		   strings.Contains(containerLower, "h265") ||
		   strings.Contains(containerLower, "hevc") ||
		   strings.Contains(containerLower, "av1") {
			return "video/" + containerLower
		}
		
		// Check if it might be an audio format
		if strings.Contains(containerLower, "audio") ||
		   strings.Contains(containerLower, "sound") {
			return "audio/" + containerLower
		}
		
		// Check if it might be an image format
		if strings.Contains(containerLower, "image") ||
		   strings.Contains(containerLower, "picture") {
			return "image/" + containerLower
		}
		
		// Check if it might be a text/subtitle format
		if strings.Contains(containerLower, "text") ||
		   strings.Contains(containerLower, "subtitle") ||
		   strings.Contains(containerLower, "caption") {
			return "text/" + containerLower
		}
		
		// Default fallback
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
	if o.id == "" {
		return nil, fmt.Errorf("no Emby item ID available for %s", o.remote)
	}

	// Build download URL for direct stream
	downloadURL := fmt.Sprintf("/Items/%s/Download", o.id)

	// Make the request using the rest client with API key authentication
	// The rest client automatically handles range requests via options for VFS caching
	opts := rest.Opts{
		Method: "GET",
		Path:   downloadURL,
		Parameters: url.Values{
			"api_key": {o.fs.apiToken},
		},
		Options: options, // This passes through range requests for VFS cache support
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

// Check the interfaces are satisfied
var (
	_ fs.Fs            = (*Fs)(nil)
	_ fs.Object        = (*Object)(nil)
	_ fs.DirEntry      = (*EmbyDirectory)(nil)
	_ fs.Metadataer    = (*EmbyDirectory)(nil)
	_ fs.ChangeNotifier = (*Fs)(nil)
)
