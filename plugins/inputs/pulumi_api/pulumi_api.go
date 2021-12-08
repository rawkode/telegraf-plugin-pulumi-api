package pulumi_api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/influxdata/telegraf"
	httpconfig "github.com/influxdata/telegraf/plugins/common/http"
	"github.com/influxdata/telegraf/plugins/inputs"
)

type PulumiApiConfig struct {
	Url          string `toml:"url"`
	Organization string `toml:"organization"`
	Token        string `toml:"token"`

	lastFetch         time.Time
	continuationToken uint64

	ctx    context.Context
	cancel context.CancelFunc

	client *http.Client
	httpconfig.HTTPClientConfig

	Log telegraf.Logger `toml:"-"`
}

type ApiError struct {
	Code    uint64
	Message string
}

type AuditLogsResponse struct {
	ContinuationToken uint64          `json:"continuationToken"`
	AuditLogEvents    []AuditLogEvent `json:"auditLogEvents"`
}

type AuditLogEvent struct {
	Timestamp   int64  `json:"timestamp"`
	SourceIP    string `json:"sourceIP"`
	Event       string `json:"event"`
	Description string `json:"description"`
	User        User   `json:"user"`
}

type User struct {
	Name        string `json:"name"`
	GitHubLogin string `json:"githubLogin"`
	AvatarUrl   string `json:"avatarUrl"`
}

func init() {
	inputs.Add("pulumi_api", func() telegraf.Input {
		return &PulumiApiConfig{
			Url: "https://api.pulumi.com",
		}
	})
}

func (p *PulumiApiConfig) Init() error {
	p.ctx, p.cancel = context.WithCancel(context.Background())

	p.continuationToken = 0
	p.lastFetch = time.Now().Add(time.Duration(-1) * time.Hour)

	client, err := p.HTTPClientConfig.CreateClient(p.ctx, p.Log)
	if err != nil {
		return err
	}

	p.client = client

	return nil
}

func (p *PulumiApiConfig) SampleConfig() string {
	return `
  ## Pulumi API Event & Metric Collector
	[inputs.pulumi_api]
	# url = "https://api.pulumi.com"
	organization = "${PULUMI_ORGANIZATION}"
	token = "${PULUMI_TOKEN}"
`
}

func (p *PulumiApiConfig) Description() string {
	return "Pulumi API Event & Metric Collector"
}

func (p *PulumiApiConfig) Gather(acc telegraf.Accumulator) error {
	p.Log.Debug("Gathering Pulumi API metrics")

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()

		p.Log.Debug("Fetching audit logs")

		lastFetch := time.Now()

		if err := p.fetchAuditLogs(acc); err != nil {
			acc.AddError(fmt.Errorf("[organization=%s,fetch=audit_logs]: %s", p.Organization, err))
		}

		p.lastFetch = lastFetch
		p.continuationToken = 0
	}()

	wg.Wait()
	return nil
}

func (p *PulumiApiConfig) Stop() {
	p.cancel()
}

func (p *PulumiApiConfig) auditLogUrl() string {
	url := fmt.Sprintf("%s/api/orgs/%s/auditlogs?startTime=%d", p.Url, p.Organization, p.lastFetch.Unix())

	if p.continuationToken != 0 {
		url = fmt.Sprintf("%s&continuationToken=%d", url, p.continuationToken)
	}

	p.Log.Debugf("audit_log_url: %s", url)

	return url
}

func (p *PulumiApiConfig) fetchAuditLogs(acc telegraf.Accumulator) error {
	p.Log.Debug("Sending Audit Log Request")

	request, err := http.NewRequest("GET", p.auditLogUrl(), nil)

	if err != nil {
		return err
	}

	request.Header.Set("Accept", "application/vnd.pulumi+8")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", fmt.Sprintf("token %s", p.Token))

	resp, err := p.client.Do(request)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	bytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		var apiErrorResponse ApiError
		err = json.Unmarshal(bytes, &apiErrorResponse)

		if err != nil {
			// Ruhoh
			return err
		}

		return fmt.Errorf("error code %d: %s", apiErrorResponse.Code, apiErrorResponse.Message)
	}

	var auditLogsResponse AuditLogsResponse
	err = json.Unmarshal(bytes, &auditLogsResponse)

	if err != nil {
		return err
	}

	for _, auditLogEvent := range auditLogsResponse.AuditLogEvents {
		tags := map[string]string{
			"organization": p.Organization,
			"event":        auditLogEvent.Event,
			"user":         auditLogEvent.User.Name,
			"github_login": auditLogEvent.User.GitHubLogin,
			"source_ip":    auditLogEvent.SourceIP,
		}

		fields := map[string]interface{}{
			"payload": string(bytes),
		}

		p.Log.Debugf("Event with tags %v and fields %v", tags, fields)

		acc.AddFields("pulumi_api", fields, tags, time.Unix(auditLogEvent.Timestamp, 0))
	}

	if auditLogsResponse.ContinuationToken != 0 {
		p.Log.Info("Response was paginated, sending additional request with continuation token")

		p.continuationToken = auditLogsResponse.ContinuationToken
		p.fetchAuditLogs(acc)
	}

	p.Log.Debug("Finished fetching audit logs")
	return nil
}
