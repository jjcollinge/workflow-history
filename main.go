package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/go-sdk/workflow"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/jjcollinge/workflow-history/workflows"
)

const (
	defaultDaprHTTPPort = "3500"
	defaultNamespace    = "default"
	defaultAppID        = "workflow-history"
	waitTimeout         = 30 * time.Second
	historyDelay        = 200 * time.Millisecond
	httpTimeout         = 5 * time.Second
	maxHistoryEntries   = 100000
	historyFileName     = "workflow-hashes.json"
)

// Result represents the final output JSON structure for a single workflow
type WorkflowResult struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	InstanceID  string          `json:"instance_id"`
	Hash        string          `json:"hash"`
	History     json.RawMessage `json:"history"`
}

// Results represents the complete output containing all workflow results
type Results struct {
	Examples []WorkflowResult `json:"examples"`
}

// HashOnlyResults represents just the hashes for disk storage
type HashOnlyResults struct {
	Examples []WorkflowHash `json:"examples"`
}

// WorkflowHash represents just the hash information for validation
type WorkflowHash struct {
	Name string `json:"name"`
	Hash string `json:"hash"`
}

// Config holds configuration values from environment variables
type Config struct {
	AppID     string
	Namespace string
	HTTPPort  string
}

// HTTPError represents an HTTP error response
type HTTPError struct {
	StatusCode int
}

func (e HTTPError) Error() string {
	return fmt.Sprintf("http status %d", e.StatusCode)
}

func main() {
	ctx := context.Background()
	config := NewConfig()

	if err := runWorkflow(ctx, config); err != nil {
		log.Fatalf("workflow execution failed: %v", err)
	}
}

func NewConfig() Config {
	return Config{
		AppID:     getenvWithDefault([]string{"APP_ID", "DAPR_APP_ID"}, defaultAppID),
		Namespace: getenvWithDefault([]string{"DAPR_NAMESPACE"}, defaultNamespace),
		HTTPPort:  getenvWithDefault([]string{"DAPR_HTTP_PORT"}, defaultDaprHTTPPort),
	}
}

func runWorkflow(ctx context.Context, config Config) error {
	log.Println("Initializing workflow worker...")
	worker, err := initializeWorker()
	if err != nil {
		return fmt.Errorf("failed to initialize worker: %w", err)
	}
	defer func() { _ = worker.Shutdown() }()

	// Give the worker a moment to fully initialize
	log.Println("Waiting for worker to initialize...")
	time.Sleep(2 * time.Second)

	log.Println("Creating workflow client...")
	client, err := workflow.NewClient()
	if err != nil {
		return fmt.Errorf("failed to create workflow client: %w", err)
	}
	defer client.Close()

	examples := workflows.GetAllExamples()
	results := make([]WorkflowResult, 0, len(examples))

	log.Printf("Executing %d workflow examples...", len(examples))
	for i, example := range examples {
		log.Printf("Starting workflow %d/%d: %s", i+1, len(examples), example.Name)
		instanceID := fmt.Sprintf("wf-%s-%s", strings.ToLower(strings.ReplaceAll(example.Name, " ", "-")), time.Now().Format("150405"))

		if err := executeWorkflowExample(ctx, client, example, instanceID); err != nil {
			return fmt.Errorf("failed to execute %s workflow: %w", example.Name, err)
		}

		log.Printf("Getting history for workflow: %s", example.Name)
		result, err := outputWorkflowHistory(ctx, config, example, instanceID)
		if err != nil {
			return fmt.Errorf("failed to get history for %s workflow: %w", example.Name, err)
		}

		results = append(results, result)
		log.Printf("Completed workflow %d/%d: %s", i+1, len(examples), example.Name)
	}

	log.Println("All workflows completed, generating output...")
	finalResults := Results{Examples: results}
	output, err := json.MarshalIndent(finalResults, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal results: %w", err)
	}

	// Print the full history to stdout first
	fmt.Println(string(output))

	// Handle file operations for history validation
	if err := handleHistoryFile(output); err != nil {
		return fmt.Errorf("failed to handle history file: %w", err)
	}

	return nil
}

func initializeWorker() (interface{ Shutdown() error }, error) {
	worker, err := workflow.NewWorker()
	if err != nil {
		return nil, err
	}

	// Register all workflows and activities from examples
	examples := workflows.GetAllExamples()
	for _, example := range examples {
		if err := worker.RegisterWorkflow(example.WorkflowFn); err != nil {
			return nil, fmt.Errorf("failed to register workflow %s: %w", example.Name, err)
		}

		for _, activity := range example.Activities {
			if err := worker.RegisterActivity(activity); err != nil {
				return nil, fmt.Errorf("failed to register activity for %s: %w", example.Name, err)
			}
		}
	}

	// Also register the child workflow used by parent workflow
	if err := worker.RegisterWorkflow(workflows.ChildWorkflow); err != nil {
		return nil, fmt.Errorf("failed to register ChildWorkflow: %w", err)
	}

	if err := worker.Start(); err != nil {
		return nil, err
	}

	return worker, nil
}

func executeWorkflowExample(ctx context.Context, client *workflow.Client, example workflows.WorkflowExample, instanceID string) error {
	// Map example names to actual workflow function names
	workflowNames := map[string]string{
		"simple": "SimpleWorkflow",
		"chain":  "ChainWorkflow",
		"fanout": "FanOutWorkflow",
		"timer":  "TimerWorkflow",
		"parent": "ParentWorkflow",
	}

	workflowName, exists := workflowNames[example.Name]
	if !exists {
		return fmt.Errorf("unknown workflow name: %s", example.Name)
	}

	log.Printf("Scheduling workflow: %s (instance: %s)", workflowName, instanceID)
	if _, err := client.ScheduleNewWorkflow(ctx, workflowName,
		workflow.WithInstanceID(instanceID),
		workflow.WithInput(example.Input),
		workflow.WithStartTime(time.Now()),
	); err != nil {
		return fmt.Errorf("failed to schedule workflow: %w", err)
	}

	log.Printf("Waiting for workflow to start: %s", instanceID)
	if _, err := client.WaitForWorkflowStart(ctx, instanceID, workflow.WithFetchPayloads(true)); err != nil {
		return fmt.Errorf("failed to wait for workflow start: %w", err)
	}

	// Only send external event for simple workflow
	if example.Name == "simple" {
		log.Printf("Sending external event to workflow: %s", instanceID)
		if err := client.RaiseEvent(ctx, instanceID, "go", workflow.WithEventPayload("ok")); err != nil {
			return fmt.Errorf("failed to raise event: %w", err)
		}
	}

	log.Printf("Waiting for workflow to complete: %s", instanceID)
	if _, err := client.WaitForWorkflowCompletion(ctx, instanceID, workflow.WithFetchPayloads(true)); err != nil {
		return fmt.Errorf("failed to wait for workflow completion: %w", err)
	}

	log.Printf("Workflow completed successfully: %s", instanceID)
	return nil
}

func outputWorkflowHistory(ctx context.Context, config Config, example workflows.WorkflowExample, instanceID string) (WorkflowResult, error) {
	time.Sleep(historyDelay)

	events, err := fetchHistoryViaActorAPI(ctx, config.HTTPPort, config.AppID, config.Namespace, instanceID)
	if err != nil {
		return WorkflowResult{}, fmt.Errorf("failed to fetch history: %w", err)
	}

	result, err := createWorkflowResult(example.Name, example.Description, instanceID, events)
	if err != nil {
		return WorkflowResult{}, fmt.Errorf("failed to create result: %w", err)
	}

	return result, nil
}

func createWorkflowResult(name, description, instanceID string, events []*protos.HistoryEvent) (WorkflowResult, error) {
	// Create a deterministic signature by grouping and sorting events logically
	signature := createDeterministicSignature(events)

	h := sha256.Sum256([]byte(signature))
	hashHex := hex.EncodeToString(h[:])

	marshaler := protojson.MarshalOptions{EmitUnpopulated: true}
	historyJSON, err := marshaler.Marshal(&protos.HistoryChunk{Events: events})
	if err != nil {
		return WorkflowResult{}, fmt.Errorf("failed to marshal history: %w", err)
	}

	return WorkflowResult{
		Name:        name,
		Description: description,
		InstanceID:  instanceID,
		Hash:        hashHex,
		History:     historyJSON,
	}, nil
}

func createDeterministicSignature(events []*protos.HistoryEvent) string {
	// Group events by type and sort them for deterministic ordering
	var execStart, taskSched, taskComp, eventRaised, timerEvents, orchEvents, execComp []string

	for _, event := range events {
		switch eventType := event.GetEventType().(type) {
		case *protos.HistoryEvent_ExecutionStarted:
			execStart = append(execStart, fmt.Sprintf("ExecutionStarted:%s", eventType.ExecutionStarted.GetName()))
		case *protos.HistoryEvent_TaskScheduled:
			taskSched = append(taskSched, fmt.Sprintf("TaskScheduled:%s", eventType.TaskScheduled.GetName()))
		case *protos.HistoryEvent_TaskCompleted:
			taskComp = append(taskComp, fmt.Sprintf("TaskCompleted:%d", eventType.TaskCompleted.GetTaskScheduledId()))
		case *protos.HistoryEvent_TaskFailed:
			taskComp = append(taskComp, fmt.Sprintf("TaskFailed:%d", eventType.TaskFailed.GetTaskScheduledId()))
		case *protos.HistoryEvent_EventRaised:
			eventRaised = append(eventRaised, fmt.Sprintf("EventRaised:%s", eventType.EventRaised.GetName()))
		case *protos.HistoryEvent_TimerCreated:
			timerEvents = append(timerEvents, "TimerCreated")
		case *protos.HistoryEvent_TimerFired:
			timerEvents = append(timerEvents, fmt.Sprintf("TimerFired:%d", eventType.TimerFired.GetTimerId()))
		case *protos.HistoryEvent_OrchestratorStarted:
			orchEvents = append(orchEvents, "OrchestratorStarted")
		case *protos.HistoryEvent_OrchestratorCompleted:
			orchEvents = append(orchEvents, "OrchestratorCompleted")
		case *protos.HistoryEvent_ExecutionCompleted:
			execComp = append(execComp, fmt.Sprintf("ExecutionCompleted:%s", eventType.ExecutionCompleted.GetOrchestrationStatus().String()))
		case *protos.HistoryEvent_SubOrchestrationInstanceCreated:
			taskSched = append(taskSched, fmt.Sprintf("SubOrchestrationInstanceCreated:%s", eventType.SubOrchestrationInstanceCreated.GetName()))
		case *protos.HistoryEvent_SubOrchestrationInstanceCompleted:
			taskComp = append(taskComp, fmt.Sprintf("SubOrchestrationInstanceCompleted:%d", eventType.SubOrchestrationInstanceCompleted.GetTaskScheduledId()))
		}
	}

	// Sort each group internally for consistency
	sort.Strings(execStart)
	sort.Strings(taskSched)
	sort.Strings(taskComp)
	sort.Strings(eventRaised)
	sort.Strings(timerEvents)
	sort.Strings(orchEvents)
	sort.Strings(execComp)

	// Combine in logical execution order
	var allEvents []string
	allEvents = append(allEvents, execStart...)
	allEvents = append(allEvents, taskSched...)
	allEvents = append(allEvents, taskComp...)
	allEvents = append(allEvents, eventRaised...)
	allEvents = append(allEvents, timerEvents...)
	allEvents = append(allEvents, orchEvents...)
	allEvents = append(allEvents, execComp...)

	return strings.Join(allEvents, ",")
}

func getEventSignature(event *protos.HistoryEvent) string {
	switch eventType := event.GetEventType().(type) {
	case *protos.HistoryEvent_ExecutionStarted:
		// Include workflow name for deterministic signature
		return fmt.Sprintf("ExecutionStarted:%s", eventType.ExecutionStarted.GetName())
	case *protos.HistoryEvent_TaskScheduled:
		// Include task name only, not event ID which can vary due to timing
		return fmt.Sprintf("TaskScheduled:%s", eventType.TaskScheduled.GetName())
	case *protos.HistoryEvent_TaskCompleted:
		// Include task scheduled ID for deterministic correlation
		return fmt.Sprintf("TaskCompleted:%d", eventType.TaskCompleted.GetTaskScheduledId())
	case *protos.HistoryEvent_TaskFailed:
		// Include task scheduled ID for deterministic correlation
		return fmt.Sprintf("TaskFailed:%d", eventType.TaskFailed.GetTaskScheduledId())
	case *protos.HistoryEvent_EventRaised:
		// Include event name for deterministic signature
		return fmt.Sprintf("EventRaised:%s", eventType.EventRaised.GetName())
	case *protos.HistoryEvent_TimerCreated:
		// Just the event type, timer creation is deterministic by workflow logic
		return "TimerCreated"
	case *protos.HistoryEvent_TimerFired:
		// Include timer ID for deterministic correlation
		return fmt.Sprintf("TimerFired:%d", eventType.TimerFired.GetTimerId())
	case *protos.HistoryEvent_OrchestratorStarted:
		return "OrchestratorStarted"
	case *protos.HistoryEvent_OrchestratorCompleted:
		return "OrchestratorCompleted"
	case *protos.HistoryEvent_ExecutionCompleted:
		// Include completion status for deterministic signature
		return fmt.Sprintf("ExecutionCompleted:%s", eventType.ExecutionCompleted.GetOrchestrationStatus().String())
	case *protos.HistoryEvent_SubOrchestrationInstanceCreated:
		// Include sub-workflow name for deterministic signature
		return fmt.Sprintf("SubOrchestrationInstanceCreated:%s", eventType.SubOrchestrationInstanceCreated.GetName())
	case *protos.HistoryEvent_SubOrchestrationInstanceCompleted:
		// Include task scheduled ID for deterministic correlation
		return fmt.Sprintf("SubOrchestrationInstanceCompleted:%d", eventType.SubOrchestrationInstanceCompleted.GetTaskScheduledId())
	default:
		return "Other"
	}
}

func getEventTypeName(event *protos.HistoryEvent) string {
	switch event.GetEventType().(type) {
	case *protos.HistoryEvent_ExecutionStarted:
		return "ExecutionStarted"
	case *protos.HistoryEvent_TaskScheduled:
		return "TaskScheduled"
	case *protos.HistoryEvent_TaskCompleted:
		return "TaskCompleted"
	case *protos.HistoryEvent_TaskFailed:
		return "TaskFailed"
	case *protos.HistoryEvent_EventRaised:
		return "EventRaised"
	case *protos.HistoryEvent_TimerCreated:
		return "TimerCreated"
	case *protos.HistoryEvent_TimerFired:
		return "TimerFired"
	case *protos.HistoryEvent_OrchestratorStarted:
		return "OrchestratorStarted"
	case *protos.HistoryEvent_OrchestratorCompleted:
		return "OrchestratorCompleted"
	case *protos.HistoryEvent_ExecutionCompleted:
		return "ExecutionCompleted"
	default:
		return "Other"
	}
}

func fetchHistoryViaActorAPI(ctx context.Context, httpPort, appID, namespace, instanceID string) ([]*protos.HistoryEvent, error) {
	actorType := fmt.Sprintf("dapr.internal.%s.%s.workflow", namespace, appID)
	baseURL := fmt.Sprintf("http://127.0.0.1:%s/v1.0/actors/%s/%s/state/", httpPort, url.PathEscape(actorType), url.PathEscape(instanceID))

	client := &http.Client{Timeout: httpTimeout}
	events := make([]*protos.HistoryEvent, 0)

	for start := 0; start <= 1 && len(events) == 0; start++ {
		foundAny := false
		for i := start; i < maxHistoryEntries; i++ {
			key := fmt.Sprintf("history-%06d", i)
			data, err := httpGet(ctx, client, baseURL+url.PathEscape(key))
			if err != nil {
				var httpErr HTTPError
				if errors.As(err, &httpErr) {
					if httpErr.StatusCode == http.StatusNoContent || httpErr.StatusCode == http.StatusNotFound {
						if foundAny {
							break
						}
						break
					}
				}
				return nil, fmt.Errorf("GET %s: %w", key, err)
			}

			if len(data) == 0 {
				if foundAny {
					break
				}
				break
			}

			event, err := decodeHistoryEvent(data)
			if err != nil {
				return nil, fmt.Errorf("decode %s: %w", key, err)
			}

			events = append(events, event)
			foundAny = true
		}
	}

	return events, nil
}

func decodeHistoryEvent(data []byte) (*protos.HistoryEvent, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty value")
	}

	var event protos.HistoryEvent

	if err := protojson.Unmarshal(data, &event); err == nil {
		return &event, nil
	}

	if unquoted, err := unquoteJSON(data); err == nil {
		if err := protojson.Unmarshal([]byte(unquoted), &event); err == nil {
			return &event, nil
		}
	}

	if err := proto.Unmarshal(data, &event); err == nil {
		return &event, nil
	}

	return nil, fmt.Errorf("unable to decode history event (len=%d)", len(data))
}

func httpGet(ctx context.Context, client *http.Client, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusNotFound {
		return nil, HTTPError{StatusCode: resp.StatusCode}
	}

	if resp.StatusCode != http.StatusOK {
		return nil, HTTPError{StatusCode: resp.StatusCode}
	}

	return io.ReadAll(resp.Body)
}

func unquoteJSON(data []byte) (string, error) {
	if len(data) >= 2 && data[0] == '"' && data[len(data)-1] == '"' {
		return strconv.Unquote(string(data))
	}
	return "", fmt.Errorf("not a quoted JSON string")
}

func handleHistoryFile(currentHistory []byte) error {
	// Parse current history to extract hashes
	var current Results
	if err := json.Unmarshal(currentHistory, &current); err != nil {
		return fmt.Errorf("failed to parse current history: %w", err)
	}

	// Create hash-only version for disk storage
	hashOnly := HashOnlyResults{
		Examples: make([]WorkflowHash, len(current.Examples)),
	}
	for i, example := range current.Examples {
		hashOnly.Examples[i] = WorkflowHash{
			Name: example.Name,
			Hash: example.Hash,
		}
	}

	// Check if history file already exists
	if _, err := os.Stat(historyFileName); os.IsNotExist(err) {
		// File doesn't exist, create it with just hashes
		log.Printf("Creating new history file: %s", historyFileName)
		hashData, err := json.MarshalIndent(hashOnly, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to marshal hash data: %w", err)
		}
		if err := os.WriteFile(historyFileName, hashData, 0644); err != nil {
			return fmt.Errorf("failed to write history file: %w", err)
		}
		log.Printf("History file created successfully")
		return nil
	} else if err != nil {
		return fmt.Errorf("failed to check if history file exists: %w", err)
	}

	// File exists, read and compare
	log.Printf("Reading existing history file: %s", historyFileName)
	existingData, err := os.ReadFile(historyFileName)
	if err != nil {
		return fmt.Errorf("failed to read existing history file: %w", err)
	}

	// Compare the hashes
	if err := compareHashes(existingData, hashOnly); err != nil {
		return fmt.Errorf("history validation failed: %w", err)
	}

	log.Printf("History validation passed - current execution matches existing file")
	return nil
}

func compareHashes(existingData []byte, currentHashes HashOnlyResults) error {
	var existing HashOnlyResults

	// Parse existing hash data
	if err := json.Unmarshal(existingData, &existing); err != nil {
		return fmt.Errorf("failed to parse existing hash data: %w", err)
	}

	// Compare number of examples
	if len(existing.Examples) != len(currentHashes.Examples) {
		return fmt.Errorf("number of workflow examples differs: existing=%d, current=%d",
			len(existing.Examples), len(currentHashes.Examples))
	}

	// Compare each example by hash (which represents the workflow execution pattern)
	for i, existingExample := range existing.Examples {
		currentExample := currentHashes.Examples[i]

		if existingExample.Name != currentExample.Name {
			return fmt.Errorf("workflow name mismatch at index %d: existing=%s, current=%s",
				i, existingExample.Name, currentExample.Name)
		}

		if existingExample.Hash != currentExample.Hash {
			return fmt.Errorf("workflow execution pattern changed for %s: existing hash=%s, current hash=%s",
				existingExample.Name, existingExample.Hash, currentExample.Hash)
		}
	}

	return nil
}

func getenvWithDefault(keys []string, defaultValue string) string {
	for _, key := range keys {
		if value := os.Getenv(key); value != "" {
			return value
		}
	}
	return defaultValue
}
