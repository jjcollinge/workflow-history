package workflows

import (
	"fmt"
	"time"

	"github.com/dapr/go-sdk/workflow"
)

// WorkflowExample represents a workflow example with metadata
type WorkflowExample struct {
	Name        string
	Description string
	WorkflowFn  workflow.Workflow
	Activities  []workflow.Activity
	Input       interface{}
}

// GetAllExamples returns all available workflow examples
func GetAllExamples() []WorkflowExample {
	return []WorkflowExample{
		{
			Name:        "simple",
			Description: "Simple workflow with activity and external event",
			WorkflowFn:  SimpleWorkflow,
			Activities:  []workflow.Activity{SimpleActivity},
			Input:       1,
		},
		{
			Name:        "chain",
			Description: "Sequential chain of activities with validation",
			WorkflowFn:  ChainWorkflow,
			Activities:  []workflow.Activity{ProcessStepActivity, ValidateActivity, NotifyActivity},
			Input:       "start",
		},
		{
			Name:        "fanout",
			Description: "Parallel processing with fan-out/fan-in pattern",
			WorkflowFn:  FanOutWorkflow,
			Activities:  []workflow.Activity{ProcessItemActivity, AggregateActivity},
			Input:       []string{"item1", "item2", "item3"},
		},
		{
			Name:        "timer",
			Description: "Workflow with timer delays for scheduling",
			WorkflowFn:  TimerWorkflow,
			Activities:  []workflow.Activity{TimerActivity},
			Input:       2 * time.Second,
		},
		{
			Name:        "parent",
			Description: "Parent workflow that orchestrates child workflows",
			WorkflowFn:  ParentWorkflow,
			Activities:  []workflow.Activity{ParentActivity},
			Input:       3,
		},
	}
}

// Simple workflow example
func SimpleWorkflow(ctx *workflow.WorkflowContext) (any, error) {
	var n int
	if err := ctx.GetInput(&n); err != nil {
		return nil, err
	}

	var out string
	if err := ctx.CallActivity(SimpleActivity, workflow.ActivityInput(n)).Await(&out); err != nil {
		return nil, err
	}

	// Wait for external event
	var evt string
	if err := ctx.WaitForExternalEvent("go", 30*time.Second).Await(&evt); err != nil {
		return nil, err
	}

	// Call activity again with incremented input
	if err := ctx.CallActivity(SimpleActivity, workflow.ActivityInput(n+1)).Await(&out); err != nil {
		return nil, err
	}

	// Add an additional activity call to change the pattern
	if err := ctx.CallActivity(SimpleActivity, workflow.ActivityInput(n+2)).Await(&out); err != nil {
		return nil, err
	}

	return out, nil
}

func SimpleActivity(ctx workflow.ActivityContext) (any, error) {
	var n int
	if err := ctx.GetInput(&n); err != nil {
		return nil, err
	}
	return fmt.Sprintf("simple:%d @ %s", n, time.Now().Format(time.RFC3339Nano)), nil
}

// Task chaining example
func ChainWorkflow(ctx *workflow.WorkflowContext) (any, error) {
	var input string
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}

	// Step 1: Process
	var step1Result string
	if err := ctx.CallActivity(ProcessStepActivity, workflow.ActivityInput(input)).Await(&step1Result); err != nil {
		return nil, err
	}

	// Step 2: Validate
	var step2Result string
	if err := ctx.CallActivity(ValidateActivity, workflow.ActivityInput(step1Result)).Await(&step2Result); err != nil {
		return nil, err
	}

	// Step 3: Notify
	var finalResult string
	if err := ctx.CallActivity(NotifyActivity, workflow.ActivityInput(step2Result)).Await(&finalResult); err != nil {
		return nil, err
	}

	return finalResult, nil
}

func ProcessStepActivity(ctx workflow.ActivityContext) (any, error) {
	var input string
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}
	return fmt.Sprintf("processed-%s", input), nil
}

func ValidateActivity(ctx workflow.ActivityContext) (any, error) {
	var input string
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}
	return fmt.Sprintf("validated-%s", input), nil
}

func NotifyActivity(ctx workflow.ActivityContext) (any, error) {
	var input string
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}
	return fmt.Sprintf("notified-%s @ %s", input, time.Now().Format("15:04:05")), nil
}

// Fan-out/fan-in example
func FanOutWorkflow(ctx *workflow.WorkflowContext) (any, error) {
	var items []string
	if err := ctx.GetInput(&items); err != nil {
		return nil, err
	}

	// Fan-out: process all items in parallel using multiple activity calls
	results := make([]string, len(items))
	for i, item := range items {
		var result string
		if err := ctx.CallActivity(ProcessItemActivity, workflow.ActivityInput(item)).Await(&result); err != nil {
			return nil, err
		}
		results[i] = result
	}

	// Fan-in: aggregate results
	var aggregatedResult string
	if err := ctx.CallActivity(AggregateActivity, workflow.ActivityInput(results)).Await(&aggregatedResult); err != nil {
		return nil, err
	}

	return aggregatedResult, nil
}

func ProcessItemActivity(ctx workflow.ActivityContext) (any, error) {
	var item string
	if err := ctx.GetInput(&item); err != nil {
		return nil, err
	}
	// Simulate some work
	time.Sleep(100 * time.Millisecond)
	return fmt.Sprintf("processed-%s", item), nil
}

func AggregateActivity(ctx workflow.ActivityContext) (any, error) {
	var results []string
	if err := ctx.GetInput(&results); err != nil {
		return nil, err
	}
	return fmt.Sprintf("aggregated %d items: %v", len(results), results), nil
}

// Timer and retry example
func TimerWorkflow(ctx *workflow.WorkflowContext) (any, error) {
	var waitDuration time.Duration
	if err := ctx.GetInput(&waitDuration); err != nil {
		return nil, err
	}

	// Create a timer
	var timerResult interface{}
	if err := ctx.CreateTimer(waitDuration).Await(&timerResult); err != nil {
		return nil, err
	}

	// Try activity with retry
	var result string
	if err := ctx.CallActivity(TimerActivity, workflow.ActivityInput("timer-task")).Await(&result); err != nil {
		return nil, err
	}

	return result, nil
}

func TimerActivity(ctx workflow.ActivityContext) (any, error) {
	var task string
	if err := ctx.GetInput(&task); err != nil {
		return nil, err
	}
	return fmt.Sprintf("timer-completed-%s @ %s", task, time.Now().Format("15:04:05")), nil
}

// Parent-child workflow example
func ParentWorkflow(ctx *workflow.WorkflowContext) (any, error) {
	var input int
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}

	// Call parent activity
	var parentResult string
	inputStr := fmt.Sprintf("parent-task-%d", input)
	if err := ctx.CallActivity(ParentActivity, workflow.ActivityInput(inputStr)).Await(&parentResult); err != nil {
		return nil, err
	}

	// Call child workflow
	var childResult string
	if err := ctx.CallChildWorkflow(ChildWorkflow, workflow.ChildWorkflowInput(parentResult)).Await(&childResult); err != nil {
		return nil, err
	}

	return fmt.Sprintf("parent completed with child result: %s", childResult), nil
}

func ParentActivity(ctx workflow.ActivityContext) (any, error) {
	var input string
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}
	return fmt.Sprintf("parent-processed-%s", input), nil
}

func ChildWorkflow(ctx *workflow.WorkflowContext) (any, error) {
	var input string
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}
	return fmt.Sprintf("child-completed-%s", input), nil
}
