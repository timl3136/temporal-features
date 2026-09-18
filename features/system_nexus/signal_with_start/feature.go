package signal_with_start

import (
	"context"
	"fmt"
	"time"

	"github.com/temporalio/features/harness/go/harness"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/api/workflowservice/v1/workflowservicenexus"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/workflow"
)

const (
	systemEndpoint     = "__temporal_system"
	signalName         = "add"
	targetWorkflowName = "SystemNexusSignalWithStartTarget"
)

type CallerResult struct {
	TargetWorkflowID string
	TargetRunID      string
}

func supportsSystemNexusEndpoint() (supported bool) {
	defer func() {
		if recovered := recover(); recovered != nil {
			if fmt.Sprint(recovered) != "endpoint cannot use reserved __temporal_ prefix" {
				panic(recovered)
			}
			supported = false
		}
	}()
	workflow.NewNexusClient(
		systemEndpoint,
		workflowservicenexus.TemporalAPIWorkflowserviceV1WorkflowService.ServiceName,
	)
	return true
}

func CallerWorkflow(
	ctx workflow.Context,
	firstRequest *workflowservice.SignalWithStartWorkflowExecutionRequest,
	secondRequest *workflowservice.SignalWithStartWorkflowExecutionRequest,
) (CallerResult, error) {
	nexusClient := workflow.NewNexusClient(
		systemEndpoint,
		workflowservicenexus.TemporalAPIWorkflowserviceV1WorkflowService.ServiceName,
	)
	options := workflow.NexusOperationOptions{ScheduleToCloseTimeout: time.Minute}

	var first workflowservice.SignalWithStartWorkflowExecutionResponse
	if err := nexusClient.ExecuteOperation(
		ctx,
		workflowservicenexus.TemporalAPIWorkflowserviceV1WorkflowService.SignalWithStartWorkflowExecution,
		firstRequest,
		options,
	).Get(ctx, &first); err != nil {
		return CallerResult{}, err
	}
	if !first.GetStarted() {
		return CallerResult{}, fmt.Errorf("first SignalWithStart did not start the target")
	}
	if first.GetRunId() == "" {
		return CallerResult{}, fmt.Errorf("first SignalWithStart returned an empty run ID")
	}

	var second workflowservice.SignalWithStartWorkflowExecutionResponse
	if err := nexusClient.ExecuteOperation(
		ctx,
		workflowservicenexus.TemporalAPIWorkflowserviceV1WorkflowService.SignalWithStartWorkflowExecution,
		secondRequest,
		options,
	).Get(ctx, &second); err != nil {
		return CallerResult{}, err
	}
	if second.GetStarted() {
		return CallerResult{}, fmt.Errorf("second SignalWithStart unexpectedly started another target")
	}
	if second.GetRunId() != first.GetRunId() {
		return CallerResult{}, fmt.Errorf(
			"SignalWithStart calls targeted different runs: first=%q second=%q",
			first.GetRunId(),
			second.GetRunId(),
		)
	}

	return CallerResult{
		TargetWorkflowID: firstRequest.GetWorkflowId(),
		TargetRunID:      first.GetRunId(),
	}, nil
}

func TargetWorkflow(ctx workflow.Context, initialValue int) (int, error) {
	total := initialValue
	signals := workflow.GetSignalChannel(ctx, signalName)
	for range 2 {
		var value int
		signals.Receive(ctx, &value)
		total += value
	}
	return total, nil
}

func encodePayloads(value any) (*commonpb.Payloads, error) {
	return converter.GetDefaultDataConverter().ToPayloads(value)
}

func newRequest(
	workflowID string,
	taskQueue string,
	workflowInput *commonpb.Payloads,
	signalInput *commonpb.Payloads,
) *workflowservice.SignalWithStartWorkflowExecutionRequest {
	return &workflowservice.SignalWithStartWorkflowExecutionRequest{
		WorkflowId:   workflowID,
		WorkflowType: &commonpb.WorkflowType{Name: targetWorkflowName},
		TaskQueue:    &taskqueuepb.TaskQueue{Name: taskQueue},
		Input:        workflowInput,
		SignalName:   signalName,
		SignalInput:  signalInput,
	}
}

var Feature = harness.Feature{
	Workflows: []any{
		CallerWorkflow,
		harness.WorkflowWithOptions{
			Workflow: TargetWorkflow,
			Options:  workflow.RegisterOptions{Name: targetWorkflowName},
		},
	},
	Execute: func(ctx context.Context, runner *harness.Runner) (client.WorkflowRun, error) {
		if !supportsSystemNexusEndpoint() {
			return nil, runner.Skip("Go SDK does not expose the __temporal_system workflow Nexus endpoint")
		}

		workflowInput, err := encodePayloads(1)
		if err != nil {
			return nil, err
		}
		firstSignalInput, err := encodePayloads(20)
		if err != nil {
			return nil, err
		}
		secondSignalInput, err := encodePayloads(22)
		if err != nil {
			return nil, err
		}

		targetWorkflowID := "system-nexus-signal-with-start-" + runner.TaskQueue
		firstRequest := newRequest(targetWorkflowID, runner.TaskQueue, workflowInput, firstSignalInput)
		secondRequest := newRequest(targetWorkflowID, runner.TaskQueue, workflowInput, secondSignalInput)

		return runner.Client.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
			TaskQueue:                runner.TaskQueue,
			WorkflowExecutionTimeout: time.Minute,
		}, CallerWorkflow, firstRequest, secondRequest)
	},
	CheckResult: func(ctx context.Context, runner *harness.Runner, run client.WorkflowRun) error {
		var callerResult CallerResult
		if err := run.Get(ctx, &callerResult); err != nil {
			return err
		}
		runner.Require.NotEmpty(callerResult.TargetWorkflowID)
		runner.Require.NotEmpty(callerResult.TargetRunID)

		var targetResult int
		if err := runner.Client.GetWorkflow(
			ctx,
			callerResult.TargetWorkflowID,
			callerResult.TargetRunID,
		).Get(ctx, &targetResult); err != nil {
			return err
		}
		runner.Require.Equal(43, targetResult)
		return nil
	},
	CheckHistory: checkHistory,
}

func readHistory(
	ctx context.Context,
	runner *harness.Runner,
	workflowID string,
	runID string,
) ([]*historypb.HistoryEvent, error) {
	iterator := runner.Client.GetWorkflowHistory(
		ctx,
		workflowID,
		runID,
		false,
		enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT,
	)
	var events []*historypb.HistoryEvent
	for iterator.HasNext() {
		event, err := iterator.Next()
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, nil
}

func countEvent(events []*historypb.HistoryEvent, eventType enumspb.EventType) int {
	count := 0
	for _, event := range events {
		if event.GetEventType() == eventType {
			count++
		}
	}
	return count
}

func checkHistory(ctx context.Context, runner *harness.Runner, run client.WorkflowRun) error {
	var callerResult CallerResult
	if err := run.Get(ctx, &callerResult); err != nil {
		return err
	}

	callerEvents, err := readHistory(ctx, runner, run.GetID(), run.GetRunID())
	if err != nil {
		return err
	}
	runner.Require.Equal(2, countEvent(callerEvents, enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED))
	runner.Require.Equal(2, countEvent(callerEvents, enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED))
	runner.Require.Zero(countEvent(callerEvents, enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED))

	targetEvents, err := readHistory(
		ctx,
		runner,
		callerResult.TargetWorkflowID,
		callerResult.TargetRunID,
	)
	if err != nil {
		return err
	}
	if len(targetEvents) < 2 {
		return fmt.Errorf("target history has only %d events", len(targetEvents))
	}
	runner.Require.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, targetEvents[0].GetEventType())
	runner.Require.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED, targetEvents[1].GetEventType())
	runner.Require.Equal(2, countEvent(targetEvents, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED))
	return nil
}
