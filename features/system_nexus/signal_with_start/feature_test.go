package signal_with_start

import (
	"testing"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/api/workflowservice/v1/workflowservicenexus"
	"go.temporal.io/sdk/testsuite"
)

func TestNewRequestUsesExistingRunAndOmitsNamespace(t *testing.T) {
	workflowInput := &commonpb.Payloads{}
	signalInput := &commonpb.Payloads{}

	request := newRequest("target-id", "task-queue", workflowInput, signalInput)

	require.Empty(t, request.GetNamespace())
	require.Equal(t, "target-id", request.GetWorkflowId())
	require.Equal(t, targetWorkflowName, request.GetWorkflowType().GetName())
	require.Equal(t, "task-queue", request.GetTaskQueue().GetName())
	require.Equal(t, signalName, request.GetSignalName())
	require.Same(t, workflowInput, request.GetInput())
	require.Same(t, signalInput, request.GetSignalInput())
	require.Equal(
		t,
		enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		request.GetWorkflowIdConflictPolicy(),
	)
}

func TestCallerWorkflowSerializesPointerOperationInput(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	firstCall := env.OnNexusOperation(
		workflowservicenexus.TemporalAPIWorkflowserviceV1WorkflowService.ServiceName,
		workflowservicenexus.TemporalAPIWorkflowserviceV1WorkflowService.SignalWithStartWorkflowExecution,
		mock.Anything,
		mock.Anything,
	).Return(
		&nexus.HandlerStartOperationResultSync[workflowservice.SignalWithStartWorkflowExecutionResponse]{
			Value: workflowservice.SignalWithStartWorkflowExecutionResponse{
				Started: true,
				RunId:   "test-run-id",
			},
		},
		nil,
	).Once()
	secondCall := env.OnNexusOperation(
		workflowservicenexus.TemporalAPIWorkflowserviceV1WorkflowService.ServiceName,
		workflowservicenexus.TemporalAPIWorkflowserviceV1WorkflowService.SignalWithStartWorkflowExecution,
		mock.Anything,
		mock.Anything,
	).Return(
		&nexus.HandlerStartOperationResultSync[workflowservice.SignalWithStartWorkflowExecutionResponse]{
			Value: workflowservice.SignalWithStartWorkflowExecutionResponse{
				RunId: "test-run-id",
			},
		},
		nil,
	).Once()
	env.InOrderMockCalls(firstCall, secondCall)

	env.ExecuteWorkflow(
		callerWorkflow,
		"test-endpoint",
		&workflowservice.SignalWithStartWorkflowExecutionRequest{WorkflowId: "test-workflow-id"},
		&workflowservice.SignalWithStartWorkflowExecutionRequest{WorkflowId: "test-workflow-id"},
	)

	require.NoError(t, env.GetWorkflowError())
	var result CallerResult
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, "test-workflow-id", result.TargetWorkflowID)
	require.Equal(t, "test-run-id", result.TargetRunID)
}
