namespace system_nexus.signal_with_start;

using Temporalio.Api.Enums.V1;
using Temporalio.Client;
using Temporalio.Features.Harness;
using Temporalio.Worker;
using Temporalio.Workflows;

class Feature : IFeature
{
    [Workflow]
    class TargetWorkflow
    {
        private readonly List<string> signals = new();

        [WorkflowRun]
        public async Task<IReadOnlyCollection<string>> RunAsync(string value)
        {
            await Workflow.WaitConditionAsync(() => signals.Count == 2);
            return [$"started: {value}", .. signals];
        }

        [WorkflowSignal]
        public Task AddAsync(string value)
        {
            signals.Add($"signal: {value}");
            return Task.CompletedTask;
        }
    }

    [Workflow]
    class CallerWorkflow
    {
        [WorkflowRun]
        public async Task<string> RunAsync(string targetId, string taskQueue)
        {
            await Workflow.SignalWithStartWorkflowAsync(
                (TargetWorkflow workflow) => workflow.RunAsync("start-value"),
                workflow => workflow.AddAsync("signal-one"),
                new(targetId, taskQueue)
                {
                    IdConflictPolicy = WorkflowIdConflictPolicy.UseExisting,
                });
            await Workflow.SignalWithStartWorkflowAsync(
                (TargetWorkflow workflow) => workflow.RunAsync("unused-start-value"),
                workflow => workflow.AddAsync("signal-two"),
                new(targetId, taskQueue)
                {
                    IdConflictPolicy = WorkflowIdConflictPolicy.UseExisting,
                });
            return targetId;
        }
    }

    public void ConfigureWorker(Runner runner, TemporalWorkerOptions options) =>
        options
            .AddWorkflow<CallerWorkflow>()
            .AddWorkflow<TargetWorkflow>();

    public async Task<WorkflowHandle?> ExecuteAsync(Runner runner)
    {
        var targetId = $"{runner.PreparedFeature.Dir}-target";
        return await runner.Client.StartWorkflowAsync(
            (CallerWorkflow workflow) =>
                workflow.RunAsync(targetId, runner.WorkerOptions.TaskQueue!),
            runner.NewWorkflowOptions());
    }

    public async Task CheckResultAsync(Runner runner, WorkflowHandle handle)
    {
        var targetId = await handle.GetResultAsync<string>();
        var target = runner.Client.GetWorkflowHandle<
            TargetWorkflow,
            IReadOnlyCollection<string>>(targetId);
        Assert.Equal(
            new[] { "started: start-value", "signal: signal-one", "signal: signal-two" },
            await target.GetResultAsync());
    }
}
