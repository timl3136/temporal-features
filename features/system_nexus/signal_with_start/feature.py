from datetime import timedelta

from temporalio import workflow
from temporalio.client import WorkflowHandle
from temporalio.common import WorkflowIDConflictPolicy

from harness.python.feature import Runner, register_feature


@workflow.defn
class TargetWorkflow:
    def __init__(self) -> None:
        self.signals: list[str] = []

    @workflow.run
    async def run(self, value: str) -> list[str]:
        await workflow.wait_condition(lambda: len(self.signals) == 2)
        return [f"started: {value}", *self.signals]

    @workflow.signal
    def add(self, value: str) -> None:
        self.signals.append(f"signal: {value}")


@workflow.defn
class CallerWorkflow:
    @workflow.run
    async def run(self, target_id: str, task_queue: str) -> str:
        await workflow.signal_with_start_workflow(
            TargetWorkflow.run,
            "start-value",
            id=target_id,
            task_queue=task_queue,
            signal=TargetWorkflow.add,
            signal_args="signal-one",
            id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
        )
        await workflow.signal_with_start_workflow(
            TargetWorkflow.run,
            "unused-start-value",
            id=target_id,
            task_queue=task_queue,
            signal=TargetWorkflow.add,
            signal_args="signal-two",
            id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
        )
        return target_id


async def start(runner: Runner) -> WorkflowHandle:
    return await runner.client.start_workflow(
        CallerWorkflow.run,
        args=[f"{runner.feature.rel_dir}-target", runner.task_queue],
        id=f"{runner.feature.rel_dir}-caller",
        task_queue=runner.task_queue,
        execution_timeout=timedelta(minutes=1),
    )


async def check_result(runner: Runner, handle: WorkflowHandle) -> None:
    target_id = await handle.result()
    target = runner.client.get_workflow_handle(target_id)
    assert await target.result() == [
        "started: start-value",
        "signal: signal-one",
        "signal: signal-two",
    ]


register_feature(
    workflows=[CallerWorkflow, TargetWorkflow],
    start=start,
    check_result=check_result,
)
