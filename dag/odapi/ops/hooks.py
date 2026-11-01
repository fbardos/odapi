import dagster as dg


@dg.success_hook(required_resource_keys={'ntfy'})
def ntfy_on_success(context: dg.HookContext):
    context.resources.ntfy.send_success_message(context)


@dg.failure_hook(required_resource_keys={'ntfy'})
def ntfy_on_failure(context: dg.HookContext):
    context.resources.ntfy.send_failure_message(context)
