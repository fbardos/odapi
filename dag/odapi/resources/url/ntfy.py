import requests
from dagster import ConfigurableResource
from dagster import HookContext


class NtfyResource(ConfigurableResource):
    server_url: str
    api_token: str
    topic: str

    @property
    def topic_url(self) -> str:
        return f'{self.server_url}/{self.topic}'

    def send_message(self, message: str, **kwargs) -> None:
        try:
            response = requests.post(
                self.topic_url,
                data=message,
                headers={
                    'Authorization': f'Bearer {self.api_token}',
                    **kwargs,
                },
            )
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            raise Exception(f"Failed to send message: {e}")

    def send_failure_message(self, context: HookContext) -> None:
        self.send_message(
            Title='FAILURE in ODAPI pipeline',
            message=f"Job: *{context.job_name}*\nRun: *{context.run_id}*",
            Priority='high',
            Tags='warning',
            Markdown='yes',
        )

    def send_success_message(self, context: HookContext) -> None:
        self.send_message(
            Title='SUCCESS in ODAPI pipeline',
            message=f"Job: *{context.job_name}*\nRun: *{context.run_id}*",
            Tags='white_check_mark',
            Markdown='yes',
        )
