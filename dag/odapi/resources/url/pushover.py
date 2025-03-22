import requests
from dagster import ConfigurableResource
from dagster import HookContext


class PushoverResource(ConfigurableResource):
    user_key: str
    api_token: str

    @property
    def MESSAGES_URL(self) -> str:
        return 'https://api.pushover.net/1/messages.json'

    def send_message(self, title: str, message: str, pushover_message_kwargs: dict) -> None:
        try:
            response = requests.post(
                self.MESSAGES_URL,
                data={
                    'token': self.api_token,
                    'user': self.user_key,
                    'title': title,
                    'message': message,
                    **pushover_message_kwargs,
                },
            )
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            raise Exception(f"Failed to send message: {e}")

    def send_failure_message(self, context: HookContext) -> None:
        self.send_message(
            title='FAILURE in ODAPI pipeline',
            message=(
                f"Job: <i>{context.job_name}</i>\n"
                f"Run: <i>{context.run_id}</i>"
            ),
            pushover_message_kwargs=dict(html=1),
        )
