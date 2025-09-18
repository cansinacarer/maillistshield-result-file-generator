import pandas as pd

from app.utilities.logging import logger
from app.utilities.rabbitmq import QueueAgent
from app.utilities.database import save_and_get_results_file_name
from app.utilities.s3 import upload_df_to_s3


class ResultsFileGenerator:
    """
    Class to handle the generation of results files after job completion.

    Attributes:
        queue_name (str): The name of the RabbitMQ queue associated with the job.

    """

    def __init__(self, queue_name):
        # Store the queue name
        self.queue_name = queue_name

        # Initialize a QueueAgent
        self.queue_agent = QueueAgent()

    def build_results_file(self):

        # Allow the QueueAgent to fetch all messages without a limit
        self.queue_agent.channel.basic_qos(prefetch_count=0)

        # Get the message count in the queue
        self.all_messages = self.queue_agent.retrieve_all_messages_and_delete_queue(
            self.queue_name
        )

        # Get the job UID
        self.job_uid = self.queue_agent.get_job_uid(self.queue_name)

        # Delete the queue after retrieving messages
        self.queue_agent.delete_queue(self.queue_name)

        print(f"Retrieved {len(self.all_messages)} messages from {self.queue_name}.")
        print(self.all_messages)

        # Convert messages to a DataFrame
        self.results_df = pd.DataFrame(self.all_messages)

        # Reorder results_df columns
        self.results_df = self.results_df.reindex(
            columns=[
                "email",
                "status",
                "status_detail",
                "email_provider",
                "email_security_gateway",
                "is_disposable",
                "is_free_provider",
                "has_mx_records",
                "has_catch_all",
                "is_role",
                "is_valid_syntax",
                "is_likely_spam_trap",
                "is_mailbox_full",
                "account",
                "account_alias_stripped",
                "domain",
                "domain_age",
                "email_alias_stripped",
                "fqdn",
                "is_alias",
                "smtp_provider_host",
                "smtp_provider_host_domain",
                "smtp_provider_host_tld",
                "smtp_provider_ip",
                "smtp_provider_ip_ptr",
                "subdomain",
                "tld",
            ]
        )

        # Save the results file S3 key to the database and get that key
        self.results_file_key = save_and_get_results_file_name(self.job_uid)

        # Upload the results DataFrame to S3
        upload_df_to_s3(self.results_df, self.results_file_key)

    def requeue_messages(self):
        # Recreate the queue
        self.queue_agent.create_queue(
            self.queue_name,
            arguments={"jobuid": self.job_uid, "row_count": len(self.all_messages)},
        )

        # Re-enqueue all messages
        for message in self.all_messages:
            # We don't want to pass the delivery_tag from the old queue
            message.pop("delivery_tag")

            # Enqueue the message again
            self.queue_agent.publish_message(self.queue_name, message)
