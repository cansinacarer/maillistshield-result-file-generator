from app.utilities.rabbitmq import QueueAgent
from app.utilities.logging import logger
from app.utilities.database import set_job_row_progress, set_job_status_via_uid


def queue_monitor():
    """
    Monitors the RabbitMQ queues to determine validation progresses of each file.
    """

    # Initialize the QueueAgent
    queue_agent = QueueAgent()

    # Fetch and log details of all queues
    queues = queue_agent.list_all_queues()

    # If no queues found, log and return, otherwise log the number of queues found
    if not queues or len(queues) == 0:
        logger.debug("No queues found.")
        return
    logger.debug(f"Found {len(queues)} queues pending file build.")

    # Monitor each queue in the vhost for queues pending file builds
    for queue in queues:
        current_message_count = queue_agent.get_message_counts(queue)["ready"]
        expected_message_count = queue_agent.get_expected_message_count(queue)

        # Get job UID from queue args
        job_uid = queue_agent.get_job_uid(queue)

        if current_message_count < expected_message_count:
            logger.debug(
                f"Queue {queue} has {current_message_count} of the expected {expected_message_count} messages ready."
            )

            # Update the status in the database
            # It is redundant to set this at every poll, but cheaper than checking the status to decide
            set_job_status_via_uid(job_uid, "file_validation_in_progress")
        else:
            logger.info(
                f"Queue {queue} has reached the expected message count of {expected_message_count}."
            )

            # TODO: Actually export the file and set results file S3 key in DB

            # Update the status in the database to indicate processing is complete
            set_job_status_via_uid(job_uid, "file_completed")

        # Update the last_pick_row in the database either way
        set_job_row_progress(job_uid, current_message_count)
