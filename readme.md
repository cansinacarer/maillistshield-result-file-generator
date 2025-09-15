# Mail List Shield - Results File Generator

This service monitors the results queues at vhost `RABBITMQ_DEFAULT_VHOSTS[1]` and when a queue at this vhost has the expected number of messages (i.e. `row_count` attribute of the queue), the messages from this queue are consumed and bundled into a final results file.

__Job States:__

- Expected before:
  - `file_queued`
- Error states
  - `error_?`
- Success state:
  - `file_completed`

---

See the [main repository](https://github.com/cansinacarer/maillistshield-com) for a complete list of other microservices.
