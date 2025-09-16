import time

from app.config import SERVICE_NAME, POLLING_INTERVAL, PAUSE
from app.utilities.reporting import ping_uptime_monitor
from app.utilities.logging import logger
from app.queue_monitor import queue_monitor

while True:

    # Pause if env variable is set to pause
    if PAUSE:
        logger.info(
            "File to validation queue publisher is paused, change the environment variable `PAUSE` to resume it."
        )
        time.sleep(POLLING_INTERVAL)
        continue

    # Ping the uptime monitor
    ping_uptime_monitor()

    # Iterations start time
    start_time = time.time()

    # TODO: Business logic here
    queue_monitor()

    # Iteration end time
    end_time = time.time()
    elapsed_time = end_time - start_time
    # If the elapsed time is not as long as the polling interval, sleep until it is
    sleep_time = POLLING_INTERVAL - elapsed_time
    if sleep_time > 0:
        time.sleep(sleep_time)
