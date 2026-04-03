import crontab
import dateutil.tz

import rq_scheduler.scheduler
import rq_scheduler.utils
from rq_scheduler.scripts.rqscheduler import main


def _get_next_scheduled_time(cron_string: str, use_local_timezone: bool = False):
    tz = dateutil.tz.tzlocal() if use_local_timezone else dateutil.tz.UTC
    now = datetime.now(tz)
    cron = crontab.CronTab(cron_string)
    next_time = cron.next(now=now, return_datetime=True, default_utc=True)
    return next_time.astimezone(tz)


from datetime import datetime


rq_scheduler.utils.get_next_scheduled_time = _get_next_scheduled_time
rq_scheduler.scheduler.get_next_scheduled_time = _get_next_scheduled_time


if __name__ == "__main__":
    main()
