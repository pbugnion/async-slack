from functools import lru_cache
import datetime

import workalendar.europe  # type: ignore


def date_range(start_date, end_date):
    """
    Return a generator for dates

    end_date is not inclusive.
    """
    for ndays in range((end_date - start_date).days):
        date = start_date + datetime.timedelta(days=ndays)
        yield date


@lru_cache(maxsize=1)
def _get_calendar():
    return workalendar.europe.UnitedKingdom()


def nworking_days_before(date: datetime.date, ndays: int) -> datetime.date:
    """ Return a date n working days before date """
    return _get_calendar().add_working_days(date, -ndays)
