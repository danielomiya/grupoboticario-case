import pendulum


def days_ago(n: int) -> pendulum.DateTime:
    """
    Used to get the DateTime of number of days in the past of present UTC time

    :param n: a number of days ago, from which the DateTime should be calculated
    :return: a pendulum.DateTime representing the specified datetime
    """
    return pendulum.today("UTC").subtract(days=n)
