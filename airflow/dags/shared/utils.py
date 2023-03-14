import pendulum


def days_ago(n: int) -> pendulum.DateTime:
    return pendulum.today("UTC").subtract(days=n)
