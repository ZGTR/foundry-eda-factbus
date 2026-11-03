from factbus.domain.rules import event_keys


def route(envelope):
    return event_keys(envelope)


def paced(items, wait, sleep):
    """The conforming shape: the wait is computed elsewhere and blocking is injected."""
    for item in items:
        sleep(wait)
        yield item


def remote_user(request):
    """The conforming shape: a request's own `environ` is not the process environment."""
    return request.environ.get("REMOTE_USER")
