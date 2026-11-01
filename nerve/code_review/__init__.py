"""Persistent pull request review controller."""

_service = None


def get_service():
    return _service


def set_service(service):
    global _service
    _service = service
