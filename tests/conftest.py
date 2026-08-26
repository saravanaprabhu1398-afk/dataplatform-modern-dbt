"""Pytest bootstrap helpers.

The project pins ``httpx<0.28`` for FastAPI/Starlette TestClient compatibility.
This shim keeps local test runs stable when a developer already has httpx 0.28+
installed in their user environment before reinstalling requirements.
"""
import inspect
from functools import wraps

import httpx


if "app" not in inspect.signature(httpx.Client.__init__).parameters:
    _httpx_client_init = httpx.Client.__init__

    @wraps(_httpx_client_init)
    def _client_init_compat(self, *args, **kwargs):
        kwargs.pop("app", None)
        return _httpx_client_init(self, *args, **kwargs)

    httpx.Client.__init__ = _client_init_compat
