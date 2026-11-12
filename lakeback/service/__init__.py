"""Read-only loopback HTTP query service."""

from lakeback.service.app import LakeBackHTTPServer, make_server

__all__ = ["LakeBackHTTPServer", "make_server"]
