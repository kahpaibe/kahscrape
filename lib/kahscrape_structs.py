import asyncio
from abc import abstractmethod, ABC
from typing import Callable, Awaitable
from aiohttp import ClientResponse
# from enum import StrEnum

class FetcherABC(ABC):
    """Make http requests."""
    def __init__(self) -> None:
        super().__init__()
        self.lock = asyncio.Lock()

    @abstractmethod
    async def fetch(self, 
                    url: str, 
                    callback: Callable[["FetcherABC", ClientResponse, bytes], Awaitable[None]], # (fetcher, response, data)
                    onerror: Callable[["FetcherABC", str, Exception, ClientResponse | None, bytes | None], Awaitable[None]], # (fetcher, url, e, [Optional response, Optional data])
                    ) -> None:
        """Request given url."""
        raise NotImplementedError()
        # Should support ToretryException

    async def fetch_now(self, 
                        url: str,
                        onerror: Callable[["FetcherABC", str, Exception, ClientResponse | None, bytes | None], Awaitable[None]], # (fetcher, url, e, [Optional response, Optional data])
                        ) -> tuple[ClientResponse, bytes] | None:
        """Fetch given url now"""
    
    @abstractmethod
    async def close(self) -> None:
        """Close the fetcher."""
        raise NotImplementedError()
    
    def fetch_non_async(self,
                        url: str, 
                        callback: Callable[["FetcherABC", ClientResponse, bytes], Awaitable[None]], # (fetcher, response, data)
                        onerror: Callable[["FetcherABC", str, Exception, ClientResponse | None, bytes | None], Awaitable[None]], # (fetcher, url, e, [Optional response, Optional data])
                        ) -> None:
        """Add fetch from non async context"""
        asyncio.run(self.fetch(url, callback, onerror))

class ToretryException(BaseException):
    """Exception to raise when pipeline fails, to ask the Fetcher to try to download the page again."""
    def __init__(self, attempt_count: int | None, *args: object) -> None:
        super().__init__(*args)
        self.attempt_count = attempt_count # Number of times the ressource was refetched

class ToskipException(BaseException):
    """Exception to raise when pipeline fails, to ask the Fetcher to skip the resource."""
    def __init__(self, *args: object) -> None:
        super().__init__(*args)

# class MediaTypes(StrEnum):
#     """Media types used for fetching."""
#     Text = 'Text'
#     Raw = "Raw"