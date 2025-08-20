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

# class MediaTypes(StrEnum):
#     """Media types used for fetching."""
#     Text = 'Text'
#     Raw = "Raw"