from abc import abstractmethod, ABC
from typing import Callable
# from enum import StrEnum
import asyncio

class FetcherABC(ABC):
    """Make http requests."""
    def __init__(self) -> None:
        super().__init__()
        self.lock = asyncio.Lock()

    @abstractmethod
    async def fetch(self, 
                      url: str, 
                      callback: Callable[[str, bytes], None], # (url, data)
                      onerror: Callable[[str, Exception], None], # (url, e)
                      ) -> None:
        """Request given url."""
        raise NotImplementedError()
    
    @abstractmethod
    async def close(self) -> None:
        """Close the fetcher."""
        raise NotImplementedError()
    
    def fetch_non_async(self,
                      url: str, 
                      callback: Callable[[str, bytes], None], # (url, data)
                      onerror: Callable[[str, Exception], None], # (url, e)
                      ) -> None:
        """Add fetch from non async context"""
        asyncio.run(self.fetch(url, callback, onerror))

# class MediaTypes(StrEnum):
#     """Media types used for fetching."""
#     Text = 'Text'
#     Raw = "Raw"