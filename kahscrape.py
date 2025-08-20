import asyncio
import aiohttp
from aiohttp import ClientResponse, ClientSession
from asyncio import Queue, LifoQueue
from typing import Optional, Callable
from dataclasses import dataclass
from logging import Logger

from .lib.kahscrape_utils import get_domain
from .lib.kahscrape_structs import FetcherABC

from typing import override, Awaitable, Tuple

@dataclass
class KahReq:
    url: str
    callback: Callable[[FetcherABC, ClientResponse, bytes], Awaitable[None]] # (fetcher, response, data)
    onerror: Callable[[FetcherABC, str, Exception, ClientResponse | None, bytes | None], Awaitable[None]] # (fetcher, url, e, [Optional response, Optional data])

class KahBaseFetcher(FetcherABC):
    """Base async fetcher."""
    def __init__(self, 
                 num_workers: int,
                 timeout: float = 10.0,
                 session: Optional[ClientSession] = None,
                 logger: Optional[Logger] = None) -> None:
        """Base async fetcher.
        
        Args:
            num_workers (int): Number of worker threads.
            timeout (float): Timeout for fetch (for unset session).
            session (Optional[ClientSession]): Optional aiohttp ClientSession.
            logger (Optional[Logger]): Optional Logger.
        """
        super().__init__()
        self.logger = logger
        self.queue: Queue[KahReq | None] = LifoQueue() # Lifo allows limiting queue size due to branching
        self.session = session if session is not None else aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout))
        self.num_workers: int = num_workers
        self.running = True
        self._start_workers()

    def _start_workers(self):
        """Start the worker tasks."""
        for _ in range(self.num_workers):
            asyncio.create_task(self._worker())

    @override
    async def fetch(self, 
                    url: str, 
                    callback: Callable[[FetcherABC, ClientResponse, bytes], Awaitable[None]], # (fetcher, response, data)
                    onerror: Callable[[FetcherABC, str, Exception, ClientResponse | None, bytes | None], Awaitable[None]], # (fetcher, url, e, [Optional response, Optional data])
                    ) -> None:
        """Fetch given url."""
        if self.running is False: # Prevents further fetching
            if self.logger:
                self.logger.warning(f"Fetcher is not running, cannot add fetch request. {url=}")
            return
        if self.logger:
            self.logger.info(f"Adding new fetch to queue. {url=}")

        req = KahReq(url=url, callback=callback, onerror=onerror)
        await self.queue.put(req)

    async def fetch_now(self, 
                        url: str,
                        onerror: Callable[[FetcherABC, str, Exception, ClientResponse | None, bytes | None], Awaitable[None]], # (fetcher, url, e, [Optional response, Optional data])
                        ) -> Tuple[ClientResponse, bytes] | None:
        """Fetch given url without going to queue."""
        if self.running is False: # Prevents further fetching
            if self.logger:
                self.logger.warning(f"Fetcher is not running, cannot add fetch request. {url=}")
            return
        if self.logger:
            self.logger.info(f"Fetching url {url=}")

        try:
            resp_buffer, data = None, None # default in case of exception
            async with self.session.get(url) as resp:
                resp_buffer = resp
                data = await resp.read()
                if self.logger:
                    self.logger.info(f"Fetched {url} successfully. ({len(data)} bytes)")
            return resp_buffer, data
        
        except Exception as e:
            await onerror(self, url, e, resp_buffer, data)


    @override
    async def close(self) -> None:
        """Close the fetcher."""
        # === Stop all workers (once they are done) ===
        self.running = False
        for _ in range(self.num_workers):
            await self.queue.put(None)
        
        await self.queue.join()  # Wait for all fetch jobs to be processed
        await self.session.close()  # Close the aiohttp session
    
    async def wait_and_close(self) -> None:
        """Wait for the queue to be empty and close."""
        await self.queue.join()  # Wait for all fetch jobs to be processed
        await self.session.close()  # Close the aiohttp session

    async def _worker(self):
        """Worker instance"""
        while True:
            req = await self.queue.get()
            if req is None: # Shutdown signal
                self.queue.task_done() # Notify completion
                break

            try:
                resp_buffer, data = None, None # default in case of exception
                async with self.session.get(req.url) as resp:
                    resp_buffer = resp
                    data = await resp.read()
                    if self.logger:
                        self.logger.info(f"Fetched {req.url} successfully. ({len(data)} bytes)")
                    await req.callback(self, resp, data)
            except Exception as e:
                await req.onerror(self, req.url, e, resp_buffer, data)
            finally:
                self.queue.task_done() # Notify completion

class CongestionController:
    """Rate limit on fetch fails, inspired by AIMD-type algorithms."""
    def __init__(self,
                 min_wait_time: float = 0.5,
                 backoff_factor: float = 2.0,
                 success_additive_decrease: float = 0.5,
                 ):
        """Rate limit on fetch fails, inspired by AIMD-type algorithms.
        
        Args:
            min_wait_time (float): Minimum wait time between fetch attempts.
            backoff_factor (float): On failed fetch, self.wait_time is multiplied by this factor.
            success_additive_decrease (float): On successful fetch, self.wait_time is decreased by this factor up to min_wait_time.
        """
        self.wait_time = min_wait_time
        self.min_wait_time = min_wait_time
        self.backoff_factor = backoff_factor
        self.success_additive_decrease = success_additive_decrease
        self.lock = asyncio.Lock()
    
    def reset(self):
        """Reset controller"""
        self.wait_time = self.min_wait_time

    # Before fetch
    async def consume(self):
        """Wait for corresponding time."""
        async with self.lock:
            await asyncio.sleep(self.wait_time)

    # After fetch
    def on_fetch_success(self):
        """If success, decrease wait time linearly."""
        self.wait_time = max(self.min_wait_time, self.wait_time - self.success_additive_decrease)

    def on_fetch_failure(self):
        """If failure, increase wait time exponentially"""
        self.wait_time *= self.backoff_factor


class KahRatelimitedFetcher(KahBaseFetcher):
    """Async fetcher with some basic rate limiting."""
    congestion_controllers: dict[str, CongestionController] = {}

    def __init__(self, 
                 num_workers: int = 4,
                 session: Optional[ClientSession] = None,
                 timeout: float = 10.0,
                 cc_min_wait_time: float = 0.5,
                 cc_backoff_factor: float = 2.0,
                 cc_success_additive_decrease: float = 0.5,
                 logger: Optional[Logger] = None) -> None:
        super().__init__(num_workers, timeout, session, logger=logger)
        self.cc_min_wait_time = cc_min_wait_time
        self.cc_backoff_factor = cc_backoff_factor
        self.cc_success_additive_decrease = cc_success_additive_decrease
    
    @override
    async def fetch_now(self, 
                        url: str,
                        onerror: Callable[[FetcherABC, str, Exception, ClientResponse | None, bytes | None], Awaitable[None]], # (fetcher, url, e, [Optional response, Optional data])
                        ) -> Tuple[ClientResponse, bytes] | None:
        """Fetch given url without going to queue."""
        if self.running is False: # Prevents further fetching
            if self.logger:
                self.logger.warning(f"Fetcher is not running, cannot add fetch request. {url=}")
            return
        if self.logger:
            self.logger.info(f"Fetching url {url=}")

        domain = get_domain(url) # Get domain
        if domain not in self.congestion_controllers:
            KahRatelimitedFetcher.congestion_controllers[domain] = CongestionController(
                self.cc_min_wait_time,
                self.cc_backoff_factor,
                self.cc_success_additive_decrease,
                )
        cc = KahRatelimitedFetcher.congestion_controllers[domain]

        try:
            await cc.consume() # Wait for corresponding time

            resp_buffer, data = None, None # default in case of exception
            async with self.session.get(url) as resp:
                resp_buffer = resp
                data = await resp.read()
                if self.logger:
                    self.logger.info(f"Fetched {url} successfully. ({len(data)} bytes)")
            return resp_buffer, data
        
        except Exception as e:
            await onerror(self, url, e, resp_buffer, data)

    async def _worker(self):
        """Worker instance"""
        while True:
            req = await self.queue.get()
            if req is None: # Shutdown signal
                self.queue.task_done() # Notify completion
                break

            domain = get_domain(req.url) # Get domain
            if domain not in self.congestion_controllers:
                KahRatelimitedFetcher.congestion_controllers[domain] = CongestionController(
                    self.cc_min_wait_time,
                    self.cc_backoff_factor,
                    self.cc_success_additive_decrease,
                    )
            cc = KahRatelimitedFetcher.congestion_controllers[domain]

            try:
                await cc.consume() # Wait for corresponding time

                resp_buffer, data = None, None # default in case of exception
                async with self.session.get(req.url) as resp:
                    resp_buffer = resp
                    data = await resp.read()
                    if self.logger:
                        self.logger.info(f"Fetched {req.url} successfully. ({len(data)} bytes)")
                    cc.on_fetch_success()

                    await req.callback(self, resp, data)
            except Exception as e:
                cc.on_fetch_failure()
                await req.onerror(self, req.url, e, resp_buffer, data)
            finally:
                self.queue.task_done() # Notify completion

    def fetch_declare_failure(self, url_or_domain: str) -> None:
        """Declare an error when fetching externally. Example: if != 200 status found."""
        domain = get_domain(url_or_domain) # Get domain
        KahRatelimitedFetcher.congestion_controllers[domain].on_fetch_failure()
    
    def fetch_declare_success(self, url_or_domain: str) -> None:
        """Declare a success when fetching externally."""
        domain = get_domain(url_or_domain) # Get domain
        KahRatelimitedFetcher.congestion_controllers[domain].on_fetch_success()
