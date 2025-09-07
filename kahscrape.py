import asyncio
import aiohttp
from aiohttp import ClientResponse, ClientSession
from asyncio import PriorityQueue
from typing import Optional, Callable
from dataclasses import dataclass
from logging import Logger

from .lib.kahscrape_utils import get_domain
from .lib.kahscrape_structs import FetcherABC, ToretryException, ToskipException

from typing import override, Awaitable, Tuple

@dataclass
class KahReq:
    url: str
    callback: Callable[[FetcherABC, ClientResponse, bytes], Awaitable[None]] # (fetcher, response, data)
    onerror: Callable[[FetcherABC, str, Exception, ClientResponse | None, bytes | None], Awaitable[None]] # (fetcher, url, e, [Optional response, Optional data])
    num_fetch: int = 1  # Number of fetch try

class KahBaseFetcher(FetcherABC):
    """Base async fetcher."""
    def __init__(self, 
                 num_workers: int,
                 timeout: float = 10.0,
                 session: Optional[ClientSession] = None,
                 max_retries: int = 5,
                 logger: Optional[Logger] = None) -> None:
        """Base async fetcher.
        
        Args:
            num_workers (int): Number of worker threads.
            timeout (float): Timeout for fetch (for unset session).
            session (Optional[ClientSession]): Optional aiohttp ClientSession.
            max_retries (int): Maximum number of retries for failed requests.
            logger (Optional[Logger]): Optional Logger.
        """
        super().__init__()
        self.logger = logger
        self.session = session if session is not None else aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout))
        self.num_workers: int = num_workers
        self.running = True
        self.queue: PriorityQueue[tuple[int, KahReq | None]] = PriorityQueue()
        self.fetch_positive_counter = 1  # Used for fetch retries
        self.fetch_negative_counter = -1 # Used for new fetches
        self.max_retries = max_retries
        self._worker_count = 0
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
        self.fetch_negative_counter -= 1 # Go down further (highest priority)
        await self.queue.put((self.fetch_negative_counter, req))

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

        for i in range(self.max_retries): # At most self.max_retries retry
            if i > 0 and self.logger:
                self.logger.info(f"Retrying fetch {url=}, attempt {i+1}/{self.max_retries}")
            try: 
                try:
                    resp_buffer, data = None, None # default in case of exception
                    async with self.session.get(url) as resp:
                        resp_buffer = resp
                        data = await resp.read()
                        if self.logger:
                            self.logger.info(f"Fetched {url} successfully. ({len(data)} bytes)")
                    return resp_buffer, data
                
                except Exception:
                    # await onerror(self, url, e, resp_buffer, data)
                    raise ToretryException(i)
                
            except ToretryException: # Try again
                await asyncio.sleep(0.1) # Allow asyncio to do something else
                continue
        if self.logger:
            self.logger.critical(f"Max retries reached for fetch {url=}, giving up.")
        await onerror(self, url, Exception("Max retry count reached"), None, None)

    @override
    async def close(self) -> None:
        """Close the fetcher."""
        # === Stop all workers (once they are done) ===
        self.running = False
        for _ in range(self.num_workers):
            self.fetch_positive_counter += 1 # Go up further (lowest priority, done at the end)
            await self.queue.put((self.fetch_positive_counter, None))
        
        await self.queue.join()  # Wait for all fetch jobs to be processed
        await self.session.close()  # Close the aiohttp session
    
    async def wait_and_close(self) -> None:
        """Wait for the queue to be empty and close."""
        await self.queue.join()  # Wait for all fetch jobs to be processed
        await self.session.close()  # Close the aiohttp session

    async def _worker(self):
        """Worker instance"""
        self._worker_count += 1
        worker_id = self._worker_count

        while True:
            item = await self.queue.get()
            req = item[1]
            if req is None: # Shutdown signal
                self.queue.task_done() # Notify completion
                if self.logger:
                    self.logger.info(f"Worker {worker_id} shutting down.")
                break
            
            try:
                try:
                    resp_buffer, data = None, None # default in case of exception
                    async with self.session.get(req.url) as resp:
                        resp_buffer = resp
                        data = await resp.read()
                        if self.logger:
                            self.logger.info(f"Fetched {req.url} successfully. ({len(data)} bytes)")
                        await req.callback(self, resp_buffer, data)
                except Exception as e:
                    # await req.onerror(self, req.url, e, resp_buffer, data) # Don't raise error when attempts left
                    raise e # Go up

            except ToretryException as te:
                if req.num_fetch >= self.max_retries:
                    if self.logger:
                        self.logger.critical(f"Max retries reached for fetch {req.url=}, giving up.")
                    await req.onerror(self, req.url, te, None, None)
                    continue
                if self.logger:
                    self.logger.info(f"Retrying fetch {req.url=}, attempt {req.num_fetch + 1}/{self.max_retries}")
                
                req = KahReq(url=req.url, callback=req.callback, onerror=req.onerror, num_fetch=req.num_fetch + 1)
                self.fetch_positive_counter += 1 # Go up further (lowest priority)
                await self.queue.put((self.fetch_positive_counter, req)) # Queue again
            except ToskipException:
                if self.logger:
                    self.logger.debug(f"Skipping fetch {req.url=}, as requested.")
            except Exception:
                pass # don't crash
            finally:
                self.queue.task_done() # Notify completion


class CongestionController:
    """Rate limit on fetch fails, inspired by AIMD-type algorithms."""
    def __init__(self,
                 min_wait_time: float = 0.5,
                 cc_max_wait_time: float = 90.0,
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
        self.max_wait_time = cc_max_wait_time
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
        self.wait_time = min(max(self.min_wait_time, self.wait_time - self.success_additive_decrease), self.max_wait_time)

    def on_fetch_failure(self):
        """If failure, increase wait time exponentially"""
        self.wait_time *= self.backoff_factor


class KahRatelimitedFetcher(KahBaseFetcher):
    """Async fetcher with some basic rate limiting."""
    congestion_controllers: dict[str, CongestionController] = {}

    def __init__(self, 
                 num_workers: int = 10,
                 session: Optional[ClientSession] = None,
                 timeout: float = 10.0,
                 cc_min_wait_time: float = 0.5,
                 cc_max_wait_time: float = 0.5,
                 cc_backoff_factor: float = 2.0,
                 cc_success_additive_decrease: float = 0.5,
                 logger: Optional[Logger] = None) -> None:
        super().__init__(num_workers, timeout, session, logger=logger)
        self.cc_min_wait_time = cc_min_wait_time
        self.cc_max_wait_time = cc_max_wait_time
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

        for i in range(self.max_retries): # At most self.max_retries retry
            if i > 0 and self.logger:
                self.logger.info(f"Retrying fetch {url=}, attempt {i+1}/{self.max_retries}")
            try: 
                try:
                    await cc.consume() # Wait for corresponding time

                    resp_buffer, data = None, None # default in case of exception
                    async with self.session.get(url) as resp:
                        resp_buffer = resp
                        data = await resp.read()
                        if self.logger:
                            self.logger.info(f"Fetched {url} successfully. ({len(data)} bytes)")
                        cc.on_fetch_success()
                    return resp_buffer, data
                
                except Exception:
                    cc.on_fetch_failure()
                    # await onerror(self, url, e, resp_buffer, data) # Don't raise error when attempts left
                    raise ToretryException(i)
                
            except ToretryException: # Try again
                await asyncio.sleep(0.1) # Allow asyncio to do something else
                continue
            except Exception:
                pass # Prevent crash
                continue
        if self.logger:
            self.logger.critical(f"Max retries reached for fetch {url=}, giving up.")
        await onerror(self, url, Exception("Max retry count reached"), None, None)


    async def _worker(self):
        """Worker instance"""
        self._worker_count += 1
        worker_id = self._worker_count

        while True:
            item = await self.queue.get()
            req = item[1]
            if req is None: # Shutdown signal
                self.queue.task_done() # Notify completion
                if self.logger:
                    self.logger.info(f"Worker {worker_id} shutting down.")
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
                try:
                    await cc.consume() # Wait for corresponding time

                    resp_buffer, data = None, None # default in case of exception
                    async with self.session.get(req.url) as resp:
                        resp_buffer = resp
                        data = await resp.read()
                        if self.logger:
                            self.logger.info(f"Fetched {req.url} successfully. ({len(data)} bytes)")
                            cc.on_fetch_success()
                        await req.callback(self, resp_buffer, data)
                except Exception:
                    cc.on_fetch_failure()
                    # await req.onerror(self, req.url, e, resp_buffer, data) # Don't raise error when attempts left
                    raise ToretryException(req.num_fetch)

            except ToretryException as te:
                if req.num_fetch >= self.max_retries:
                    if self.logger:
                        self.logger.critical(f"Max retries reached for fetch {req.url=}, giving up.")
                    await req.onerror(self, req.url, te, None, None)
                    continue
                if self.logger:
                    self.logger.info(f"Retrying fetch {req.url=}, attempt {req.num_fetch+1}/{self.max_retries}")
                
                req = KahReq(url=req.url, callback=req.callback, onerror=req.onerror, num_fetch=req.num_fetch + 1)
                self.fetch_positive_counter += 1 # Go up further (lowest priority)
                await self.queue.put((self.fetch_positive_counter, req)) # Queue again
            except ToskipException:
                if self.logger:
                    self.logger.debug(f"Skipping fetch {req.url=}, as requested.")
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








# TODO NOW:
# The exception hadling ndoes not work correctly, retries are never notified / logged for some reason
# do : try merging the try, having only 1 layer (smart priority management)