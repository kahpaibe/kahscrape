from urllib.parse import urlparse

def get_domain(url: str) -> str:
    """Extract the domain from a URL."""
    parsed_url = urlparse(url)
    domain = parsed_url.netloc
    domain = domain.lstrip('w.') # remove www.
    return domain
