import requests
import logging
from typing import Any

logger = logging.getLogger(__name__)


class APIExecutor:
    """HTTP/REST API executor for making API calls and webhooks."""

    def execute(self, config: dict) -> tuple[bool, Any]:
        """
        Execute HTTP requests.
        
        config = {
            "method": "GET|POST|PUT|DELETE|PATCH",
            "url": "https://api.example.com/endpoint",
            "headers": {"Authorization": "Bearer token"},
            "params": {"key": "value"},
            "json": {"data": "payload"},
            "timeout": 30,
            "retry_count": 3
        }
        """
        try:
            method = config.get("method", "GET").upper()
            url = config.get("url")
            headers = config.get("headers", {})
            params = config.get("params")
            json_data = config.get("json")
            timeout = config.get("timeout", 30)
            retry_count = config.get("retry_count", 1)

            if not url:
                return False, {"error": "URL is required"}

            for attempt in range(retry_count):
                try:
                    if method == "GET":
                        response = requests.get(url, headers=headers, params=params, timeout=timeout)
                    elif method == "POST":
                        response = requests.post(url, headers=headers, json=json_data, params=params, timeout=timeout)
                    elif method == "PUT":
                        response = requests.put(url, headers=headers, json=json_data, params=params, timeout=timeout)
                    elif method == "DELETE":
                        response = requests.delete(url, headers=headers, params=params, timeout=timeout)
                    elif method == "PATCH":
                        response = requests.patch(url, headers=headers, json=json_data, params=params, timeout=timeout)
                    else:
                        return False, {"error": f"Unsupported method: {method}"}

                    if 200 <= response.status_code < 300:
                        try:
                            result = response.json()
                        except:
                            result = response.text
                        
                        logger.info(f"✓ API call successful: {method} {url} - Status: {response.status_code}")
                        return True, {
                            "status_code": response.status_code,
                            "headers": dict(response.headers),
                            "data": result
                        }
                    else:
                        if attempt == retry_count - 1:
                            return False, {
                                "status_code": response.status_code,
                                "error": response.text
                            }
                        logger.warning(f"API call failed (attempt {attempt + 1}/{retry_count}): Status {response.status_code}")

                except requests.exceptions.Timeout:
                    if attempt == retry_count - 1:
                        return False, {"error": "Request timeout"}
                    logger.warning(f"Request timeout (attempt {attempt + 1}/{retry_count})")

                except requests.exceptions.ConnectionError:
                    if attempt == retry_count - 1:
                        return False, {"error": "Connection error"}
                    logger.warning(f"Connection error (attempt {attempt + 1}/{retry_count})")

        except Exception as e:
            logger.error(f"API call error: {e}")
            return False, {"error": str(e)}

        return False, {"error": "Max retries exceeded"}
