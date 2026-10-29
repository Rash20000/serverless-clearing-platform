"""
utils.py – Outbound Partner Egress Gateway utilities.

Provides functional helpers for:
- Retrieving API keys from AWS Secrets Manager with in-memory caching
- Retrieving vendor endpoint base URLs from AWS SSM Parameter Store with in-memory caching
- Outbound HTTPS dispatch with connection pooling and adaptive retries
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, Optional
import urllib3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
import boto3
from aws_lambda_powertools import Logger, Tracer

logger = Logger(service="partner-egress-gateway")
tracer = Tracer(service="partner-egress-gateway")

_BOTO_CONFIG = Config(retries={"max_attempts": 3, "mode": "adaptive"})

_secretsmanager_client = None
_ssm_client = None
_http_pool = None

_secrets_cache: Dict[str, str] = {}
_ssm_cache: Dict[str, str] = {}


def get_secretsmanager_client():
    """Return cached Secrets Manager client."""
    global _secretsmanager_client
    if _secretsmanager_client is None:
        _secretsmanager_client = boto3.client("secretsmanager", config=_BOTO_CONFIG)
    return _secretsmanager_client


def get_ssm_client():
    """Return cached SSM client."""
    global _ssm_client
    if _ssm_client is None:
        _ssm_client = boto3.client("ssm", config=_BOTO_CONFIG)
    return _ssm_client


def get_http_pool() -> urllib3.PoolManager:
    """Return cached urllib3 PoolManager with connection reuse."""
    global _http_pool
    if _http_pool is None:
        _http_pool = urllib3.PoolManager(
            maxsize=10,
            retries=urllib3.util.Retry(total=2, backoff_factor=0.2),
            timeout=urllib3.util.Timeout(connect=2.0, read=10.0),
        )
    return _http_pool


@tracer.capture_method
def get_vendor_secret(vendor: str, force_refresh: bool = False) -> str:
    """
    Fetch vendor API key from AWS Secrets Manager.
    Secret ID format: /egress/market-data/{vendor}/api-key
    Cached in memory across warm starts.
    If force_refresh is True, bypasses cache and updates stored secret.
    """
    secret_id = f"/egress/market-data/{vendor.lower()}/api-key"
    if not force_refresh and secret_id in _secrets_cache:
        return _secrets_cache[secret_id]

    client = get_secretsmanager_client()
    logger.info("Fetching API key from Secrets Manager", extra={"vendor": vendor, "secret_id": secret_id, "force_refresh": force_refresh})
    try:
        response = client.get_secret_value(SecretId=secret_id)
        secret_val = response.get("SecretString", "")
        _secrets_cache[secret_id] = secret_val
        return secret_val
    except (ClientError, BotoCoreError) as err:
        logger.exception("Failed to retrieve vendor secret", extra={"secret_id": secret_id})
        raise


def evict_vendor_secret(vendor: str) -> None:
    """Evict cached vendor API key to force refresh on subsequent requests."""
    secret_id = f"/egress/market-data/{vendor.lower()}/api-key"
    if _secrets_cache.pop(secret_id, None) is not None:
        logger.info("Evicted cached vendor secret from memory", extra={"vendor": vendor, "secret_id": secret_id})


@tracer.capture_method
def get_vendor_base_url(vendor: str, fallback_default: str = "") -> str:
    """
    Fetch vendor base URL from AWS SSM Parameter Store.
    Parameter format: /egress/market-data/{vendor}/base-url
    Cached in memory across warm starts.
    """
    param_name = f"/egress/market-data/{vendor.lower()}/base-url"
    if param_name in _ssm_cache:
        return _ssm_cache[param_name]

    client = get_ssm_client()
    logger.info("Fetching vendor base URL from SSM", extra={"vendor": vendor, "param_name": param_name})
    try:
        response = client.get_parameter(Name=param_name)
        val = response.get("Parameter", {}).get("Value", fallback_default)
        _ssm_cache[param_name] = val
        return val
    except (ClientError, BotoCoreError):
        if fallback_default:
            logger.warning("SSM parameter not found; using fallback default", extra={"param": param_name})
            _ssm_cache[param_name] = fallback_default
            return fallback_default
        raise


@tracer.capture_method
def send_vendor_http_request(
    url: str,
    method: str = "GET",
    headers: Optional[Dict[str, str]] = None,
    body: Optional[Any] = None,
    timeout: float = 10.0,
) -> Dict[str, Any]:
    """
    Dispatch HTTP request to external vendor endpoint with connection pooling.
    Returns status code and parsed JSON data (or raw text).
    """
    pool = get_http_pool()
    req_headers = {"User-Agent": "Antigravity-EgressGateway/1.0", **(headers or {})}

    encoded_body = None
    if body is not None:
        if isinstance(body, (dict, list)):
            encoded_body = json.dumps(body).encode("utf-8")
            req_headers.setdefault("Content-Type", "application/json")
        elif isinstance(body, str):
            encoded_body = body.encode("utf-8")
        elif isinstance(body, bytes):
            encoded_body = body

    logger.info("Dispatching HTTPS call to vendor", extra={"url": url, "method": method})
    try:
        response = pool.request(
            method=method.upper(),
            url=url,
            headers=req_headers,
            body=encoded_body,
            timeout=timeout,
        )

        status_code = response.status
        data_text = response.data.decode("utf-8", errors="replace")

        try:
            parsed_data = json.loads(data_text)
        except json.JSONDecodeError:
            parsed_data = {"raw_response": data_text}

        return {
            "status_code": status_code,
            "data": parsed_data,
        }
    except Exception as exc:
        logger.exception("Vendor HTTP request failed", extra={"url": url, "method": method})
        raise


@tracer.capture_method
def execute_vendor_request_with_secret_retry(
    vendor: str,
    url: str,
    method: str = "GET",
    headers: Optional[Dict[str, str]] = None,
    body: Optional[Any] = None,
    timeout: float = 10.0,
) -> Dict[str, Any]:
    """
    Execute HTTP call to vendor with automatic secret re-fetch and retry.
    
    If vendor returns 401 Unauthorized or 403 Forbidden (credential rotation occurred),
    the in-memory secret cache is invalidated, the fresh secret is fetched from
    AWS Secrets Manager, and the request is retried once.
    """
    api_key = get_vendor_secret(vendor)
    req_headers = dict(headers or {})
    req_headers["X-Api-Key"] = api_key

    resp = send_vendor_http_request(url=url, method=method, headers=req_headers, body=body, timeout=timeout)

    # Detect credential rotation in flight (401/403)
    if resp.get("status_code") in (401, 403):
        logger.warning(
            "Vendor returned auth challenge; evicting cached secret and retrying with fresh secret",
            extra={"vendor": vendor, "initial_status": resp.get("status_code")},
        )
        fresh_key = get_vendor_secret(vendor, force_refresh=True)
        req_headers["X-Api-Key"] = fresh_key
        resp = send_vendor_http_request(url=url, method=method, headers=req_headers, body=body, timeout=timeout)
        logger.info(
            "Completed retry with refreshed vendor secret",
            extra={"vendor": vendor, "retry_status": resp.get("status_code")},
        )

    return resp
