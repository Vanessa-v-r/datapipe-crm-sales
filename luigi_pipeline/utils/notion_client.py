# utils/notion_client.py

import requests
import time
import logging
from luigi_pipeline.configs.settings import (
    NOTION_API_URL,
    NOTION_API_KEY,
    NOTION_RATE_LIMIT_CALLS,
    NOTION_RATE_LIMIT_PERIOD
)
from requests.exceptions import ConnectionError, HTTPError, Timeout, RequestException
from threading import Lock
from datetime import datetime
import structlog

class NotionClient:
    def __init__(self, logger=None):
        """
        Initializes the Notion client with a specific logger.

        Args:
            logger (structlog.BoundLogger, optional): Specific task logger.
        """
        self.headers = {
            'Authorization': f'Bearer {NOTION_API_KEY}',
            'Notion-Version': '2022-06-28',
            'Content-Type': 'application/json'
        }
        self.session = requests.Session()
        self.lock = Lock()
        self.call_times = []
        self.rate_limit_calls = NOTION_RATE_LIMIT_CALLS  # e.g., 3
        self.rate_limit_period = NOTION_RATE_LIMIT_PERIOD  # e.g., 1 second
        self.logger = logger or structlog.get_logger(__name__)

    def _enforce_rate_limit(self):
        with self.lock:
            current_time = datetime.now()
            # Remove timestamps that are outside the rate limiting period
            self.call_times = [t for t in self.call_times if (current_time - t).total_seconds() < self.rate_limit_period]
            if len(self.call_times) >= self.rate_limit_calls:
                # Calculate how long to wait until the next call is allowed
                wait_time = self.rate_limit_period - (current_time - self.call_times[0]).total_seconds()
                if wait_time > 0:
                    self.logger.info(event="rate_limit", message=f"Rate limit reached. Waiting for {wait_time:.2f} seconds.")
                    time.sleep(wait_time)
            # Record the time of the new call
            self.call_times.append(datetime.now())

    def fetch_database(self, database_id):
        """
        Fetches data from a specific Notion database.

        Args:
            database_id (str): ID of the Notion database.

        Returns:
            dict: Dictionary containing the query results.
        """
        url = f'{NOTION_API_URL}/databases/{database_id}/query'
        has_more = True
        next_cursor = None
        all_results = []

        while has_more:
            self._enforce_rate_limit()
            payload = {}
            if next_cursor:
                payload['start_cursor'] = next_cursor

            try:
                self.logger.info(event="fetch_request", message=f"Fetching data from database ID: {database_id}.")
                response = self.session.post(url, headers=self.headers, json=payload, timeout=60)
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', '1'))
                    self.logger.warning(event="rate_limit_exceeded", message=f"Rate limit exceeded. Retrying after {retry_after} seconds.")
                    time.sleep(retry_after)
                    continue  # Retry after waiting
                response.raise_for_status()
                data = response.json()
                all_results.extend(data.get('results', []))
                has_more = data.get('has_more', False)
                next_cursor = data.get('next_cursor', None)
                self.logger.debug(event="fetch_success", message=f"Fetched {len(data.get('results', []))} records from database ID: {database_id}.")
            except HTTPError as http_err:
                self.logger.error(event="http_error", message=f'HTTP error while fetching database {database_id}: {http_err} - Response: {response.text}')
                raise
            except ConnectionError as conn_err:
                self.logger.error(event="connection_error", message=f'Connection error while fetching database {database_id}: {conn_err}')
                raise
            except Timeout as timeout_err:
                self.logger.error(event="timeout_error", message=f'Timeout error while fetching database {database_id}: {timeout_err}')
                raise
            except RequestException as req_err:
                self.logger.error(event="request_exception", message=f'Request exception while fetching database {database_id}: {req_err}')
                raise

        self.logger.info(event="fetch_complete", message=f"Completed fetching database ID: {database_id}. Total records fetched: {len(all_results)}.")
        return {'results': all_results}
