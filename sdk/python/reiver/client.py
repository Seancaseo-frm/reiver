"""
Reiver Python SDK - Error monitoring client
"""

import json
import sys
import traceback
import threading
import time
from typing import Optional, Dict, Any, List
from datetime import datetime
import requests
from queue import Queue


class ReiverClient:
    """Main client for sending errors to Reiver"""
    
    def __init__(
        self,
        api_key: str,
        api_url: str = "http://localhost:3000",
        environment: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
        rate_limit: int = 100,
    ):
        self.api_key = api_key
        self.api_url = api_url.rstrip('/')
        self.environment = environment or "production"
        self.tags = tags or {}
        self.rate_limit = rate_limit
        
        # Queue for async sending
        self.queue = Queue()
        self.worker_thread = None
        self.running = False
        
        # Rate limiting
        self.request_count = 0
        self.window_start = time.time()
        self.lock = threading.Lock()
        
        # Start worker thread
        self.start_worker()
        
        # Install exception hook
        self._install_exception_hook()
    
    def _install_exception_hook(self):
        """Install global exception hook to capture uncaught exceptions"""
        original_excepthook = sys.excepthook
        
        def exception_hook(exc_type, exc_value, exc_traceback):
            # Call original hook first
            original_excepthook(exc_type, exc_value, exc_traceback)
            
            # Capture exception
            self.capture_exception(exc_value, exc_traceback)
        
        sys.excepthook = exception_hook
    
    def start_worker(self):
        """Start background worker thread for sending errors"""
        if self.worker_thread is None or not self.worker_thread.is_alive():
            self.running = True
            self.worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
            self.worker_thread.start()
    
    def _worker_loop(self):
        """Worker loop that processes error queue"""
        while self.running:
            try:
                error_data = self.queue.get(timeout=1)
                if error_data is None:
                    continue
                
                self._send_error(error_data)
                self.queue.task_done()
            except:
                time.sleep(0.1)
    
    def _check_rate_limit(self) -> bool:
        """Check if we're within rate limit"""
        with self.lock:
            current_time = time.time()
            
            # Reset window if 60 seconds have passed
            if current_time - self.window_start >= 60:
                self.request_count = 0
                self.window_start = current_time
            
            if self.request_count >= self.rate_limit:
                return False
            
            self.request_count += 1
            return True
    
    def _send_error(self, error_data: Dict[str, Any]):
        """Send error to Reiver API"""
        if not self._check_rate_limit():
            return  # Silently drop if rate limited
        
        try:
            response = requests.post(
                f"{self.api_url}/api/v1/exceptions",
                json=error_data,
                headers={"Content-Type": "application/json"},
                timeout=5
            )
            response.raise_for_status()
        except Exception as e:
            # Silently fail - don't break the application
            pass
    
    def _extract_stacktrace(self, exc_traceback) -> List[Dict[str, Any]]:
        """Extract stack trace from traceback"""
        frames = []
        
        if exc_traceback is None:
            return frames
        
        tb = exc_traceback
        while tb is not None:
            frame = tb.tb_frame
            frames.append({
                "filename": frame.f_code.co_filename,
                "function": frame.f_code.co_name,
                "lineno": tb.tb_lineno,
                "code": None,  # Could extract source code here
            })
            tb = tb.tb_next
        
        return frames
    
    def capture_exception(
        self,
        exception: Exception,
        exc_traceback=None,
        context: Optional[Dict[str, Any]] = None,
        tags: Optional[Dict[str, str]] = None,
        user: Optional[Dict[str, Any]] = None,
    ):
        """Capture an exception and send it to Reiver"""
        if exc_traceback is None:
            exc_traceback = sys.exc_info()[2]
        
        stacktrace = self._extract_stacktrace(exc_traceback)
        
        # Try to get trace_id from OpenTelemetry context
        trace_id = None
        try:
            from opentelemetry import trace
            span = trace.get_current_span()
            if span and span.is_recording():
                span_context = span.get_span_context()
                if span_context.is_valid:
                    # Format trace_id as hex string (32 hex chars)
                    trace_id = format(span_context.trace_id, "032x")
        except ImportError:
            # OpenTelemetry not installed, skip trace_id
            pass
        except Exception:
            # Failed to get trace_id, skip it
            pass
        
        error_data = {
            "project_key": self.api_key,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": "error",
            "message": str(exception),
            "exception": {
                "type": type(exception).__name__,
                "value": str(exception),
                "stacktrace": stacktrace,
            },
            "context": {
                "runtime": {
                    "name": "python",
                    "version": f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
                },
                "environment": self.environment,
                **(context or {}),
            },
            "tags": {
                **self.tags,
                **(tags or {}),
            },
            "user": user,
        }
        
        if trace_id:
            error_data["trace_id"] = trace_id
        
        # Queue for async sending
        try:
            self.queue.put(error_data, block=False)
        except:
            pass  # Queue full, drop error
    
    def capture_message(
        self,
        message: str,
        level: str = "error",
        context: Optional[Dict[str, Any]] = None,
        tags: Optional[Dict[str, str]] = None,
        user: Optional[Dict[str, Any]] = None,
    ):
        """Capture a message and send it to Reiver"""
        # Try to get trace_id from OpenTelemetry context
        trace_id = None
        try:
            from opentelemetry import trace
            span = trace.get_current_span()
            if span and span.is_recording():
                span_context = span.get_span_context()
                if span_context.is_valid:
                    # Format trace_id as hex string (32 hex chars)
                    trace_id = format(span_context.trace_id, "032x")
        except ImportError:
            # OpenTelemetry not installed, skip trace_id
            pass
        except Exception:
            # Failed to get trace_id, skip it
            pass
        
        """Capture a message (non-exception error)"""
        error_data = {
            "project_key": self.api_key,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": level,
            "message": message,
            "context": {
                "runtime": {
                    "name": "python",
                    "version": f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
                },
                "environment": self.environment,
                **(context or {}),
            },
            "tags": {
                **self.tags,
                **(tags or {}),
            },
            "user": user,
        }
        
        if trace_id:
            error_data["trace_id"] = trace_id
        
        try:
            self.queue.put(error_data, block=False)
        except:
            pass
    
    def close(self):
        """Close the client and wait for pending errors"""
        self.running = False
        if self.worker_thread:
            self.worker_thread.join(timeout=5)


# Global client instance
_client: Optional[ReiverClient] = None


def init(
    api_key: str,
    api_url: str = "http://localhost:3000",
    environment: Optional[str] = None,
    tags: Optional[Dict[str, str]] = None,
):
    """Initialize the global Reiver client"""
    global _client
    _client = ReiverClient(api_key, api_url, environment, tags)
    return _client


def capture_exception(
    exception: Exception,
    exc_traceback=None,
    context: Optional[Dict[str, Any]] = None,
    tags: Optional[Dict[str, str]] = None,
    user: Optional[Dict[str, Any]] = None,
):
    """Capture an exception using the global client"""
    if _client is None:
        raise RuntimeError("Reiver not initialized. Call reiver.init() first.")
    
    _client.capture_exception(exception, exc_traceback, context, tags, user)


def capture_message(
    message: str,
    level: str = "error",
    context: Optional[Dict[str, Any]] = None,
    tags: Optional[Dict[str, str]] = None,
    user: Optional[Dict[str, Any]] = None,
):
    """Capture a message using the global client"""
    if _client is None:
        raise RuntimeError("Reiver not initialized. Call reiver.init() first.")
    
    _client.capture_message(message, level, context, tags, user)
