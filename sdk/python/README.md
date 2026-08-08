# Reiver Python SDK

The Observability Platform for AI Applications - Python SDK for error monitoring.

## Installation

```bash
pip install reiver
```

## Quick Start

```python
import reiver

# Initialize Reiver
reiver.init(
    api_key="your-project-api-key",
    api_url="https://api.reiver.io",  # Optional, defaults to localhost
    environment="production",  # Optional
    tags={"server": "web-1"}  # Optional
)

# Automatic exception capture
try:
    risky_operation()
except Exception as e:
    reiver.capture_exception(e)

# Manual error reporting
reiver.capture_message(
    "Something went wrong",
    level="error",
    context={"request_id": "123"},
    tags={"component": "payment"},
    user={"id": "user_123", "email": "user@example.com"}
)
```

## Features

- **Automatic exception capture** - Uncaught exceptions are automatically captured
- **Async sending** - Errors are sent in the background, never blocking your application
- **Rate limiting** - Built-in client-side rate limiting
- **Context capture** - Add custom context, tags, and user information
- **Zero dependencies** - Only requires `requests` library

## API Reference

### `reiver.init(api_key, api_url=None, environment=None, tags=None)`

Initialize the global Reiver client.

**Parameters:**
- `api_key` (str): Your project API key
- `api_url` (str, optional): Reiver API URL (default: "http://localhost:3000")
- `environment` (str, optional): Environment name (e.g., "production", "staging")
- `tags` (dict, optional): Default tags to include with all errors

### `reiver.capture_exception(exception, exc_traceback=None, context=None, tags=None, user=None)`

Capture an exception and send it to Reiver.

**Parameters:**
- `exception` (Exception): The exception to capture
- `exc_traceback` (traceback, optional): Traceback object (auto-detected if not provided)
- `context` (dict, optional): Additional context data
- `tags` (dict, optional): Additional tags
- `user` (dict, optional): User information (id, email, etc.)

### `reiver.capture_message(message, level="error", context=None, tags=None, user=None)`

Capture a message (non-exception error).

**Parameters:**
- `message` (str): Error message
- `level` (str): Error level ("error", "warning", "info")
- `context` (dict, optional): Additional context data
- `tags` (dict, optional): Additional tags
- `user` (dict, optional): User information

## License

Apache 2.0
