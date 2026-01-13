"""
Job handlers module.

This module provides the handler registry system and all registered handlers.

Importing this module automatically registers all handlers by importing
their modules.

Example:
    from app.handlers import registry
    
    # List all handlers
    print(registry.list())
    
    # Get a handler
    handler = registry.get("send_email")
    handler({"to": "user@example.com", "subject": "Hello"})
"""
import logging

# Import registry first
from app.handlers.registry import registry, HandlerMetadata

# Import all handler modules to trigger registration
# Add new handler modules here as you create them
from app.handlers import examples

# You can add more handler modules here:
# from app.handlers import email
# from app.handlers import notifications
# from app.handlers import reports
# from app.handlers import custom

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────
# Export public API
# ─────────────────────────────────────────────────────────────────

__all__ = [
    "registry",
    "HandlerMetadata",
]

# ─────────────────────────────────────────────────────────────────
# Log registered handlers on import
# ─────────────────────────────────────────────────────────────────

def _log_registered_handlers():
    """Log all registered handlers (called on module import)."""
    handler_count = len(registry)
    
    if handler_count == 0:
        logger.warning("⚠️  No handlers registered!")
        return
    
    logger.info(f"📋 Registered {handler_count} job handler(s):")
    
    for name in registry.list():
        metadata = registry.get_metadata(name)
        # Get first line of description
        desc_preview = metadata.description.split('\n')[0].strip()
        logger.info(f"   • {name}: {desc_preview}")


# Call on import (only if not running in test mode)
import sys
if "pytest" not in sys.modules:
    _log_registered_handlers()


# ─────────────────────────────────────────────────────────────────
# Convenience functions
# ─────────────────────────────────────────────────────────────────

def list_handlers() -> list[str]:
    """
    Get list of all registered handler names.
    
    Returns:
        Sorted list of handler names
    
    Example:
        from app.handlers import list_handlers
        
        handlers = list_handlers()
        print(f"Available: {', '.join(handlers)}")
    """
    return registry.list()


def list_handlers_with_info() -> dict[str, str]:
    """
    Get all handlers with their descriptions.
    
    Returns:
        Dict mapping handler names to descriptions
    
    Example:
        from app.handlers import list_handlers_with_info
        
        for name, desc in list_handlers_with_info().items():
            print(f"{name}: {desc}")
    """
    return registry.list_with_descriptions()


def get_handler(name: str):
    """
    Get a handler function by name.
    
    Args:
        name: Handler name
    
    Returns:
        Handler function
    
    Raises:
        KeyError: If handler not found
    
    Example:
        from app.handlers import get_handler
        
        handler = get_handler("send_email")
        handler({"to": "user@example.com"})
    """
    return registry.get(name)


def handler_exists(name: str) -> bool:
    """
    Check if a handler is registered.
    
    Args:
        name: Handler name
    
    Returns:
        True if handler exists
    
    Example:
        from app.handlers import handler_exists
        
        if handler_exists("send_email"):
            print("Handler available!")
    """
    return registry.exists(name)


# Update __all__ with convenience functions
__all__.extend([
    "list_handlers",
    "list_handlers_with_info",
    "get_handler",
    "handler_exists",
])