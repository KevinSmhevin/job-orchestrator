"""
Handler registry for job execution.

Provides a decorator-based system for registering and looking up job handlers.
"""
from typing import Callable, Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


class HandlerMetadata:
    """Metadata about a registered handler."""
    
    def __init__(
        self,
        name: str,
        func: Callable,
        description: Optional[str] = None,
        timeout: Optional[int] = None,
        retries: Optional[int] = None,
    ):
        self.name = name
        self.func = func
        self.description = description or func.__doc__ or "No description"
        self.timeout = timeout
        self.retries = retries
    
    def __repr__(self) -> str:
        return f"<Handler {self.name}: {self.description[:50]}>"


class HandlerRegistry:
    """
    Registry for job handlers.
    
    Handlers are functions that process jobs. They receive a payload dict
    and perform some work.
    
    Example:
        registry = HandlerRegistry()
        
        @registry.register("send_email")
        def send_email(payload: dict) -> None:
            to = payload["to"]
            subject = payload["subject"]
            # Send email...
    """
    
    def __init__(self):
        self._handlers: Dict[str, HandlerMetadata] = {}
    
    def register(
        self,
        name: str,
        description: Optional[str] = None,
        timeout: Optional[int] = None,
        retries: Optional[int] = None,
    ) -> Callable:
        """
        Decorator to register a handler function.
        
        Args:
            name: Unique handler name (used in job.handler field)
            description: Optional description (defaults to docstring)
            timeout: Optional timeout in seconds (for future use)
            retries: Optional retry count override (for future use)
        
        Returns:
            Decorated function
        
        Raises:
            ValueError: If handler name already registered
        
        Example:
            @registry.register("send_email", timeout=60)
            def send_email(payload: dict) -> None:
                # Send email logic
                pass
        """
        def decorator(func: Callable) -> Callable:
            if name in self._handlers:
                raise ValueError(
                    f"Handler '{name}' is already registered. "
                    f"Existing: {self._handlers[name].func.__module__}.{self._handlers[name].func.__name__}"
                )
            
            # Create metadata
            metadata = HandlerMetadata(
                name=name,
                func=func,
                description=description,
                timeout=timeout,
                retries=retries,
            )
            
            # Store handler
            self._handlers[name] = metadata
            
            logger.info(f"Registered handler: {name} -> {func.__module__}.{func.__name__}")
            
            return func
        
        return decorator
    
    def get(self, name: str) -> Callable:
        """
        Get a handler function by name.
        
        Args:
            name: Handler name
        
        Returns:
            Handler function
        
        Raises:
            KeyError: If handler not found
        
        Example:
            handler_func = registry.get("send_email")
            handler_func({"to": "user@example.com", "subject": "Hello"})
        """
        if name not in self._handlers:
            available = ", ".join(self.list())
            raise KeyError(
                f"Handler '{name}' not registered. "
                f"Available handlers: [{available}]"
            )
        
        return self._handlers[name].func
    
    def get_metadata(self, name: str) -> HandlerMetadata:
        """
        Get handler metadata.
        
        Args:
            name: Handler name
        
        Returns:
            Handler metadata
        
        Raises:
            KeyError: If handler not found
        """
        if name not in self._handlers:
            available = ", ".join(self.list())
            raise KeyError(
                f"Handler '{name}' not registered. "
                f"Available handlers: [{available}]"
            )
        
        return self._handlers[name]
    
    def exists(self, name: str) -> bool:
        """
        Check if a handler is registered.
        
        Args:
            name: Handler name
        
        Returns:
            True if handler exists, False otherwise
        
        Example:
            if registry.exists("send_email"):
                print("Handler is registered!")
        """
        return name in self._handlers
    
    def list(self) -> list[str]:
        """
        List all registered handler names.
        
        Returns:
            Sorted list of handler names
        
        Example:
            handlers = registry.list()
            print(f"Available handlers: {', '.join(handlers)}")
        """
        return sorted(self._handlers.keys())
    
    def list_with_descriptions(self) -> Dict[str, str]:
        """
        List all handlers with descriptions.
        
        Returns:
            Dict mapping handler names to descriptions
        
        Example:
            for name, desc in registry.list_with_descriptions().items():
                print(f"{name}: {desc}")
        """
        return {
            name: metadata.description
            for name, metadata in sorted(self._handlers.items())
        }
    
    def unregister(self, name: str) -> None:
        """
        Unregister a handler (useful for testing).
        
        Args:
            name: Handler name
        
        Raises:
            KeyError: If handler not found
        
        Example:
            registry.unregister("send_email")
        """
        if name not in self._handlers:
            raise KeyError(f"Handler '{name}' not registered")
        
        del self._handlers[name]
        logger.info(f"Unregistered handler: {name}")
    
    def clear(self) -> None:
        """
        Clear all registered handlers (useful for testing).
        
        Example:
            registry.clear()
        """
        self._handlers.clear()
        logger.info("Cleared all handlers")
    
    def __len__(self) -> int:
        """Return number of registered handlers."""
        return len(self._handlers)
    
    def __contains__(self, name: str) -> bool:
        """Check if handler is registered (supports 'in' operator)."""
        return name in self._handlers
    
    def __repr__(self) -> str:
        """String representation."""
        return f"<HandlerRegistry: {len(self)} handlers registered>"


# Global singleton registry
registry = HandlerRegistry()


# Convenience functions for common operations
def get_handler(name: str) -> Callable:
    """
    Get a handler function by name.
    
    Convenience function that uses the global registry.
    
    Args:
        name: Handler name
    
    Returns:
        Handler function
    
    Example:
        handler = get_handler("send_email")
        handler({"to": "user@example.com"})
    """
    return registry.get(name)


def list_handlers() -> list[str]:
    """
    List all registered handler names.
    
    Convenience function that uses the global registry.
    
    Returns:
        Sorted list of handler names
    
    Example:
        print(f"Available: {', '.join(list_handlers())}")
    """
    return registry.list()


def handler_exists(name: str) -> bool:
    """
    Check if a handler exists.
    
    Convenience function that uses the global registry.
    
    Args:
        name: Handler name
    
    Returns:
        True if handler exists
    
    Example:
        if handler_exists("send_email"):
            print("Handler is available!")
    """
    return registry.exists(name)