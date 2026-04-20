from fastapi import Depends, HTTPException, Header
import os

def verify_internal_key(x_internal_key: str = Header(..., alias="X-Internal-Key")):
    """
    Dependency to verify internal API key for scrape endpoints.
    Compares the X-Internal-Key header with INTERNAL_API_KEY environment variable.
    """
    expected_key = os.getenv("INTERNAL_API_KEY")
    if not expected_key:
        raise HTTPException(status_code=500, detail="Internal API key not configured")
    
    if x_internal_key != expected_key:
        raise HTTPException(status_code=403, detail="Invalid internal API key")
    
    return True