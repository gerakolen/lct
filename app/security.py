import secrets
from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from starlette.status import HTTP_401_UNAUTHORIZED

_security = HTTPBasic(auto_error=True)


def require_basic_auth(
    request: Request,
    credentials: HTTPBasicCredentials = Depends(_security),
) -> str:
    cfg = request.app.state.settings.auth
    u_ok = secrets.compare_digest(credentials.username, cfg.username)
    p_ok = secrets.compare_digest(credentials.password, cfg.password)
    if not (u_ok and p_ok):
        raise HTTPException(
            status_code=HTTP_401_UNAUTHORIZED,
            detail="Unauthorized",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username
