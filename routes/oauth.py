"""
OAuth routes — Google (id_token) and GitHub (server-side code exchange).

Env vars required:
  GOOGLE_CLIENT_ID      — from Google Cloud Console
  GOOGLE_CLIENT_SECRET  — (only needed for code flow; token flow uses tokeninfo)
  GITHUB_CLIENT_ID      — from GitHub OAuth App settings
  GITHUB_CLIENT_SECRET  — from GitHub OAuth App settings
  FRONTEND_URL          — e.g. https://socratic.pro  (for post-OAuth redirect)

Google flow (SPA):
  Frontend uses @react-oauth/google → gets id_token → POST /api/auth/oauth/google

GitHub flow (server-side redirect):
  Frontend links to GET /api/auth/github
  GitHub redirects to  GET /api/auth/github/callback?code=...
  Backend redirects to FRONTEND_URL/dashboard?token=JWT
"""

import os
import uuid

import httpx
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import RedirectResponse
from pydantic import BaseModel
from sqlalchemy.orm import Session

from auth import create_access_token
from database import get_db
from models import User
from schemas import TokenResponse, UserOut

router = APIRouter(prefix="/api/auth", tags=["oauth"])

GOOGLE_CLIENT_ID     = os.getenv("GOOGLE_CLIENT_ID", "")
GITHUB_CLIENT_ID     = os.getenv("GITHUB_CLIENT_ID", "")
GITHUB_CLIENT_SECRET = os.getenv("GITHUB_CLIENT_SECRET", "")
FRONTEND_URL         = os.getenv("FRONTEND_URL", "https://socratic.pro")


# ── shared helper ─────────────────────────────────────────────────────────────

def _upsert_oauth_user(
    email: str,
    full_name: str,
    provider: str,
    db: Session,
) -> tuple[str, User]:
    """Find or create a user for OAuth login. Returns (jwt, user)."""
    user = db.query(User).filter(User.email == email).first()
    if not user:
        user = User(
            id=str(uuid.uuid4()),
            email=email,
            full_name=full_name,
            hashed_pw=None,
            auth_provider=provider,
        )
        db.add(user)
        db.commit()
        db.refresh(user)
    return create_access_token(user.id), user


# ── Google ────────────────────────────────────────────────────────────────────

class GoogleTokenBody(BaseModel):
    id_token: str


@router.post("/oauth/google", response_model=TokenResponse)
async def google_token_login(body: GoogleTokenBody, db: Session = Depends(get_db)):
    """
    The frontend obtains an id_token from Google Sign-In (e.g. @react-oauth/google)
    and sends it here. We verify it with Google's tokeninfo endpoint.
    """
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            "https://oauth2.googleapis.com/tokeninfo",
            params={"id_token": body.id_token},
            timeout=10,
        )

    if resp.status_code != 200:
        raise HTTPException(status_code=401, detail="Invalid Google token")

    info = resp.json()

    # Verify the token was issued for our app
    if GOOGLE_CLIENT_ID and info.get("aud") != GOOGLE_CLIENT_ID:
        raise HTTPException(status_code=401, detail="Google token audience mismatch")

    email     = info.get("email", "").strip()
    full_name = info.get("name", "").strip()

    if not email:
        raise HTTPException(status_code=400, detail="Could not retrieve email from Google")

    token, user = _upsert_oauth_user(email, full_name, "google", db)
    return TokenResponse(access_token=token, user=UserOut.model_validate(user))


# ── GitHub ────────────────────────────────────────────────────────────────────

@router.get("/github")
def github_login():
    """Redirect the browser to GitHub's OAuth authorization page."""
    if not GITHUB_CLIENT_ID:
        raise HTTPException(status_code=500, detail="GitHub OAuth not configured")
    url = (
        "https://github.com/login/oauth/authorize"
        f"?client_id={GITHUB_CLIENT_ID}"
        "&scope=user:email"
    )
    return RedirectResponse(url)


@router.get("/github/callback")
async def github_callback(code: str, db: Session = Depends(get_db)):
    """
    GitHub redirects here after user approves.
    We exchange the code for a token, fetch the user's email, create a JWT,
    and redirect to the frontend dashboard with ?token=JWT in the URL.
    The frontend should read this param and store the token.
    """
    if not GITHUB_CLIENT_ID or not GITHUB_CLIENT_SECRET:
        raise HTTPException(status_code=500, detail="GitHub OAuth not configured")

    async with httpx.AsyncClient(timeout=15) as client:
        # 1. Exchange code → access token
        token_resp = await client.post(
            "https://github.com/login/oauth/access_token",
            json={
                "client_id":     GITHUB_CLIENT_ID,
                "client_secret": GITHUB_CLIENT_SECRET,
                "code":          code,
            },
            headers={"Accept": "application/json"},
        )
        token_data   = token_resp.json()
        access_token = token_data.get("access_token")

        if not access_token:
            raise HTTPException(status_code=401, detail="GitHub OAuth failed — no access token")

        headers = {"Authorization": f"Bearer {access_token}"}

        # 2. Fetch user profile
        user_resp = await client.get("https://api.github.com/user", headers=headers)
        user_info = user_resp.json()

        email = (user_info.get("email") or "").strip()

        # 3. If email is not public, fetch from emails endpoint
        if not email:
            emails_resp = await client.get("https://api.github.com/user/emails", headers=headers)
            for e in emails_resp.json():
                if e.get("primary") and e.get("verified"):
                    email = e["email"]
                    break

    if not email:
        raise HTTPException(status_code=400, detail="Could not retrieve verified email from GitHub")

    full_name = (user_info.get("name") or user_info.get("login") or "").strip()
    jwt_token, _ = _upsert_oauth_user(email, full_name, "github", db)

    # Redirect to frontend — the SPA reads ?token= and stores it
    return RedirectResponse(f"{FRONTEND_URL}/dashboard?token={jwt_token}")
