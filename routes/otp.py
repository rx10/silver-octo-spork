"""
OTP authentication routes.

POST /api/auth/otp/request  — send OTP to a @socratic.pro email
POST /api/auth/otp/verify   — verify OTP, return JWT + user
"""

import asyncio
import random
import string
import uuid
from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, field_validator
from sqlalchemy.orm import Session

from auth import create_access_token
from database import get_db
from models import OTPToken, User
from schemas import TokenResponse, UserOut
from services.email import send_otp_email

router = APIRouter(prefix="/api/auth/otp", tags=["otp"])

OTP_EXPIRY_MINUTES = 10
OTP_COOLDOWN_SECONDS = 60


class OTPRequest(BaseModel):
    email: str

    @field_validator("email")
    @classmethod
    def must_be_socratic(cls, v: str) -> str:
        v = v.strip().lower()
        if not v.endswith("@socratic.pro"):
            raise ValueError("Only @socratic.pro email addresses are allowed.")
        return v


class OTPVerify(BaseModel):
    email: str
    code: str

    @field_validator("email")
    @classmethod
    def normalise(cls, v: str) -> str:
        return v.strip().lower()


def _generate_code() -> str:
    return "".join(random.choices(string.digits, k=6))


@router.post("/request", status_code=200)
async def request_otp(body: OTPRequest, db: Session = Depends(get_db)):
    """
    Generate and email a 6-digit OTP.
    Rate-limited: one OTP per email per 60 seconds.
    """
    now = datetime.utcnow()

    # Rate-limit: reject if a fresh (unused, not expired) OTP was issued recently
    recent = (
        db.query(OTPToken)
        .filter(
            OTPToken.email == body.email,
            OTPToken.used == False,
            OTPToken.expires_at > now,
            OTPToken.created_at > now - timedelta(seconds=OTP_COOLDOWN_SECONDS),
        )
        .first()
    )
    if recent:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=f"An OTP was already sent. Please wait {OTP_COOLDOWN_SECONDS} seconds before requesting another.",
        )

    code = _generate_code()
    token = OTPToken(
        id=str(uuid.uuid4()),
        email=body.email,
        code=code,
        expires_at=now + timedelta(minutes=OTP_EXPIRY_MINUTES),
    )
    db.add(token)
    db.commit()

    # Send email in a thread so we don't block the event loop
    await asyncio.to_thread(send_otp_email, body.email, code)

    return {"detail": f"OTP sent to {body.email}. It expires in {OTP_EXPIRY_MINUTES} minutes."}


@router.post("/verify", response_model=TokenResponse)
def verify_otp(body: OTPVerify, db: Session = Depends(get_db)):
    """
    Validate OTP, create user if new, return JWT + user.
    """
    now = datetime.utcnow()

    token = (
        db.query(OTPToken)
        .filter(
            OTPToken.email == body.email,
            OTPToken.code == body.code,
            OTPToken.used == False,
            OTPToken.expires_at > now,
        )
        .order_by(OTPToken.created_at.desc())
        .first()
    )

    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired OTP.",
        )

    # Mark token as used
    token.used = True
    db.commit()

    # Find or create user
    user = db.query(User).filter(User.email == body.email).first()
    if not user:
        user = User(
            id=str(uuid.uuid4()),
            email=body.email,
            auth_provider="otp",
            subscription_tier="free",
        )
        db.add(user)
        db.commit()
        db.refresh(user)
    else:
        db.refresh(user)

    jwt = create_access_token(user.id)
    return TokenResponse(access_token=jwt, user=UserOut.model_validate(user))
