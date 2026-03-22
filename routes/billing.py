"""
Stripe billing routes — /api/billing

Env vars required:
  STRIPE_SECRET_KEY     — sk_live_... or sk_test_...
  STRIPE_WEBHOOK_SECRET — whsec_... from Stripe dashboard → Webhooks
  STRIPE_PRO_PRICE_ID   — price_... for the Pro monthly plan
  FRONTEND_URL          — https://socratic.pro

Stripe webhook events handled:
  checkout.session.completed          → upgrade user to "pro"
  customer.subscription.deleted       → downgrade user to "free"
  customer.subscription.paused        → downgrade user to "free"
"""

import logging
import os

import stripe
from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.orm import Session

from auth import get_current_user
from database import get_db
from models import User

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/billing", tags=["billing"])

STRIPE_SECRET_KEY     = os.getenv("STRIPE_SECRET_KEY", "")
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "")
STRIPE_PRO_PRICE_ID   = os.getenv("STRIPE_PRO_PRICE_ID", "")
FRONTEND_URL          = os.getenv("FRONTEND_URL", "https://socratic.pro")

if STRIPE_SECRET_KEY:
    stripe.api_key = STRIPE_SECRET_KEY


def _require_stripe():
    if not STRIPE_SECRET_KEY:
        raise HTTPException(status_code=500, detail="Stripe not configured on this server")


# ── routes ────────────────────────────────────────────────────────────────────

@router.get("/status")
def billing_status(current_user: User = Depends(get_current_user)):
    """Return the current user's subscription tier."""
    return {
        "subscription_tier": current_user.subscription_tier,
        "is_pro": current_user.subscription_tier in ("pro", "enterprise"),
    }


@router.post("/checkout")
def create_checkout_session(
    current_user: User = Depends(get_current_user),
):
    """
    Create a Stripe Checkout session for the Pro plan.
    Returns { checkout_url } — redirect the user to this URL.
    """
    _require_stripe()
    if not STRIPE_PRO_PRICE_ID:
        raise HTTPException(status_code=500, detail="STRIPE_PRO_PRICE_ID not configured")

    try:
        session = stripe.checkout.Session.create(
            payment_method_types=["card"],
            mode="subscription",
            line_items=[{"price": STRIPE_PRO_PRICE_ID, "quantity": 1}],
            customer_email=current_user.email,
            success_url=f"{FRONTEND_URL}/dashboard?upgraded=true",
            cancel_url=f"{FRONTEND_URL}/pricing",
            metadata={"user_id": current_user.id},
        )
    except stripe.StripeError as e:
        logger.error(f"Stripe checkout error: {e}")
        raise HTTPException(status_code=502, detail=str(e))

    return {"checkout_url": session.url}


@router.post("/portal")
def customer_portal(current_user: User = Depends(get_current_user)):
    """
    Create a Stripe Customer Portal session so the user can manage
    their subscription, update payment method, or cancel.
    Returns { portal_url }.
    """
    _require_stripe()

    # Look up the Stripe customer by email
    customers = stripe.Customer.list(email=current_user.email, limit=1)
    if not customers.data:
        raise HTTPException(
            status_code=404,
            detail="No billing account found. Please subscribe first.",
        )

    try:
        session = stripe.billing_portal.Session.create(
            customer=customers.data[0].id,
            return_url=f"{FRONTEND_URL}/settings",
        )
    except stripe.StripeError as e:
        logger.error(f"Stripe portal error: {e}")
        raise HTTPException(status_code=502, detail=str(e))

    return {"portal_url": session.url}


@router.post("/webhook")
async def stripe_webhook(request: Request, db: Session = Depends(get_db)):
    """
    Receive and verify Stripe webhook events.
    Register this URL in the Stripe dashboard under Webhooks.
    """
    _require_stripe()

    payload = await request.body()
    sig     = request.headers.get("stripe-signature", "")

    try:
        event = stripe.Webhook.construct_event(payload, sig, STRIPE_WEBHOOK_SECRET)
    except stripe.SignatureVerificationError:
        raise HTTPException(status_code=400, detail="Invalid webhook signature")
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    event_type = event["type"]
    data       = event["data"]["object"]

    # ── Successful checkout → upgrade ─────────────────────────────────────────
    if event_type == "checkout.session.completed":
        user_id = (data.get("metadata") or {}).get("user_id")
        if user_id:
            user = db.query(User).filter(User.id == user_id).first()
            if user:
                user.subscription_tier = "pro"
                db.commit()
                logger.info(f"Upgraded user {user_id} to pro")

    # ── Subscription cancelled / paused → downgrade ───────────────────────────
    elif event_type in ("customer.subscription.deleted", "customer.subscription.paused"):
        customer_id = data.get("customer")
        if customer_id:
            try:
                customer = stripe.Customer.retrieve(customer_id)
                email = customer.get("email", "")
                if email:
                    user = db.query(User).filter(User.email == email).first()
                    if user:
                        user.subscription_tier = "free"
                        db.commit()
                        logger.info(f"Downgraded {email} to free")
            except stripe.StripeError as e:
                logger.error(f"Webhook customer retrieval error: {e}")

    return {"received": True}
