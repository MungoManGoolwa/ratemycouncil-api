"""
Trust & Safety Module for RateMyCouncil
Handles rate limiting, verification, and anti-abuse measures.
"""

import hashlib
import hmac
import secrets
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, func

from models import User, VerificationToken, AuditLog, IssueStatusUpdate
from database import SessionLocal

class RateLimiter:
    """Rate limiting for API endpoints"""

    def __init__(self, db: Session):
        self.db = db
        self.limits = {
            'submit_issue': {'requests': 5, 'window_minutes': 60},  # 5 issues per hour
            'vote': {'requests': 20, 'window_minutes': 60},  # 20 votes per hour
            'comment': {'requests': 10, 'window_minutes': 60},  # 10 comments per hour
            'api_general': {'requests': 100, 'window_minutes': 60},  # 100 requests per hour
        }

    def check_limit(self, user_id: int, action: str) -> Tuple[bool, int]:
        """
        Check if user is within rate limits
        Returns: (allowed, seconds_until_reset)
        """
        if action not in self.limits:
            return True, 0

        limit = self.limits[action]
        window_start = datetime.utcnow() - timedelta(minutes=limit['window_minutes'])

        # Count recent actions
        count = self.db.query(AuditLog).filter(
            and_(
                AuditLog.user_id == user_id,
                AuditLog.action == action,
                AuditLog.timestamp >= window_start
            )
        ).count()

        if count >= limit['requests']:
            # Find oldest action to calculate reset time
            oldest = self.db.query(AuditLog.timestamp).filter(
                and_(
                    AuditLog.user_id == user_id,
                    AuditLog.action == action,
                    AuditLog.timestamp >= window_start
                )
            ).order_by(AuditLog.timestamp.asc()).first()

            if oldest:
                reset_time = oldest.timestamp + timedelta(minutes=limit['window_minutes'])
                seconds_until_reset = max(0, int((reset_time - datetime.utcnow()).total_seconds()))
                return False, seconds_until_reset

        return True, 0

    def record_action(self, user_id: int, action: str, details: Optional[Dict] = None):
        """Record a user action for rate limiting"""
        audit_log = AuditLog(
            user_id=user_id,
            action=action,
            details=details or {},
            timestamp=datetime.utcnow()
        )
        self.db.add(audit_log)
        self.db.commit()

class VerificationService:
    """Email verification and user verification system"""

    def __init__(self, db: Session, secret_key: str = None):
        self.db = db
        self.secret_key = secret_key or "default-secret-change-in-production"

    def create_verification_token(self, user_id: int, email: str, purpose: str = "email_verification") -> str:
        """Create a verification token for user actions"""
        token_value = secrets.token_urlsafe(32)
        expires_at = datetime.utcnow() + timedelta(hours=24)

        # Hash the token for storage
        token_hash = self._hash_token(token_value)

        verification_token = VerificationToken(
            user_id=user_id,
            token_hash=token_hash,
            purpose=purpose,
            expires_at=expires_at,
            email=email
        )

        self.db.add(verification_token)
        self.db.commit()

        return token_value

    def verify_token(self, token: str, purpose: str = "email_verification") -> Optional[User]:
        """Verify a token and return the associated user if valid"""
        token_hash = self._hash_token(token)

        verification_token = self.db.query(VerificationToken).filter(
            and_(
                VerificationToken.token_hash == token_hash,
                VerificationToken.purpose == purpose,
                VerificationToken.expires_at > datetime.utcnow(),
                VerificationToken.used_at.is_(None)
            )
        ).first()

        if not verification_token:
            return None

        # Mark token as used
        verification_token.used_at = datetime.utcnow()
        self.db.commit()

        return verification_token.user

    def _hash_token(self, token: str) -> str:
        """Hash a token for secure storage"""
        return hmac.new(
            self.secret_key.encode(),
            token.encode(),
            hashlib.sha256
        ).hexdigest()

class AntiAbuseService:
    """Anti-abuse measures and content moderation"""

    def __init__(self, db: Session):
        self.db = db

    def detect_suspicious_activity(self, user_id: int) -> List[str]:
        """Detect suspicious user activity patterns"""
        warnings = []

        # Check for rapid submissions
        recent_issues = self.db.query(AuditLog).filter(
            and_(
                AuditLog.user_id == user_id,
                AuditLog.action == 'submit_issue',
                AuditLog.timestamp >= datetime.utcnow() - timedelta(hours=1)
            )
        ).count()

        if recent_issues > 10:
            warnings.append("High frequency issue submissions")

        # Check for repetitive content patterns
        # This would need more sophisticated analysis in production

        # Check voting patterns
        recent_votes = self.db.query(AuditLog).filter(
            and_(
                AuditLog.user_id == user_id,
                AuditLog.action == 'vote',
                AuditLog.timestamp >= datetime.utcnow() - timedelta(hours=1)
            )
        ).count()

        if recent_votes > 50:
            warnings.append("High frequency voting activity")

        return warnings

    def moderate_content(self, content: str, user_id: int) -> Dict:
        """
        Basic content moderation
        Returns: {'approved': bool, 'flags': List[str], 'confidence': float}
        """
        flags = []

        # Basic checks
        if len(content.strip()) < 10:
            flags.append("Content too short")

        if len(content) > 5000:
            flags.append("Content too long")

        # Check for excessive caps
        caps_ratio = sum(1 for c in content if c.isupper()) / len(content) if content else 0
        if caps_ratio > 0.3:
            flags.append("Excessive use of capital letters")

        # Check for spam patterns (simplified)
        spam_words = ['free money', 'click here', 'buy now', 'urgent']
        content_lower = content.lower()
        for word in spam_words:
            if word in content_lower:
                flags.append(f"Potential spam content: '{word}'")

        # Suspicious user activity
        user_warnings = self.detect_suspicious_activity(user_id)
        flags.extend(user_warnings)

        # Simple confidence score (higher = more likely spam)
        confidence = min(1.0, len(flags) * 0.2)

        return {
            'approved': len(flags) == 0,
            'flags': flags,
            'confidence': confidence
        }

class TrustSafetyManager:
    """Main trust & safety coordinator"""

    def __init__(self, db: Session):
        self.db = db
        self.rate_limiter = RateLimiter(db)
        self.verification = VerificationService(db)
        self.anti_abuse = AntiAbuseService(db)

    def check_user_action(self, user_id: int, action: str) -> Dict:
        """
        Comprehensive check before allowing user action
        Returns: {'allowed': bool, 'reason': str, 'wait_seconds': int}
        """
        # Check rate limits
        allowed, wait_seconds = self.rate_limiter.check_limit(user_id, action)
        if not allowed:
            return {
                'allowed': False,
                'reason': f'Rate limit exceeded. Try again in {wait_seconds} seconds.',
                'wait_seconds': wait_seconds
            }

        # Check user verification status for sensitive actions
        if action in ['submit_issue', 'vote']:
            user = self.db.query(User).filter(User.id == user_id).first()
            if not user or not user.email_verified:
                return {
                    'allowed': False,
                    'reason': 'Email verification required for this action.',
                    'wait_seconds': 0
                }

        return {
            'allowed': True,
            'reason': '',
            'wait_seconds': 0
        }

    def record_user_action(self, user_id: int, action: str, details: Optional[Dict] = None):
        """Record a user action"""
        self.rate_limiter.record_action(user_id, action, details)

    def moderate_issue_submission(self, user_id: int, title: str, description: str) -> Dict:
        """Moderate issue submission content"""
        content = f"{title} {description}"
        return self.anti_abuse.moderate_content(content, user_id)