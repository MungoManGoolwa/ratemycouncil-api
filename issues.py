"""
Issue Reporting and Moderation System for RateMyCouncil
Handles user-submitted issues, moderation workflow, and status updates.
"""

from datetime import datetime
from typing import Dict, List, Optional
from enum import Enum

from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, func

from models import Issue, IssueStatusUpdate, User, Council
from trust_safety import TrustSafetyManager

class IssueStatus(Enum):
    PENDING = "pending"
    UNDER_REVIEW = "under_review"
    VERIFIED = "verified"
    REJECTED = "rejected"
    RESOLVED = "resolved"

class IssuePriority(Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

class IssueService:
    """Service for managing user-submitted issues"""

    def __init__(self, db: Session):
        self.db = db
        self.trust_safety = TrustSafetyManager(db)

    def submit_issue(self, user_id: int, council_id: int, title: str, description: str,
                    category: str, priority: str = "medium") -> Dict:
        """
        Submit a new issue for moderation
        Returns: {'success': bool, 'issue_id': int, 'moderation_result': Dict}
        """
        # Check trust & safety
        action_check = self.trust_safety.check_user_action(user_id, 'submit_issue')
        if not action_check['allowed']:
            return {
                'success': False,
                'error': action_check['reason'],
                'issue_id': None,
                'moderation_result': None
            }

        # Moderate content
        moderation = self.trust_safety.moderate_issue_submission(user_id, title, description)

        # Create issue
        issue = Issue(
            user_id=user_id,
            council_id=council_id,
            title=title,
            description=description,
            category=category,
            priority=priority,
            status=IssueStatus.PENDING.value,
            moderation_flags=moderation['flags'],
            moderation_confidence=moderation['confidence'],
            submitted_at=datetime.utcnow()
        )

        self.db.add(issue)
        self.db.commit()
        self.db.refresh(issue)

        # Record the action
        self.trust_safety.record_user_action(
            user_id,
            'submit_issue',
            {'issue_id': issue.id, 'council_id': council_id}
        )

        # Auto-approve if no flags
        if moderation['approved']:
            self._update_issue_status(issue.id, IssueStatus.UNDER_REVIEW.value, "Auto-approved")

        return {
            'success': True,
            'issue_id': issue.id,
            'moderation_result': moderation
        }

    def get_issues_for_moderation(self, status: str = None, limit: int = 50) -> List[Issue]:
        """Get issues pending moderation"""
        query = self.db.query(Issue)

        if status:
            query = query.filter(Issue.status == status)
        else:
            query = query.filter(Issue.status.in_([
                IssueStatus.PENDING.value,
                IssueStatus.UNDER_REVIEW.value
            ]))

        return query.order_by(Issue.submitted_at.desc()).limit(limit).all()

    def moderate_issue(self, issue_id: int, moderator_id: int, action: str,
                      reason: str = None, notes: str = None) -> bool:
        """
        Moderate an issue
        action: 'approve', 'reject', 'verify', 'resolve'
        """
        issue = self.db.query(Issue).filter(Issue.id == issue_id).first()
        if not issue:
            return False

        status_map = {
            'approve': IssueStatus.UNDER_REVIEW.value,
            'reject': IssueStatus.REJECTED.value,
            'verify': IssueStatus.VERIFIED.value,
            'resolve': IssueStatus.RESOLVED.value
        }

        if action not in status_map:
            return False

        new_status = status_map[action]

        # Update issue status
        success = self._update_issue_status(issue_id, new_status, reason, moderator_id, notes)

        if success:
            # Record moderation action
            self.trust_safety.record_user_action(
                moderator_id,
                'moderate_issue',
                {
                    'issue_id': issue_id,
                    'action': action,
                    'new_status': new_status,
                    'reason': reason
                }
            )

        return success

    def _update_issue_status(self, issue_id: int, status: str, reason: str = None,
                           moderator_id: int = None, notes: str = None) -> bool:
        """Update issue status with audit trail"""
        issue = self.db.query(Issue).filter(Issue.id == issue_id).first()
        if not issue:
            return False

        old_status = issue.status
        issue.status = status
        issue.updated_at = datetime.utcnow()

        # Create status update record
        status_update = IssueStatusUpdate(
            issue_id=issue_id,
            old_status=old_status,
            new_status=status,
            changed_by=moderator_id,
            reason=reason,
            notes=notes,
            changed_at=datetime.utcnow()
        )

        self.db.add(status_update)
        self.db.commit()

        return True

    def get_issue_stats(self) -> Dict:
        """Get issue statistics for dashboard"""
        stats = self.db.query(
            Issue.status,
            func.count(Issue.id).label('count')
        ).group_by(Issue.status).all()

        return {status: count for status, count in stats}

    def get_user_issues(self, user_id: int, limit: int = 20) -> List[Issue]:
        """Get issues submitted by a user"""
        return self.db.query(Issue).filter(
            Issue.user_id == user_id
        ).order_by(Issue.submitted_at.desc()).limit(limit).all()

    def get_council_issues(self, council_id: int, status: str = None, limit: int = 50) -> List[Issue]:
        """Get issues for a specific council"""
        query = self.db.query(Issue).filter(Issue.council_id == council_id)

        if status:
            query = query.filter(Issue.status == status)

        return query.order_by(Issue.submitted_at.desc()).limit(limit).all()

class ModerationDashboard:
    """Dashboard for moderators to manage issues"""

    def __init__(self, db: Session):
        self.db = db
        self.issue_service = IssueService(db)

    def get_pending_queue(self) -> Dict:
        """Get moderation queue statistics"""
        pending = self.issue_service.get_issues_for_moderation(IssueStatus.PENDING.value)
        under_review = self.issue_service.get_issues_for_moderation(IssueStatus.UNDER_REVIEW.value)

        return {
            'pending_count': len(pending),
            'under_review_count': len(under_review),
            'total_queue': len(pending) + len(under_review),
            'pending_issues': [self._format_issue_summary(issue) for issue in pending[:10]],
            'under_review_issues': [self._format_issue_summary(issue) for issue in under_review[:10]]
        }

    def get_moderation_stats(self) -> Dict:
        """Get comprehensive moderation statistics"""
        stats = self.issue_service.get_issue_stats()

        # Get recent activity
        recent_updates = self.db.query(IssueStatusUpdate).filter(
            IssueStatusUpdate.changed_at >= datetime.utcnow() - timedelta(days=7)
        ).count()

        return {
            'issue_counts': stats,
            'recent_activity': recent_updates,
            'moderation_rate': self._calculate_moderation_rate()
        }

    def _calculate_moderation_rate(self) -> float:
        """Calculate average time for issue moderation"""
        # Simplified calculation - in production would be more sophisticated
        recent_issues = self.db.query(Issue).filter(
            and_(
                Issue.status.in_([IssueStatus.VERIFIED.value, IssueStatus.REJECTED.value]),
                Issue.updated_at.isnot(None)
            )
        ).limit(100).all()

        if not recent_issues:
            return 0.0

        total_time = sum(
            (issue.updated_at - issue.submitted_at).total_seconds()
            for issue in recent_issues
        )

        return total_time / len(recent_issues) / 3600  # hours

    def _format_issue_summary(self, issue: Issue) -> Dict:
        """Format issue for dashboard display"""
        return {
            'id': issue.id,
            'title': issue.title,
            'category': issue.category,
            'priority': issue.priority,
            'status': issue.status,
            'submitted_at': issue.submitted_at.isoformat(),
            'moderation_flags': issue.moderation_flags,
            'council_name': issue.council.name if issue.council else 'Unknown'
        }