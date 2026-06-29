import secrets
from typing import Dict, Tuple

from django.urls import reverse
from django.core.cache import cache
from django.contrib.auth import get_user_model
from allauth.account.models import EmailAddress

from magiclink.models import Scope, Event, AuditEvent

User = get_user_model()


class BaseService:

    """
        This is a OTT only
        # TODO: Offer OPT
    """

    PREFIX = None

    @classmethod
    def _set(klass, data: Dict, timeout: int) -> str:
        token = secrets.token_urlsafe(32)
        cache.set(
            f"{klass.PREFIX}:{token}", data,
            timeout=timeout
        )
        return token

    @classmethod
    def _burn(klass, token: str) -> Dict:
        if not token:
            raise TokenInvalid("Token is required")
        key = f'{klass.PREFIX}:{token}'
        data = cache.get(key)
        cache.delete(key) # Consume token (single-use)
        if not data:
            raise TokenExpired("Token has expired or been consumed")
        return data

    @classmethod
    def rotate(klass, token: str) -> Tuple[str, str]:
        data = klass._burn(token)
        token = klass._set(data, data.get('ttl', 60))
        return token, data

    @classmethod
    def _validate(klass, token) -> Dict:
        return klass._burn(token)

    @staticmethod
    def _sanitize(request, next_url) -> str: 
        # Validate next_url to prevent open redirect
        from django.utils.http import url_has_allowed_host_and_scheme
        if not url_has_allowed_host_and_scheme(
            next_url,
            allowed_hosts={request.get_host()},
            require_https=request.is_secure()
        ):
            return ''
        return next_url

    @staticmethod
    def _get_IP_UA(request) -> str:
        return request.META.get("HTTP_CF_CONNECTING_IP"), request.META.get("HTTP_USER_AGENT", "")

    @classmethod
    def _redeem(klass, token, ip_address=None, user_agent=None):

        try:
            data = klass._validate(token)
            AuditEvent.objects.create(
                email=data.get('email'),
                event_type=Event.SUCCESS,
                scope=data.get('scope'),
                ip_address=ip_address,
                user_agent=user_agent,
            )
        except TokenError as e:
            AuditEvent.objects.create(
                event_type=Event.FAILURE,
                ip_address=ip_address,
                user_agent=user_agent,
            )
            raise

        return data


class MagicLinkService(BaseService):

    PREFIX = "magic_link"

    @staticmethod
    def url(request, token) -> str:
        return request.build_absolute_uri(
            reverse('magic-login',
                kwargs={"token": token},
            )
        )

    @staticmethod
    def create_token(*, user, request,
        scope: Scope=Scope.QRCODE,
        next_url: str = '/',
        ttl: int = 60
    ) -> str:
        next_url = BaseService._sanitize(request, next_url)
        ip, ua = BaseService._get_IP_UA(request)
        return MagicLinkService._create_token(
            user, scope, next_url, ttl, ip, ua
        )

    @staticmethod
    def redeem_token(*, token: str, request) -> Tuple[User, str]:
        ip, ua = BaseService._get_IP_UA(request)
        data = MagicLinkService._redeem(token, ip, ua)
        return data['user'], data['next_url']

    @staticmethod
    def _create_token(user, scope, next_url, ttl, ip_address, user_agent):
        """
        scope: Scope.QRCODE or Scope.EMAIL
        ttl: seconds until expiration
        next_url: where to redirect after login
        """
        token = MagicLinkService._set(
            {
                'ttl': ttl,
                'scope': scope,
                'user_id': user.id,                
                'next_url': next_url,
            },
            timeout=ttl
        )
        
        AuditEvent.objects.create(
            email=user.email,
            event_type=Event.CREATED,
            scope=scope,
            ip_address=ip_address,
            user_agent=user_agent,
        )
        
        return token

    @staticmethod
    def _validate(token) -> Dict:

        data = MagicLinkService._burn(token)
        try:
            user = User.objects.get(id=data['user_id'])
        except User.DoesNotExist:
            raise UserNotFound("User no longer exists")
        
        data['user'] = user
        data['email'] = user.email

        return data

    # AuditEvent should be cache with ttl=90days?
    #@staticmethod
    #def cleanup_old_events(days=90):
    #    cutoff = timezone.now() - timedelta(days=days)
    #    deleted_count, _ = AuditEvent.objects.filter(created__lt=cutoff).delete()
    #    return deleted_count


class MagicEmailService(BaseService):

    PREFIX = "email_claim"

    @staticmethod
    def url(request, token) -> str:
        return request.build_absolute_uri(
            reverse('onboarding',
                kwargs={"token": token},
            )
        )

    @staticmethod
    def create_claim(*,
        email: str,
        request,
        next_url: str = '/',
        ttl: int = 300
    ) -> str:
        next_url = BaseService._sanitize(request, next_url)
        ip, ua = BaseService._get_IP_UA(request)
        return MagicEmailService._create_claim(
            email, next_url, ttl, ip, ua
        )

    @staticmethod
    def redeem_claim(*, token: str, request) -> Tuple[str, str]:
        ip, ua = BaseService._get_IP_UA(request)        
        data = MagicEmailService._redeem(token, ip, ua)
        return data['email'], data['next_url']

    @staticmethod
    def _create_claim(email, next_url, ttl, ip_address, user_agent):
        """
        Creates an email ownership claim. Does NOT create a User.
        """

        #from django.core.signing import TimestampSigner
        #signer = TimestampSigner()
        # Better? Allows multiple register atempts
        # No need to rotate the token

        if EmailAddress.objects.filter(email=email, verified=True).exists():
            raise TokenInvalid("This email already exists")

        token = MagicEmailService._set(
            {
                "ttl": ttl,
                "email": email,
                'scope': Scope.EMAIL,
                'next_url': next_url,
            },
            timeout=ttl,
        )

        AuditEvent.objects.create(
            email=email,
            event_type=Event.CREATED,
            scope=Scope.EMAIL,
            ip_address=ip_address,
            user_agent=user_agent,
        )

        return token


class TokenError(Exception):
    pass


class TokenExpired(TokenError):
    """Token has expired or been consumed"""
    pass


class TokenInvalid(TokenError):
    """Token does not exist or is malformed"""
    pass


class UserNotFound(TokenError):
    """User associated with token no longer exists"""
    pass