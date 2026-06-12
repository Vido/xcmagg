from typing import Optional

from django.conf import settings
from django.core.mail import EmailMultiAlternatives
from django.template.loader import render_to_string
from django.utils.translation import gettext_lazy as _

from magiclink.services import MagicLinkService, MagicEmailService


class YeetService:
    
    @staticmethod
    def yeet(
        to_email: str,
        subject: str,
        magic_url: str,
        template_name: str,
        context: Optional[dict] = None
    ) -> bool:

        ctx = context or {}
        ctx.update({
            'magic_url': magic_url,
        })

        """
        # Render HTML and text versions
        html_content = render_to_string(
            f'emails/{template_name}.html',
            ctx
        )
        """
        text_content = render_to_string(
            f'emails/{template_name}.txt',
            ctx
        )
        
        msg = EmailMultiAlternatives(
            subject=subject,
            body=text_content,
            from_email=settings.DEFAULT_FROM_EMAIL,
            to=[to_email]
        )
        # msg.attach_alternative(html_content, "text/html")

        # Postmark-specific metadata (ignored by console backend)
        if 'postmark' in settings.EMAIL_BACKEND.lower():
            msg.metadata = {
                'type': 'magic_url',
                'template': template_name
            }
            msg.track_opens = False
        
        msg.send(fail_silently=False)


    def authenticate(*, user, request, next_url):

        token = MagicLinkService.create_token(
            user=user,
            request=request,
            next_url=next_url
        )
        
        print('token', token)
        print('token', token)
        print('token', token)
        print('token', token)
        print('token', token)
        url = MagicLinkService.url(request, token)
        return YeetService.yeet(
            user.email,
            _('Log in link is here'),
            url, 'authentication'
        )

    def verify(*, email, request, next_url):

        token = MagicEmailService.create_claim(
            email=email,
            request=request,
            next_url=next_url
        )
        print('token', token)
        print('token', token)
        print('token', token)
        print('token', token)
        print('token', token)
        url = MagicEmailService.url(request, token)

        return YeetService.yeet(
            email,
            _('Verify your registration'),
            url, 'registration'
        )