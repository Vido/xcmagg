from django import forms
from django.conf import settings
from django.utils import timezone

from allauth.account.forms import SignupForm, LoginForm

from allauth.account.adapter import get_adapter
from allauth.account.models import EmailAddress

from profiles.models import Profile
from profiles.turnstile import TurnstileField
from nodes.models import Node, NodeKind

from allauth.account.adapter import DefaultAccountAdapter


class AccountAdapter(DefaultAccountAdapter):
    def get_login_redirect_url(self, request):
        return "/"  # or dynamic logic

    def get_logout_redirect_url(self, request):
        return "/welcome/"  # safe post-logout redirect

    def confirm_email_allowed(self, request, email):
        # control if email confirmation is required
        return True

    def is_open_for_signup(self, request):
        return False

    def clean_email(self, email):
        """Return a cleaned version of the email (used for deduplication)."""
        email = email.lower().strip()
        local, at, domain = email.partition("@")
        if domain in ["gmail.com", "googlemail.com"]:
            local = local.split("+", 1)[0]  # strip +tags only for Gmail
        return f"{local}@{domain}"


class TurnstileLoginForm(LoginForm):
    captcha = TurnstileField(required=True)

    def __init__(self, *args, **kwargs):
        self.request = kwargs.get("request")
        super().__init__(*args, **kwargs)
        # Optional: control placement
        self.fields["captcha"].request = self.request
        self.fields.move_to_end('captcha')


class CustomSignupForm(SignupForm):
    """
    Passwordless signup form for magic link onboarding.
    Handles TOS acceptance and Profile/Node creation.
    """

    add_password = forms.BooleanField(
        required=False,
        initial=False,
        label="Set a password (optional)"
    )
    
    accept_tos = forms.BooleanField(
        required=True,
        label="I accept the Terms of Service",
        error_messages={
            'required': 'You must accept the Terms of Service to continue.'
        }
    )

    captcha = TurnstileField(required=True)

    def __init__(self, *args, email=None, **kwargs):
        self.request = kwargs.pop("request", None)
        # Pre-fill initial data before calling super()
        if email:
            if 'initial' not in kwargs:
                kwargs['initial'] = {}
            kwargs['initial']['email'] = email

            # Generate suggested username from email
            if 'username' not in kwargs['initial']:
                adapter = get_adapter()
                kwargs['initial']['username'] = adapter.generate_unique_username([
                    email.split('@')[0],
                    email,
                ])

        super().__init__(*args, **kwargs)
        
        # Disable email field (already verified)
        if 'email' in self.fields:
            #self.fields['email'].widget.attrs['disabled'] = True # Breaks Form
            self.fields['email'].widget.attrs['readonly'] = True

        # Username is required and editable
        if 'username' in self.fields:
            self.fields['username'].required = True
            self.fields['username'].help_text = 'You can edit this suggested username'

        # Make passwords optional (passwordless by default)
        if 'password1' in self.fields:
            self.fields['password1'].required = False
        if 'password2' in self.fields:
            self.fields['password2'].required = False

        self.fields["captcha"].request = self.request
        self.fields.move_to_end('captcha')

    def clean_email(self):
        """Email is disabled in form, retrieve from initial data."""
        return self.initial.get('email')

    def clean(self):
        cleaned_data = super().clean()
        add_password = cleaned_data.get('add_password')
        password1 = cleaned_data.get('password1')
        password2 = cleaned_data.get('password2')
        
        # If user wants password, validate it
        if add_password:
            if not password1 or not password2:
                raise forms.ValidationError(
                    "Please enter a password or uncheck 'Set a password'"
                )
            if password1 != password2:
                raise forms.ValidationError("Passwords don't match")
        else:
            # Clear passwords if not wanted (passwordless signup)
            cleaned_data['password1'] = ''
            cleaned_data['password2'] = ''
        
        return cleaned_data

    def save(self, request):
        user = super().save(request)
        raw_email = self.cleaned_data.get('email')
        adapter = get_adapter()
        cleaned_email = adapter.clean_email(raw_email)

        # Store both emails in EmailAddress
        # Raw email -> primary eg. john+tag@gmail.com
        EmailAddress.objects.update_or_create(
            user=user,
            email=raw_email,
            defaults={'primary': True, 'verified': True}
        )

        # Cleaned email -> secondary eg. john@gmail.com
        if raw_email != cleaned_email:
            EmailAddress.objects.update_or_create(
                user=user,
                email=cleaned_email,
                defaults={'primary': False, 'verified': True}
            )

        return user

    def custom_signup(self, request, user):
        node = Node.objects.create(
            owner=user,
            title=user.username,
            kind=NodeKind.USER_PROFILE
        )

        Profile.objects.create(
            user=user,
            node=node,
            version_tos='v0',
            last_tos=timezone.now(),
        )