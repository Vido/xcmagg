from django.contrib import admin
from profiles.models import Profile

class ProfileAdmin(admin.ModelAdmin):
    list_display = (
        'id',
        'user',
        'version_tos',
        'last_tos',
        'last_verification',
    )
    list_filter = (
        'version_tos',
        'last_verification',
    )
    search_fields = (
        'user__username',
        'user__email',
    )
    readonly_fields = (
        #'node',
        'last_verification',
    )
    autocomplete_fields = (
        'user',
    )
    ordering = ('-last_verification',)

    fieldsets = (
        (None, {
            'fields': ('user', 'node')
        }),
        ('Terms of Service', {
            'fields': ('version_tos', 'last_tos')
        }),
        ('Verification', {
            'fields': ('last_verification',)
        }),
    )

admin.site.register(Profile, ProfileAdmin)