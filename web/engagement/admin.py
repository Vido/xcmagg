# admin.py

from django import forms
from django.contrib import admin

from nodes.admin import NodeAdmin
from nodes.models import Node, NodeKind
from .models import Post, Vote


class PostInlineForm(forms.ModelForm):
    body = forms.CharField(widget=forms.Textarea)

    class Meta:
        model = Node
        fields = ("parent",)

    def save(self, commit=True):
        node = super().save(commit=False)
        node.kind = NodeKind.COMMENT

        if commit:
            node.save()

        comment, _ = Post.objects.get_or_create(node=node)
        comment.body = self.cleaned_data["body"]
        comment.save()

        return node


class PostInline(admin.TabularInline):
    model = Node
    fk_name = 'parent'
    form = PostInlineForm
    verbose_name = 'Post'
    verbose_name_plural = 'Posts'
    extra = 1

    fields = (
        'owner',
        'body',
        'comment_reply_to',
        'comment_depth',
        'comment_thread',
        'created_at',
    )

    readonly_fields = (
        'comment_reply_to',
        'comment_depth',
        'comment_thread',
        'created_at',
    )

    def get_queryset(self, request):
        qs = super().get_queryset(request)
        return qs.filter(kind=NodeKind.COMMENT)

    def comment_reply_to(self, obj):
        return obj.comment.reply_to if hasattr(obj, 'comment') else None

    def comment_depth(self, obj):
        return obj.comment.depth if hasattr(obj, 'comment') else 0

    def comment_thread(self, obj):
        return obj.comment.thread if hasattr(obj, 'comment') else ''

    comment_reply_to.short_description = 'Reply to'
    comment_depth.short_description = 'Depth'
    comment_thread.short_description = 'Thread'


# Contribute the Post/comment inline to the Node admin without the spine
# importing engagement.
NodeAdmin.inlines = (*NodeAdmin.inlines, PostInline)


@admin.register(Vote)
class VoteAdmin(admin.ModelAdmin):
    list_display = ("id", "node", "owner", "value", "created_at")
    list_filter = ("value", "created_at")
    search_fields = ("owner__username",)
    autocomplete_fields = ("node", "owner")
    readonly_fields = ("created_at",)
    ordering = ("-created_at",)


@admin.register(Post)
class PostAdmin(admin.ModelAdmin):
    list_display = (
        "slug",
        "node",
        "owner",
        "reply_to",
        "depth",
        "thread",
        "short_body",
    )
    list_filter = ("depth",)
    search_fields = ("body", "node__owner__username")
    autocomplete_fields = ("node", "reply_to")
    readonly_fields = ("depth", "thread")
    ordering = ("thread",)

    def owner(self, obj):
        return obj.node.owner

    owner.admin_order_field = "node__owner"
    owner.short_description = "Owner"

    def slug(self, obj):
        return obj.node.slug

    def short_body(self, obj):
        return (obj.body[:75] + "...") if len(obj.body) > 75 else obj.body

    short_body.short_description = "Body"
