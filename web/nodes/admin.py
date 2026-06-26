from django import forms
from django.contrib import admin
from django.utils.html import format_html

from media.models import Photo
from nodes.models import Node, NodeKind
from engagement.models import Post, Vote
from catalog.models import RetailerLink


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
        #if self.kind in [NodeKind.COMMENT]:
        #    return None
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


"""
class PostBobyInline(admin.TabularInline):
    model = Post
    fk_name = 'node'
    form = PostInlineForm
    verbose_name = 'Body'
    extra = 0
    readonly_fields = [
        'reply_to',
        'depth',
        'thread',
    ]
    fields = ['body'] + readonly_fields
"""


class PhotoInline(admin.TabularInline):
    model = Photo
    extra = 0
    readonly_fields = ('image_preview', 'created_at')
    fields = ('image', 'image_preview', 'created_at')  # show both

    def image_preview(self, obj):
        if obj.image:
            return format_html('<img src="{}" style="height: 75px;"/>', obj.image.url)
        return 'N/A'
    image_preview.short_description = 'Preview'

class RetailerLinkInline(admin.TabularInline):
    model = RetailerLink
    extra = 0

@admin.register(Node)
class NodeAdmin(admin.ModelAdmin):
    list_display = (
        'shortcode',
        'slug',
        'kind',
        'visibility',
        'created_at',
    )
    list_filter = ('kind', 'visibility')
    search_fields = ('title', 'shortcode')
    readonly_fields = ('shortcode', 'created_at', 'updated_at')

    inlines = (
        PhotoInline,
        PostInline,
        RetailerLinkInline,
    )