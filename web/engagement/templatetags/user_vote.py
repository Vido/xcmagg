from django import template

register = template.Library()

@register.filter
def user_vote(node, user):
    action = node.user_vote(user)
    return action.name if action else None