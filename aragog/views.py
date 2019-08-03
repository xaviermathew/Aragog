from django.apps import apps
from django.http import Http404
from django.shortcuts import render_to_response


def describe(request, model_name):
    model_name = model_name.lower()
    try:
        model = [m for m in apps.get_app_config('aragog').get_models() if m.__name__.lower() == model_name][0]
    except IndexError:
        raise Http404('Unknown model')
    else:
        return render_to_response('aragog/describe.html', context={'model': model})
