from django.test import TestCase


class CheckUtils(TestCase):
    def test_injecting_base_class(self):
        from aragog.utils import inject_base

        class Q(object):
            pass

        class W:
            pass

        inject_base(Q, W)
