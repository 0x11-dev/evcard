# -*- coding: utf-8 -*-


"""Main `demo` CLI."""

import os
import sys
import logging

import click
import arrow


from evcard import __version__
from evcard.common import to_formatted_dict
from evcard.config import get_user_config, get_config

logger = logging.getLogger(__name__)


def version_msg():
    """Return the app version, location and Python powering it."""
    python_version = sys.version[:3]
    location = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    message = u'Evcard %(version)s from {} (Python {})'
    return message.format(location, python_version)


@click.command(context_settings=dict(help_option_names=[u'-h', u'--help']))
@click.version_option(__version__, u'-V', u'--version', message=version_msg())
@click.option(
    '-v', '--verbose',
    is_flag=True, help='Print debug information', default=False
)
@click.option(
    u'--config-file', type=click.Path(), default=None,
    help=u'User configuration file'
)
@click.option(
    u'--default-config', is_flag=False,
    help=u'Do not load a config file. Use the defaults instead'
)
@click.option(
    u'--task', type=click.Choice(['hello', 'index', 'desen', 'sample', 'kafka']), default='hello',
    help=u'task'
)
@click.option(
    u'--target', type=click.Choice(['order', 'vehicle', 'shop', 'member', 'month']),
    help=u'target'
)
@click.argument('date')
def main(verbose, config_file, default_config, task, target, date):

    get_user_config(
        config_file=config_file,
        default_config=default_config,
    )

    if task == 'hello':
        print(to_formatted_dict(get_config()))
        return
    elif task == 'kafka':
        from evcard.main import subscribe
        subscribe()
    elif task == 'desen':
        from evcard.desensitization import goo
        goo()
        return
    elif task == 'sample':
        from evcard.sample import go_sample
        go_sample()
        return
    else:
        from evcard.order import handle_order
        from evcard.shop import process_shop
        from evcard.vehicle import process_vehicle
        from evcard.member import process_membership
        from evcard.gather import sf
        from evcard.cache import (load_area_data, load_org_data)

        load_area_data()
        load_org_data()

        if len(date) == 6:
            _date = arrow.get(date, 'YYYYMM')
            days = (_date.shift(months=+1) - _date).days
        elif len(date) == 8:
            _date = arrow.get(date, 'YYYYMMDD')
            days = 1
        else:
            days = 0

        if target == 'vehicle':
            fn = process_vehicle
        elif target == 'shop':
            fn = process_shop
        elif target == 'order':
            fn = handle_order
        elif target == 'member':
            fn = process_membership
        elif target == 'month':
            fn = sf
        else:
            fn = None

        if days == 1:
            if fn:
                if target != 'member':
                    fn(_date.year, _date.month, _date.day)
            else:
                logger.debug('handling %s-%s-%s...', _date.year, _date.month, _date.day)
                handle_order(_date.year, _date.month, _date.day)
                process_shop(_date.year, _date.month, _date.day)
                process_vehicle(_date.year, _date.month, _date.day)
        elif days > 1:
            if fn:
                if target in ('member', 'month'):
                    fn(_date.year, _date.month)
                else:
                    for day in range(1, days + 1):
                        count = fn(_date.year, _date.month, day)
                        logger.debug("day %s --> %s", day, count)
            else:
                for day in range(1, days + 1):
                    logger.debug('handling %s-%s-%s...', _date.year, _date.month, day)
                    handle_order(_date.year, _date.month, day)
                    process_shop(_date.year, _date.month, day)
                    process_vehicle(_date.year, _date.month, day)

                process_membership(_date.year, _date.month)


if __name__ == "__main__":
    main()
