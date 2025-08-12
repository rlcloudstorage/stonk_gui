"""src/pkg/config_srv/utils\n
get_arg_value()\n
update_debug()\n
update_list()\n
write_file()"""

import logging

from configparser import ConfigParser

from pkg import DEBUG


logger = logging.getLogger(__name__)


def get_arg_value(ctx: dict) -> str:
    """Return first value from arguments tuple."""
    if DEBUG:
        logger.debug(f"get_arg_value(ctx={ctx}")

    new_value = ctx["interface"]["arguments"][0]
    return new_value


def update_debug(ctx: dict):
    """Enter any character to toggle debug (logging) status."""
    if DEBUG:
        logger.debug(f"update_debug(ctx={ctx}")

    # Toggle current boolean value
    new_value = not ctx["default"]["debug"]
    return new_value


def update_list(ctx: dict):
    """Set the default list of items to download."""
    # Get service, arguments, and opt_trans
    service = ctx["interface"]["service"]
    arguments = list(ctx["interface"]["arguments"])
    opt_trans = ctx["interface"]["opt_trans"]

    cur_list = ctx[service][opt_trans].split(" ")

    if DEBUG:
        logger.debug(f"update_list(ctx={ctx})")

    extend_list, remove_list = [], []  # create lists

    # Add symbols to extend_list/remove_list
    for item in arguments:
        item = item.upper().strip()

        if item in cur_list:
            remove_list.append(item)
        else:
            extend_list.append(item)

    # Extend/remove items in symbol_list
    if extend_list:
        cur_list.extend(extend_list)
    if remove_list:
        for r in remove_list:
            cur_list.remove(r)

    # Convert symbol list to string
    new_value = ", ".join(cur_list).replace(",", "")
    return new_value


def write_file(ctx: dict):
    """Write new value to the appropriate config file"""
    if DEBUG:
        logger.debug(f"write_file(ctx={ctx})")

    # Extract some config info from context object
    config_name = ctx["interface"]["config_file"]
    config_file = ctx["default"][config_name]
    section = ctx["interface"]["section"]
    option = ctx["interface"]["opt_trans"]
    new_value = ctx["interface"]["new_value"]

    # Create getlist() converter, used for reading ticker symbols
    config_obj = ConfigParser(allow_no_value=True, converters={"list": lambda x: [i.strip() for i in x.split(",")]})
    try:
        config_obj.read(config_file)
    except Exception as e:
        print(e)

    try:
        config_obj.set(section, option, str(new_value))
    except Exception as e:
        print(e)

    # import sys
    # config_obj.write(sys.stdout)
    with open(config_file, "w") as cf:
        config_obj.write(cf)
