import datetime

import dill


def timestr() -> str:
    return datetime.datetime.now().isoformat()


def run_dilled_function(dilled_function: bytes):
    try:
        f = dill.loads(dilled_function)
        res = f()
        return res
    except Exception as e:
        import traceback

        traceback.print_exc()
        raise e
