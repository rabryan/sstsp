import datetime
import logging as log

def error(code, msg=""):
    log.info("{} error - {}".format(code, msg))
    raise abort(code)

def internal_error(msg=""):
    error(500, msg)

def tz_offset_seconds(tz_str):
    if tz_str[0] != '-':
        """ instead of requiring a '+' char which would
        need to be escaped, just infer it based on presence of 
        minus sign"""
        tz_str = "+" + tz_str
    try:
        dt = datetime.datetime.strptime(tz_str, "%z" ) 
    except ValueError as e:
        log.error("{}".format(e))
        log.error("invalid tz str {}".format(tz_str))
        return 0
    
    delta = dt.utcoffset()
    return delta.total_seconds()
