import logging
from datetime import timedelta
from typing import List

from meteomatics.exceptions import (
    InternalServerError,
    NotFound,
    TooManyRequests,
)
from numpy import array_split, floor
from requests import ReadTimeout

ERROR_CODES = {
    NotFound: -404,
    ReadTimeout: -408,
    TooManyRequests: -429,
    InternalServerError: -500,
}

ERROR_PRINT = [NotFound, InternalServerError]


def get_meteomatics_error_info(e: Exception, request: dict) -> dict:
    """DOCSTRING."""
    request["error"] = repr(e)
    request["request_seconds"] = ERROR_CODES[type(e)]
    return request


def _reformat_not_found_request(request: dict) -> List[dict]:
    """DOCSTRING."""
    # clean up
    r = request.copy()
    error = r["error"]
    for ky in ["status", "date_requested", "error", "request_seconds"]:
        r.pop(ky)

    # determine the problem parameter
    good_parameters = [p for p in r["parameters"] if p not in error]

    if len(good_parameters) == r["parameters"]:
        # avoid possible insanity (i.e., infinite loops)
        return []
    else:
        r["parameters"] = good_parameters
        return [r]


def _reformat_read_time_out_request(request: dict) -> List[dict]:
    """DOCSTRING.

    (408) RequestTimeOut: First, split the request temporally. If not possible, split the
    the request spatially (i.e., reduce the number of coordinates requested per request).
    """
    # clean up
    r = request.copy()
    for ky in ["status", "date_requested", "error", "request_seconds"]:
        r.pop(ky)
    request_list = [r.copy(), r.copy()]

    # if we are only requesting one day, let's split spatially first
    # otherwise split by parameters
    if request["startdate"] == request["enddate"]:
        if len(request["coordinate_list"]) > 1:
            split_dim = "coordinate_list"
        else:
            split_dim = "parameters"
        splits = array_split(request[split_dim], 2)

    # otherwise, always split by days
    else:
        n_days = (request["enddate"] - request["startdate"]).days + 1
        n_day_split = int(floor(n_days / 2))
        date_split = r["startdate"] + timedelta(days=n_day_split)
        splits = [
            (r["startdate"], date_split),
            (date_split + timedelta(days=1), r["enddate"]),
        ]
        for i in range(2):
            request_list[i]["startdate"] = splits[i][0]
            request_list[i]["enddate"] = splits[i][1]

    return request_list


ERROR_FORMAT = {
    NotFound: _reformat_not_found_request,
    ReadTimeout: _reformat_read_time_out_request,
}


def reformat_failed_requests(request_list: List[dict]) -> List[dict]:
    """Identify and reformat failed requests in `request_list`.

    Desired behavior for Meteomatics API exceptions:

    (404) NotFound: Use regular expression to identify the problem parameter.
    Remove that parameter from the list, re-try the remaining parameters, and print the
    error message for the user.

    (408) RequestTimeOut: First, split the request temporally. If not possible, split the
    the request spatially (i.e., reduce the number of coordinates requested per request).

    (429) TooManyRequests: No more requests are available today on our account.

    (500) InternalServerError: Poorly understood error. For now, we will just report.
    """
    reformat_request_list = []

    # identify failed requests and reformat using error-based function
    n_failed = 0
    for r in request_list:
        if r["status"] == "FAIL":
            n_failed += 1

            error_code = r["request_seconds"]
            error_type = [
                error
                for error in ERROR_CODES.keys()
                if ERROR_CODES[error] == error_code
            ][0]

            if error_type in ERROR_FORMAT.keys():
                reformat_function = ERROR_FORMAT[error_type]
                reformat_request_list += reformat_function(r)
    logging.info("%s requests failed of %s", n_failed, len(request_list))

    return reformat_request_list


def print_meteomatics_request_report(request_list: List[dict]):
    n_completed = len(request_list)

    failed_requests = [r for r in request_list if r["status"] == "FAIL"]
    n_success = n_completed - len(failed_requests)

    logging.info("Summary of weather process performance:")
    logging.info("  Number of requests attempted: %s", n_completed)
    logging.info("  Number of successful requests: %s", n_success)

    if len(failed_requests) > 0:
        print_requests = []
        logging.info("Summary of request failures:")
        for error in ERROR_CODES.keys():
            this_error_requests = [
                r for r in failed_requests if r["request_seconds"] == ERROR_CODES[error]
            ]

            if not isinstance(this_error_requests, list):
                this_error_requests = [this_error_requests]
            if len(this_error_requests) > 0:
                logging.info(
                    "  %s requests failed due to: %s",
                    len(this_error_requests),
                    error.__name__,
                )
                if error in ERROR_PRINT:
                    print_requests += this_error_requests

        logging.info("List of failed requests yet to handle:")
        for r in print_requests:
            logging.info(r["error"])
            logging.info("   %s", r["zone"])
            logging.info("   %s", r["startdate"])
            logging.info("   %s", r["enddate"])
            logging.info("   %s", r["coordinate_list"])
            logging.info("   %s", r["parameters"])
