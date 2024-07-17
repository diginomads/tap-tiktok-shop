import hmac
import hashlib
import urllib.parse
from urllib.parse import urlencode, urlparse, parse_qs

def generate_signature(url, params, secret, body=None):
    # Parse URL and query parameters
    parsed_url = urlparse(url)
    queries = {**parse_qs(parsed_url.query), **params}

    # Flatten the query parameters and remove 'sign' and 'access_token'
    flattened_queries = {k: v[0] if isinstance(v, list) else v for k, v in queries.items()}
    filtered_queries = {k: flattened_queries[k] for k in flattened_queries if k not in ['sign', 'access_token']}

    # Sort the query parameters
    sorted_keys = sorted(filtered_queries.keys())

    # Concatenate all the parameters in the format of {key}{value}
    input_string = ''.join(f"{key}{filtered_queries[key]}" for key in sorted_keys)

    # Append the request path
    input_string = parsed_url.path + input_string

    # Append the request body if it's a POST request
    if body:
        input_string += urllib.parse.urlencode(body)

    print(input_string)

    # Wrap the string with the App secret
    input_string = secret + input_string + secret

    # Generate HMAC-SHA256 signature
    signature = hmac.new(secret.encode(), input_string.encode(), hashlib.sha256).hexdigest()
    return signature