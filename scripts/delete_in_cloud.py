#!/usr/bin/env python

# Delete a file in the cloud (or possibly an entire directory tree, the semantics may depend on the cloud platform)

import cloudstorage
import argparse
import sys
import os

def main():
    parser = argparse.ArgumentParser(description='Delete an object from the cloud.')
    parser.add_argument('platform', type=str.lower, help="Cloud platform to use.", choices=['s3', 'googledrive'])
    parser.add_argument('path', type=str.lower, help="POSIX path to object to delete.")
    parser.add_argument("--platform_args", type=str, default = None, help="Arguments needed to initialize each platform")

    args = parser.parse_args()
    
    if args.platform_args is None:
        if args.platform == "s3":            
            args.platform_args = os.getenv("ENDPOINT_URL", default="https://s3-west.nrp-nautilus.io")
        elif args.platform == "googledrive":
            args.platform_args = f"{os.environ['HOME']}/.config/gspread/service_account.json"
        else:
            print(f"Unknown platform: {args.platform}", file=sys.stderr)
            return 1

    storage = cloudstorage.initialize_cloud_storage(args.platform, args.platform_args)

    storage.delete(args.path)

if __name__ == '__main__':    
    sys.exit(main())



