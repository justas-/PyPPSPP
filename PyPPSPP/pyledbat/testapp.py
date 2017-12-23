"""
Copyright 2017, J. Poderys, Technical University of Denmark

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Test LEDBAT implementation in iperf-ish way

"""
import logging
import argparse

from testledbat import test_ledbat

logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(asctime)s %(message)s')

def main():
    """Main entrance point, mainly to stop PyLint from nagging"""

    # Setup the command line parser
    parser = argparse.ArgumentParser(description='LEDBAT Test program')

    parser.add_argument('--role', help='Role of the instance {client|server}. Server ignores all other arguments!', default='server')
    parser.add_argument('--remote', help='IP Address of the test server')
    parser.add_argument('--makelog', help='Save runtime values into CSV file', action='store_true')
    parser.add_argument('--log-name', help='Name of the log file (replace default UnixTime-IP-Port)')
    parser.add_argument('--log-dir', help='Directory to place results file')
    parser.add_argument('--time', help='Time to run the test', type=int)
    parser.add_argument('--parallel', help='Number of parallel streams to send', type=int)
    parser.add_argument('--ledbat-set-target', help='Set LEDBAT target queuing delay', type=int)
    parser.add_argument('--ledbat-set-allowed-increase', help='Set LEDBAT allowed cwnd increase factor', type=float)

    # Parse the command line params
    args = parser.parse_args()

    # Run the test
    test_ledbat(args)

# Move scope to main() (I hate PyLint...)
if __name__ == '__main__':
    main()
