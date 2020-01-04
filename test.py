"""
TODO: This.
"""

import os
import subprocess
import unittest
from threading import Thread
from time import sleep
from timeit import timeit

import easyrpc


class RequestHandler(easyrpc.OSCommandRequestHandler):
    @staticmethod
    def rpc_raise_error() -> None:
        """Raises an error for testing server errors."""

        raise Exception('This is a test error.')

    @easyrpc.cache
    def rpc_expensive_calculation(self, *args, **kwargs):
        sleep(0.25)
        return args, kwargs


class Test(unittest.TestCase):
    def setUp(self) -> None:
        self.server = easyrpc.Server(RequestHandler)

        self.server_thread = Thread(target=self.server.serve_forever, kwargs={'poll_interval': 0.1})
        self.server_thread.start()

        self.client = easyrpc.Client('localhost')

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server_thread.join()
        self.server.server_close()

    def test_cache(self) -> None:
        # Call the "expensive_calculation" function twice with the same arguments
        results = []
        dt0, dt1 = [
            timeit(
                'results.append(self.client.expensive_calculation(123, foo="bar"))',
                number=1,
                globals={'self': self, 'results': results})
            for _ in range(2)
        ]

        # Make sure the results from the two calls are the same
        self.assertEqual(results[0], results[1])

        # Make sure the second call took less time than the first
        self.assertLess(dt1, dt0)

    def test_get_functions_human_readable(self) -> None:
        self.assertIsInstance(self.client.get_functions(), str)

    def test_get_functions_not_human_readable(self) -> None:
        functions = self.client.get_functions(human_readable=False)
        self.assertIsInstance(functions, dict)

        for key, value in functions.items():
            self.assertIsInstance(key, str)

            if value is not None:
                self.assertIsInstance(value, str, msg=f'Key "{key}" value is not an instance of str.')

    def test_invalid_function(self) -> None:
        self.assertRaises(easyrpc.BadRequestError, self.client.this_does_not_exist)

    def test_run_command(self) -> None:
        # Command to list the root directory
        ls_command = ('ls', '/') if os.name == 'posix' else ('dir', 'C:\\')

        result = self.client.run_command(ls_command)

        self.assertIsInstance(result, subprocess.CompletedProcess)

        self.assertIsInstance(result.args, tuple)
        self.assertIsInstance(result.returncode, int)
        self.assertIsInstance(result.stdout, str)
        self.assertIsInstance(result.stderr, str)

    def test_function_error(self) -> None:
        self.assertRaises(Exception, self.client.raise_error)


if __name__ == '__main__':
    unittest.main()
