import asyncio
import nest_asyncio
from typing import Iterable, Union, Optional, Tuple


class Async:
    """
    Object to de-sync anything
    Attributes:
        semaphore: an asyncio.Semaphore object to limit the amount of concurrent tasks. Default = 100
        loop: the asyncio event loop
        coroutines: a list of the coroutines to be run
        output: a dictionary of the functions inputted, key'd by the index it was run by
    Methods:
        append_coroutines: a method for calling the private async method __append_coroutines to put coroutines into the coroutines list
        run: a method for running the coroutines in the event loop, blocking until complete, returns the output attribute
        number_of_coroutines: method for getting the length of the coroutine list
        clear_coroutines: method for clearing coroutines from the list
    Example:
        import numpy as np

        # Initialize the asyncer
        example = Async()

        # Setup the coroutines np.sin(1), np.sin(2), np.sin(3), np.sin(50)
        example.append_coroutines(np.sin, [(1,), (2,), (3,), (50,)])

        # Setup a pause
        example.append_coroutines(time.sleep, [(10,)])

        # Setup the coroutines np.cos(1), np.cos(2), np.cos(3), np.cos(50)
        example.append_coroutines(np.cos, [(1,), (2,), (3,), (50,)])

        # Run the coroutines with asyncio
        data = example.run()

        print(data)
        > {0: 0.8414709848078965, 1: 0.9092974268256817, 5: -0.4161468365471424,
           2: 0.1411200080598672, 6: -0.9899924966004454, 3: -0.26237485370392877,
           7: 0.9649660284921133, 4: 0.5403023058681398, 4: None}}
    """

    def __init__(self, error: Union[asyncio.TimeoutError, Exception] = asyncio.TimeoutError, timeout: Union[float, int, None] = None, semaphore: int = 100):
        """
        Async initializer function
        Arguments:
            error: The exception to raise on a timeout
            timeout: Float describing how many seconds to run the tasks before timeout
            semaphore: integer to set the semaphore limiter by
        """
        self.semaphore = asyncio.Semaphore(semaphore)   # Set semaphore limit
        self.loop = asyncio.get_event_loop()            # Access asyncio event loop
        self.coroutines = []                            # Initialize variables
        self.output = {}

        self.error = error
        self.timeout = timeout

    def append_coroutines(self, f: callable, args: Iterable[tuple]):
        """
        Synchronous entry point for adding routines to the asyncio event loop
        Arguments:
            f: the callable to be called in the coroutine
            args: list of tuples, each of which are the arguments for 1 coroutine
        """
        self.loop.run_until_complete(self.__append_coroutines(f, args))  # Append the coroutines as coroutines via the asynchronous appender

    def run(self, sort: bool = True) -> Union[dict, list]:
        """
        Method for running the coroutines appended. Blocks until complete
        Arguments:
            sort: boolean. Returns af list with the output in the same order as the input if True. If False returns a dict key'd by the input index
        Returns:
            dict or list: key is the index if the coroutine in the coroutine list, value is the coroutine output.
                          If sort=True then out is a sorted list
        """
        self.output = {}                             # Clear the output before running
        self.loop.run_until_complete(self.__main())  # Run main function until complete

        self.clear_coroutines()

        if sort:
            return [self.output[key] for key in sorted(self.output.keys())]     # Output sorted list if sort
        else:
            return self.output                                                  # Otherwise output indexed dict

    def number_of_coroutines(self) -> int:
        """
        Convenience method for getting the number of coroutines in the coroutines list
        Returns:
            int: the length of the coroutines list
        """
        return len(self.coroutines)

    def clear_coroutines(self, indexes: Optional[Tuple[int]] = None):
        """
        Method for clearing the coroutines from the list. Optional, if not given then it clears the entire list
        Arguments:
            indexes: Tuple of indexes to clear, optional
        """
        if indexes is None:
            [coroutine.close() for coroutine in self.coroutines]
            self.coroutines = []
        else:
            [self.coroutines.pop(i).close() for i in reversed(sorted(indexes))]

    async def __append_coroutines(self, f: callable, args: Iterable[tuple]):
        """
        Asynchronous private method for appending the function calls as coroutines
        Arguments:
            f: the callable to be called in the coroutine
            args: list of tuples, each of which are the arguments for 1 coroutine
        """
        current_addresses = self.number_of_coroutines()
        self.coroutines += [self.__de_sync(f, x, current_addresses + n) for n, x in enumerate(args)]  # Appends the de-synced functions as coroutines

    async def __main(self):
        """
        Asynchronous private method for running all of the coroutines
        """
        if not self.coroutines:
            return                  # Don't run if no coroutines have been added
        async with self.semaphore:  # Limit with semaphore
            try:
                await asyncio.wait_for(self.__intermediary_main(), timeout=self.timeout)  # Run all the coroutines
            except asyncio.TimeoutError:
                raise self.error

    async def __intermediary_main(self):
        """
        Asynchronous private intermediary method to allow catching TimeoutErrors
        """
        await asyncio.wait(self.coroutines)

    async def __de_sync(self, f: callable, args: tuple, n: int):
        """
        Asynchronous private method for redefining af function to be run asyncronously, and save the output in output
        Arguments:
            f: callable, the function to redefine
            args: tuple, the arguments the run the function with
            n: int, address of the function output in output
        """
        try:
            self.output[n] = (await self.loop.run_in_executor(None, f, *args))
        except asyncio.TimeoutError:
            raise self.error  # Raise exception on timeout


nest_asyncio.apply()   # Allow asyncio event loop nesting

if __name__ == '__main__':
    example = Async()

    def test_number_of_coroutines():
        import numpy as np

        print('Testing number_of_coroutines')
        arguments = [(1,), (2,), (3,), (50,)]
        example.append_coroutines(np.sin, arguments)
        print('True Length:', len(arguments))
        print('Measured length:', example.number_of_coroutines())
        example.clear_coroutines()

    def test_append_coroutines():
        import numpy as np

        print('Testing append_coroutines')
        example.append_coroutines(np.sin, [(1,), (2,), (3,), (50,)])
        print('Number of coroutines:', example.number_of_coroutines())
        example.append_coroutines(np.cos, [(1,), (2,), (3,), (50,)])
        print('New number of coroutines:', example.number_of_coroutines())
        example.clear_coroutines()

    def test_run():
        import numpy as np
        import time

        def time_indicator(s: float, message: str):
            time.sleep(s)
            return message

        print('Testing run')
        example.append_coroutines(time_indicator, [(2, 'timed')])
        example.append_coroutines(np.sin, [(1,), (2,), (3,), (50,)])
        print(example.run())

    def test_run_sorted():
        import numpy as np
        import time

        def time_indicator(s: float, message: str):
            time.sleep(s)
            return message

        print('Testing run(sort=True)')
        example.append_coroutines(time_indicator, [(2, 'timed')])
        example.append_coroutines(np.sin, [(1,), (2,), (3,), (50,)])
        print(example.run(sort=True))

    def test_effect():
        import requests
        from PersonalPackages import kingtimer

        print('Testing actual improvements')
        URL = 'https://www.google.com/'
        N = 100

        def test1(url: str, n: int):
            output = [requests.get(url) for _ in range(n)]
            return output

        def test2(url: str, n: int):
            ac = Async()
            ac.append_coroutines(requests.get, [(url,) for _ in range(n)])
            return ac.run(sort=True)

        print('Runtime for test 1:')
        _ = kingtimer.runtime(test1, (URL, N))[0]
        print('Runtime for test 2:')
        _ = kingtimer.runtime(test2, (URL, N))[0]

    test_number_of_coroutines()
    print()
    test_append_coroutines()
    print()
    test_run()
    print()
    test_run_sorted()
    print()
    test_effect()
