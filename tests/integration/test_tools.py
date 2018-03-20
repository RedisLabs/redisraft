def with_setup_args(setup, teardown=None):
    """Decorator to add setup and/or teardown methods to a test function::
      @with_setup_args(setup, teardown)
      def test_something():
          " ... "
    The setup function should return (args, kwargs) which will be passed to
    test function, and teardown function.
    Note that `with_setup_args` is useful *only* for test functions, not for test
    methods or inside of TestCase subclasses.
    """
    def decorate(func):
        args = []
        kwargs = {}

        def test_wrapped():
            func(*args, **kwargs)

        test_wrapped.__name__ = func.__name__

        def setup_wrapped():
            a, k = setup()
            args.extend(a)
            kwargs.update(k)
            if hasattr(func, 'setup'):
                func.setup()
        test_wrapped.setup = setup_wrapped

        if teardown:
            def teardown_wrapped():
                if hasattr(func, 'teardown'):
                    func.teardown()
                teardown(*args, **kwargs)

            test_wrapped.teardown = teardown_wrapped
        else:
            if hasattr(func, 'teardown'):
                test_wrapped.teardown = func.teardown()
        return test_wrapped
    return decorate
