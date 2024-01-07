import traceback

import trio
from outcome import Error

import moat

moat.kill = False

import logging

logger = logging.getLogger("TRACE")
e = logger.error


class Tracer(trio.abc.Instrument):
    def __init__(self):
        super().__init__()
        self.etasks = set()

    def _print_with_task(self, msg, task, err=None):
        # repr(task) is perhaps more useful than task.name in general,
        # but in context of a tutorial the extra noise is unhelpful.
        if err is not None:
            e("%s: %s %s", msg, task.name, repr(err))
            traceback.print_exception(type(err), err, err.__traceback__)
        else:
            e("%s: %s", msg, task.name)

    def nursery_end(self, task, exception):
        if isinstance(exception, Exception):
            self.etasks.add(task)
            self._print_with_task("*** task excepted", task, exception)
        pass

    def before_task_step(self, task):
        if isinstance(task._next_send, Error) and isinstance(
            task._next_send.error, Exception
        ):
            self._print_with_task("*** step resume ERROR", task, task._next_send.error)
            self.etasks.add(task)
        elif moat.kill:  # pylint: disable=c-extension-no-member  # OH COME ON
            self._print_with_task("*** step resume", task)

    def task_scheduled(self, task):
        e("SCHED %r", task)

    def task_exited(self, task):
        self._print_with_task("*** task exited", task)
        self.etasks.discard(task)

    def before_io_wait(self, timeout):
        if timeout > 10000 and self.etasks:
            e("\n\n\n\n\n\n\n\n\n\n")
            e("*** ERROR: lock-out, killing off error tasks")
            e("\n\n\n\n")
            for t in self.etasks:
                if t._next_send_fn is None:
                    self._print_with_task("!!! Killing", t)
                    t._runner.reschedule(t, Error(RuntimeError("*** Locked ***")))
                else:
                    self._print_with_task("??? already scheduled", t)


# trio.run(main_, instruments=[Tracer()])
