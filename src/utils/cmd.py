""" Implementation of a general command line

Ctrl-D to exit. Type command 'help' to get help
"""
import sys

def print_list(method):
    """ Help function to convert a function that returns a list to a function
    that prints item in the list
    """
    def inner():
        for item in method():
            print item
    return inner

class CommandLine(object):
    def __init__(self):
        self.handlers = { 'help': self.help }
        self.usages = { 'help': 'get help!'}

    def exit(self):
        print 'bye~'
        sys.exit(0)

    def run(self):
        while True:
            try:
                user_input = raw_input('>>')
            except EOFError:
                self.exit()
            except KeyboardInterrupt:
                continue

            if len(user_input) == 0:
                continue

            tmp = user_input.split()

            cmd = tmp[0]
            args = tmp[1:]

            handler = self.handlers.get(cmd)
            if handler is None:
                print 'command not found. try help'
            else:
                try:
                    handler(*args)
                except Exception as e:
                    print 'error! %s' % e.message

    def register(self, cmd, handler, usage):
        """ Register a command with name, handler and usage to the command line
        """
        self.handlers[cmd] = handler
        self.usages[cmd] = usage

    def help(self):
        for cmd in self.usages:
            print '%s\t%s' % (cmd, self.usages[cmd])
