from bolt_server.socket_handler import SocketHandler
from bolt_server.message_dispatcher import MessageDispatcher
from bolt_server.plugin_loader import PluginLoader
from bolt_server.execution_engine import ExecutionEngine

s = SocketHandler()
m = MessageDispatcher(s)
p = PluginLoader()
p.load_plugins()

e = ExecutionEngine(m, p)
print "Server started"
test = raw_input("Enter something to continue:")

print s.client_list.client_list
i = e.new_task('hello', 'Command', {'command': 'date'}, ['Test'])
print i
e.execute_task(i)

i2 = e.new_task('hello2', 'Command', {'command': 'uptime'}, ['Test2'])
print i2
e.execute_task(i2)
