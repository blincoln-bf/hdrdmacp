

import sbms

# get env object and clone it
Import('*')
env = env.Clone()

sbms.AddZEROMQ(env)

env.AppendUnique(CXXFLAGS=['--std=c++11', '-g'])
env.AppendUnique(LIBS=['ibverbs','z', 'pthread'])

sbms.executable(env)

