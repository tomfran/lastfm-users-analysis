from abc import ABCMeta, abstractmethod

class AbstractDestination(metaclass = ABCMeta):

	def __init__(self):
		pass
		
	@abstractmethod	
	def write(self):
		pass

	@abstractmethod	
	def commit(self):
		pass

	@abstractmethod
	def rollback(self):
		pass